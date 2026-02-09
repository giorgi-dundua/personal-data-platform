"""
SQLite-backed artifact registry. Schema aligned with pipeline.artifacts.Artifact.
Supports context management for safe connection handling.
Uses INSERT OR REPLACE for idempotent register; single transaction per write.
"""
import sqlite3
import json
from pathlib import Path
from typing import Optional
from contextlib import contextmanager
from pipeline.artifacts import Artifact


class SQLiteArtifactRegistry:
    """
    SQLite implementation of the Artifact Registry.
    Uses the Repository Pattern to isolate SQL logic from domain models.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()

    # ---- Context Manager Protocol (Restored) ----
    def __enter__(self):
        """
        Allows usage in 'with' statements.
        Since we use per-operation connections (_get_conn), this just returns self.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """No-op: Connections are managed atomically by _get_conn."""
        pass

    # ---- Internal Helpers ----
    @contextmanager
    def _get_conn(self):
        """
        Yields a connection with automatic transaction handling.
        Commits on success, rolls back on exception.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Access columns by name
        try:
            with conn:  # Transaction block
                yield conn
        finally:
            conn.close()

    def _init_db(self):
        """Idempotent schema initialization."""
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT NOT NULL,
                    version TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    path TEXT NOT NULL,
                    type TEXT NOT NULL,
                    format TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    created_by_stage TEXT,
                    created_by_run TEXT,
                    inputs JSON,
                    schema_def JSON,
                    metadata JSON,
                    PRIMARY KEY (id, version)
                )
            """)
            # Index for O(1) cache lookups
            conn.execute("CREATE INDEX IF NOT EXISTS idx_content_hash ON artifacts(content_hash)")

    # ---- Public API ----

    def register(self, artifact: Artifact) -> None:
        """
        Upsert an artifact. Uses INSERT OR REPLACE to handle re-runs safely.
        """
        query = """
            INSERT OR REPLACE INTO artifacts (
                id, version, content_hash, path, type, format, 
                created_at, created_by_stage, created_by_run, 
                inputs, schema_def, metadata
            ) VALUES (
                :id, :version, :content_hash, :path, :type, :format, 
                :created_at, :created_by_stage, :created_by_run, 
                :inputs, :schema_def, :metadata
            )
        """
        # FIX: by_alias=False ensures we get 'schema_def' (matching DB column), not 'schema'
        data = artifact.model_dump(by_alias=False)

        # Serialization for SQLite
        data['path'] = str(data['path'])
        data['created_at'] = data['created_at'].isoformat()
        data['inputs'] = json.dumps(data['inputs'])
        data['schema_def'] = json.dumps(data['schema_def'])  # Now this key exists
        data['metadata'] = json.dumps(data['metadata'])

        with self._get_conn() as conn:
            conn.execute(query, data)

    def get(self, artifact_id: str, version: Optional[str] = None) -> Optional[Artifact]:
        """Retrieve artifact by ID and optional version."""
        if version:
            query = "SELECT * FROM artifacts WHERE id = ? AND version = ?"
            with self._get_conn() as conn:
                row = conn.execute(query, (artifact_id, version)).fetchone()
                return self._row_to_artifact(row) if row else None
        return self.get_latest_version(artifact_id)

    def latest(self, artifact_id: str) -> Optional[Artifact]:
        """Alias for get_latest_version (Backward Compatibility)."""
        return self.get_latest_version(artifact_id)

    def get_latest_version(self, artifact_id: str) -> Optional[Artifact]:
        """
        Retrieve the latest version of an artifact.
        Deterministic sort: created_at DESC, then version DESC.
        """
        query = """
            SELECT * FROM artifacts 
            WHERE id = ? 
            ORDER BY created_at DESC, version DESC
            LIMIT 1
        """
        with self._get_conn() as conn:
            row = conn.execute(query, (artifact_id,)).fetchone()
            if row:
                return self._row_to_artifact(row)
        return None

    def get_by_hash(self, content_hash: str) -> Optional[Artifact]:
        """Cache Hit Mechanism: Find an artifact by its content hash."""
        query = "SELECT * FROM artifacts WHERE content_hash = ? LIMIT 1"
        with self._get_conn() as conn:
            row = conn.execute(query, (content_hash,)).fetchone()
            if row:
                return self._row_to_artifact(row)
        return None

    def get_by_input_hash(self, input_hash: str) -> Optional[Artifact]:
        """
        Find an artifact that was produced by a specific input state.
        Used for Cache-Aware Execution (skipping stages).
        """
        # We store inputs as a JSON list: ["hash_value"]
        # We use the LIKE operator to find the hash inside that JSON string.
        query = "SELECT * FROM artifacts WHERE inputs LIKE ? ORDER BY created_at DESC LIMIT 1"
        search_pattern = f'%"{input_hash}"%'

        with self._get_conn() as conn:
            row = conn.execute(query, (search_pattern,)).fetchone()
            if row:
                return self._row_to_artifact(row)
        return None

    def exists_hash(self, content_hash: str) -> bool:
        """Check if hash exists (Backward Compatibility)."""
        return self.get_by_hash(content_hash) is not None

    def next_version(self, artifact_id: str) -> str:
        """
        Calculate next version string (e.g., 'v1' -> 'v2').
        Scans all versions for the ID to find the max integer.
        """
        query = "SELECT version FROM artifacts WHERE id = ?"
        with self._get_conn() as conn:
            rows = conn.execute(query, (artifact_id,)).fetchall()

        if not rows:
            return "v1"

        max_v = 0
        for row in rows:
            v_str = row[0]
            # Robust parsing: handle 'v1', 'v10', ignore 'beta', etc.
            if v_str.startswith("v") and v_str[1:].isdigit():
                v_num = int(v_str[1:])
                if v_num > max_v:
                    max_v = v_num

        return f"v{max_v + 1}"

    @staticmethod
    def _row_to_artifact(row: sqlite3.Row) -> Artifact:
        """Map DB Row -> Pydantic Model safely."""
        d = dict(row)
        # Deserialize JSON fields
        try:
            d['inputs'] = json.loads(d['inputs']) if d['inputs'] else []
            d['schema'] = json.loads(d['schema_def']) if d['schema_def'] else None
            d['metadata'] = json.loads(d['metadata']) if d['metadata'] else {}
        except json.JSONDecodeError:
            # Fallback for corrupted metadata to prevent pipeline crash
            d['inputs'] = []
            d['schema'] = None
            d['metadata'] = {"error": "metadata_corrupted"}

        return Artifact(**d)