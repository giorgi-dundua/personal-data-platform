# pipeline/registry_sqlite.py
import sqlite3
from pathlib import Path
from typing import Optional, List
from pipeline.artifacts import Artifact


class SQLiteArtifactRegistry:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    hash TEXT NOT NULL,
                    path TEXT NOT NULL,
                    schema TEXT,
                    created_at TEXT NOT NULL,
                    metadata TEXT
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_name ON artifacts(name)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_stage ON artifacts(stage)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_hash ON artifacts(hash)")
            conn.commit()

    # ---------- API ----------

    def register(self, artifact: Artifact):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO artifacts (
                    id, name, stage, version, hash, path, schema, created_at, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                artifact.id,
                artifact.name,
                artifact.stage,
                artifact.version,
                artifact.hash,
                str(artifact.path),
                artifact.schema,
                artifact.created_at,
                artifact.metadata_json(),
            ))
            conn.commit()

    def get(self, artifact_id: str) -> Optional[Artifact]:
        with self._connect() as conn:
            cur = conn.cursor()
            row = cur.execute(
                "SELECT * FROM artifacts WHERE id = ?",
                (artifact_id,)
            ).fetchone()
            return Artifact.from_row(row) if row else None

    def latest(self, name: str) -> Optional[Artifact]:
        with self._connect() as conn:
            cur = conn.cursor()
            row = cur.execute("""
                SELECT * FROM artifacts
                WHERE name = ?
                ORDER BY version DESC
                LIMIT 1
            """, (name,)).fetchone()
            return Artifact.from_row(row) if row else None

    def exists_hash(self, hash_value: str) -> bool:
        with self._connect() as conn:
            cur = conn.cursor()
            row = cur.execute(
                "SELECT 1 FROM artifacts WHERE hash = ? LIMIT 1",
                (hash_value,)
            ).fetchone()
            return row is not None

    def next_version(self, name: str) -> int:
        with self._connect() as conn:
            cur = conn.cursor()
            row = cur.execute("""
                SELECT MAX(version) FROM artifacts WHERE name = ?
            """, (name,)).fetchone()
            return (row[0] or 0) + 1
