"""
JSON-backed artifact registry implementation.

Stores artifact metadata in a JSON file with in-memory caching for fast lookups.
Provides hash-based indexing for cache-aware execution and version tracking.
"""
import json
from pathlib import Path
from typing import Optional

from pipeline.artifacts import Artifact


class ArtifactRegistry:
    """
    JSON-backed artifact registry with hash indexing and version tracking.
    
    Maintains artifact metadata in memory and persists to JSON file. Supports
    versioning, hash-based lookups, and reference resolution (by id:version or hash).
    
    Attributes:
        registry_path: Path to the JSON registry file.
        _data: In-memory registry data structure with artifacts, hash_index, and runs.
    """
    def __init__(self, registry_path: Path) -> None:
        """
        Initialize registry from JSON file or create empty registry.
        
        Args:
            registry_path: Path to JSON file. If file exists, loads existing data.
                          If not, creates empty registry structure.
        """
        self.registry_path = Path(registry_path)
        self._data: dict = {
            "artifacts": {},
            "hash_index": {},
            "runs": {},
        }
        if self.registry_path.exists():
            self.load()

    # ----------------- persistence -----------------

    def load(self) -> None:
        """
        Load registry data from JSON file into memory.
        
        Raises:
            FileNotFoundError: If registry_path doesn't exist (shouldn't happen
                              if called from __init__).
            json.JSONDecodeError: If JSON file is malformed.
        """
        with open(self.registry_path, "r", encoding="utf-8") as f:
            self._data = json.load(f)

    def save(self) -> None:
        """
        Persist registry data to JSON file atomically.
        
        Uses temp-file-then-replace pattern to prevent corruption if process
        crashes mid-write. Writes to `.json.tmp`, then renames to final path.
        This ensures the registry file is always valid JSON or doesn't exist.
        
        Raises:
            OSError: If parent directory cannot be created or file cannot be written.
        """
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.registry_path.with_suffix(".json.tmp")
        try:
            tmp.write_text(
                json.dumps(self._data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            tmp.replace(self.registry_path)
        except Exception:
            if tmp.exists():
                try:
                    tmp.unlink()
                except OSError:
                    pass
            raise

    # ----------------- core API -----------------

    def register(self, artifact: Artifact) -> None:
        """
        Register an artifact in the registry.
        
        Stores artifact metadata, updates hash index for cache lookups, and
        marks the version as latest for the artifact id. If the same (id, version)
        already exists, it is overwritten (idempotent operation).
        
        Args:
            artifact: The artifact to register. Must have valid id, version,
                     content_hash, and path. All fields are stored as-is.
        
        Note:
            This method automatically persists changes to disk via save().
            The operation is idempotent: registering the same artifact twice
            produces the same final state.
        """
        aid = artifact.id
        ver = artifact.version
        h = artifact.content_hash

        self._data["artifacts"].setdefault(aid, {
            "versions": {},
            "latest": None
        })

        self._data["artifacts"][aid]["versions"][ver] = {
            "hash": h,
            "path": str(artifact.path),
            "created_at": artifact.created_at.isoformat(),
            "created_by_stage": artifact.created_by_stage,
            "created_by_run": artifact.created_by_run,
            "inputs": artifact.inputs,
            "type": artifact.type,
            "format": artifact.format,
            "metadata": artifact.metadata,
        }

        self._data["artifacts"][aid]["latest"] = ver

        self._data["hash_index"][h] = {
            "artifact": aid,
            "version": ver
        }

        self.save()

    def get(self, artifact_id: str, version: Optional[str] = None) -> dict:
        """
        Retrieve artifact metadata by id and optional version.
        
        Args:
            artifact_id: The artifact identifier (e.g., "normalized_bp").
            version: Specific version to retrieve (e.g., "v1"). If None, returns
                    the latest version for the artifact.
        
        Returns:
            Dictionary containing artifact metadata: hash, path, created_at,
            created_by_stage, created_by_run, inputs, type, format, metadata.
        
        Raises:
            KeyError: If artifact_id doesn't exist or version is invalid.
        """
        if version is None:
            version = self._data["artifacts"][artifact_id]["latest"]
        return self._data["artifacts"][artifact_id]["versions"][version]

    def latest(self, artifact_id: str) -> dict:
        """
        Get the latest version of an artifact.
        
        Args:
            artifact_id: The artifact identifier.
        
        Returns:
            Dictionary containing metadata for the latest version.
        
        Raises:
            KeyError: If artifact_id doesn't exist or has no versions.
        """
        return self.get(artifact_id, None)

    def exists_hash(self, content_hash: str) -> bool:
        """
        Check if an artifact with the given content hash exists.
        
        Used for cache-aware execution: if input data hasn't changed (same hash),
        the stage can be skipped.
        
        Args:
            content_hash: SHA-256 hash string (e.g., "sha256:abc123...").
        
        Returns:
            True if an artifact with this hash is registered, False otherwise.
        """
        return content_hash in self._data["hash_index"]

    def resolve(self, ref: str) -> dict:
        """
        Resolve an artifact reference to its metadata.
        
        Supports multiple reference formats:
        - "artifact_id:version" (e.g., "normalized_bp:v2")
        - "artifact_id:latest" (resolves to latest version)
        - "sha256:hash" (lookup by content hash)
        
        Args:
            ref: Reference string in one of the supported formats.
        
        Returns:
            Dictionary containing artifact metadata.
        
        Raises:
            KeyError: If reference cannot be resolved (invalid id, version, or hash).
            ValueError: If reference format is unrecognized.
        
        Example:
            >>> registry.resolve("normalized_bp:latest")
            {"hash": "sha256:...", "path": "...", ...}
            >>> registry.resolve("sha256:abc123")
            {"artifact": "normalized_bp", "version": "v1"}
        """
        if ref.startswith("sha256:"):
            return self._data["hash_index"][ref]

        aid, ver = ref.split(":", 1)
        if ver == "latest":
            ver = self._data["artifacts"][aid]["latest"]

        return self._data["artifacts"][aid]["versions"][ver]

    # ----------------- versioning -----------------

    def next_version(self, artifact_id: str) -> str:
        """
        Calculate the next version string for an artifact.
        
        Parses existing versions (assumes format "v1", "v2", ...) and returns
        the next sequential version. If no versions exist or none match the "vN"
        pattern, returns "v1".
        
        Args:
            artifact_id: The artifact identifier.
        
        Returns:
            Next version string (e.g., "v1", "v2", "v3").
        
        Example:
            >>> registry.next_version("normalized_bp")  # No versions exist
            "v1"
            >>> registry.next_version("normalized_bp")  # After v1 exists
            "v2"
        """
        if artifact_id not in self._data["artifacts"]:
            return "v1"
        versions = self._data["artifacts"][artifact_id]["versions"]
        nums = [int(v[1:]) for v in versions if v.startswith("v")]
        return f"v{max(nums) + 1}" if nums else "v1"
