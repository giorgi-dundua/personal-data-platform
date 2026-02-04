import json
from pathlib import Path
from typing import Optional
from pipeline.artifacts import Artifact


class ArtifactRegistry:
    def __init__(self, registry_path: Path):
        self.registry_path = registry_path
        self._data = {
            "artifacts": {},
            "hash_index": {},
            "runs": {}
        }

        if registry_path.exists():
            self.load()

    # ----------------- persistence -----------------

    def load(self):
        with open(self.registry_path, "r", encoding="utf-8") as f:
            self._data = json.load(f)

    def save(self):
        with open(self.registry_path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)

    # ----------------- core API -----------------

    def register(self, artifact: Artifact):
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
        if version is None:
            version = self._data["artifacts"][artifact_id]["latest"]
        return self._data["artifacts"][artifact_id]["versions"][version]

    def latest(self, artifact_id: str) -> dict:
        return self.get(artifact_id, None)

    def exists_hash(self, content_hash: str) -> bool:
        return content_hash in self._data["hash_index"]

    def resolve(self, ref: str) -> dict:
        # formats:
        # normalized_bp:v2
        # normalized_bp:latest
        # sha256:abc123...

        if ref.startswith("sha256:"):
            return self._data["hash_index"][ref]

        aid, ver = ref.split(":", 1)
        if ver == "latest":
            ver = self._data["artifacts"][aid]["latest"]

        return self._data["artifacts"][aid]["versions"][ver]

    # ----------------- versioning -----------------

    def next_version(self, artifact_id: str) -> str:
        if artifact_id not in self._data["artifacts"]:
            return "v1"
        versions = self._data["artifacts"][artifact_id]["versions"]
        nums = [int(v[1:]) for v in versions if v.startswith("v")]
        return f"v{max(nums) + 1}" if nums else "v1"
