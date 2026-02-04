"""
Pipeline state manager.
Controls resumable execution and stage lifecycle.
"""

from typing import Any
import json
from datetime import datetime, timezone

from config.settings import config


STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_PASSED = "passed"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"


class PipelineState:
    """
    Centralized pipeline execution state controller.
    """

    def __init__(self):
        self.state_path = config.PIPELINE_STATE_DIR
        self.state: dict[str, Any] = self._load_state()

        # Ensure base structure
        if "stages" not in self.state:
            self.state["stages"] = {}

    # ---- State I/O ----
    def _load_state(self) -> dict:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text())
        return {}

    def _save(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(self.state, indent=4))

    # ---- Query API ----
    def get_status(self, stage: str) -> str:
        return self.state["stages"].get(stage, {}).get("status", STATUS_PENDING)

    def is_done(self, stage: str) -> bool:
        return self.get_status(stage) == STATUS_PASSED

    def is_failed(self, stage: str) -> bool:
        return self.get_status(stage) == STATUS_FAILED

    def can_run(self, stage: str) -> bool:
        status = self.get_status(stage)
        return status in {STATUS_PENDING, STATUS_FAILED}

    # ---- Control API ----
    def mark_running(self, stage: str):
        self.state["stages"][stage] = {
            "status": STATUS_RUNNING,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._save()

    def mark_passed(
        self,
        stage: str,
        *,
        rows: int | None = None,
        sources: dict | None = None,
        gate_passed: bool = True,
    ):
        payload: dict[str, Any] = {
            "status": STATUS_PASSED,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "gate_passed": gate_passed,
        }

        if rows is not None:
            payload["rows"] = rows

        if sources is not None:
            payload["sources"] = sources

        self.state["stages"][stage] = payload
        self._save()

    def mark_failed(self, stage: str, error: str):
        self.state["stages"][stage] = {
            "status": STATUS_FAILED,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": error,
        }
        self._save()
