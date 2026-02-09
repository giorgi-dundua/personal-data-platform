"""Pipeline state manager.

Persists execution status for each DAG stage so runs can be resumed safely.
All writes use atomic file replacement to avoid corrupting the state file
on crash or interruption.
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
    """Centralized controller for pipeline execution state.

    Tracks per-stage status, timestamps, and simple metadata such as
    row counts and source information. State is stored in a JSON file
    configured via ``config.PIPELINE_STATE_DIR``.
    """

    def __init__(self):
        self.state_path = config.PIPELINE_STATE_DIR
        self.state: dict[str, Any] = self._load_state()

        # Ensure base structure
        if "stages" not in self.state:
            self.state["stages"] = {}

    # ---- State I/O ----
    def _load_state(self) -> dict:
        """Load existing pipeline state from disk, if present."""
        if self.state_path.exists():
            return json.loads(self.state_path.read_text())
        return {}

    def _save(self) -> None:
        """Persist current state to disk using atomic write.

        Writes to a temporary file in the same directory and then replaces
        the final state file. This prevents half-written JSON if the process
        crashes mid-write.
        """
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(".json.tmp")
        try:
            tmp.write_text(json.dumps(self.state, indent=4), encoding="utf-8")
            tmp.replace(self.state_path)
        except Exception:
            if tmp.exists():
                try:
                    tmp.unlink()
                except OSError:
                    pass
            raise

    # ---- Query API ----
    def get_status(self, stage: str) -> str:
        """Return the current status string for a stage."""
        return self.state["stages"].get(stage, {}).get("status", STATUS_PENDING)

    def is_done(self, stage: str) -> bool:
        """Return True if the stage has successfully passed."""
        return self.get_status(stage) == STATUS_PASSED

    def is_failed(self, stage: str) -> bool:
        """Return True if the stage has failed."""
        return self.get_status(stage) == STATUS_FAILED

    def can_run(self, stage: str) -> bool:
        """Return True if a stage is eligible to run (pending or failed)."""
        status = self.get_status(stage)
        return status in {STATUS_PENDING, STATUS_FAILED}

    # ---- Control API ----
    def mark_running(self, stage: str) -> None:
        """Mark a stage as currently running and persist the state."""
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
    ) -> None:
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

    def mark_failed(self, stage: str, error: str) -> None:
        """Mark a stage as failed with an associated error message."""
        self.state["stages"][stage] = {
            "status": STATUS_FAILED,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": error,
        }
        self._save()
