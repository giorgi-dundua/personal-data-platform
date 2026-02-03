"""
Pipeline state manager.
Tracks execution metadata, row counts, and gating results.
"""

from typing import Any
import json
from datetime import datetime, timezone

from config.settings import config


class PipelineState:
    """
    Centralized pipeline execution state tracker.
    """

    def __init__(self):
        self.state_path = config.PIPELINE_STATE_DIR
        self.state: dict[str, Any] = self._load_state()

    # ---- State I/O ----
    def _load_state(self) -> dict:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text())
        return {}

    def _save(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(self.state, indent=4))

    # ---- Public API ----
    def update_stage(
        self,
        stage: str,
        gate_passed: bool,
        rows: int | None = None,
        sources: dict | None = None,
    ):
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "gate_passed": gate_passed,
        }

        if rows is not None:
            payload["rows"] = rows

        if sources is not None:
            payload["sources"] = sources

        self.state[stage] = payload
        self._save()
