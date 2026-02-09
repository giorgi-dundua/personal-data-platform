from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator, ConfigDict

class Artifact(BaseModel):
    """
    Data Transfer Object for Pipeline Artifacts.
    Enforces strict typing and path validation using Pydantic V2.
    """
    id: str
    version: str
    content_hash: str
    path: Path
    type: str
    format: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_by_stage: str = ""
    created_by_run: str = ""
    inputs: List[str] = Field(default_factory=list)
    # 'schema' is a reserved keyword in some SQL contexts, so we alias it in Python
    schema_def: Optional[Dict[str, Any]] = Field(default=None, alias="schema")
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Pydantic V2 Configuration
    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={Path: str}
    )

    @field_validator("path", mode="before")
    @classmethod
    def convert_path(cls, v: Any) -> Path:
        """Ensure path is always a Path object, even if a string is passed."""
        if isinstance(v, str):
            return Path(v)
        return v