from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional


@dataclass(frozen=True)
class Artifact:
    # logical identity
    id: str                     # e.g. "normalized_bp"
    version: str                # e.g. "v1", "v2" (human-facing)

    # physical identity
    content_hash: str           # sha256 hash of file content
    path: Path                  # storage location

    # classification
    type: str                   # "raw" | "normalized" | "validated" | "merged"
    format: str                 # "csv" | "parquet" | "json"

    # provenance
    created_at: datetime
    created_by_stage: str       # DAG node name
    created_by_run: str         # pipeline run id

    # lineage
    inputs: List[str] = field(default_factory=list)   # ["raw_bp:v1", ...]

    # optional enrichment
    schema: Optional[Dict] = None
    stats: Optional[Dict] = None
    metadata: Dict = field(default_factory=dict)
