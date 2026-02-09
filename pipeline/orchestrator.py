"""
Pipeline Orchestrator.

This module is the central nervous system of the data platform. It is responsible for:
1.  Resolving the DAG dependency order (Topological Sort).
2.  Calculating input hashes for Cache-Aware Execution.
3.  Invoking stage functions (Nodes).
4.  Registering output artifacts into the SQLite Registry.

It bridges the gap between the static DAG definition and the dynamic runtime state.
"""

import inspect
from typing import List
from pathlib import Path
from datetime import datetime, timezone

from config.settings import config
from config.logging import get_logger
from pipeline.dag import PIPELINE_DAG
from pipeline.dag_executor import topo_sort
from pipeline.registry_sqlite import SQLiteArtifactRegistry
from pipeline.artifacts import Artifact
from pipeline.hash_utils import hash_file, hash_strings

logger = get_logger("orchestrator")


class PipelineOrchestrator:
    """
    Manages the execution lifecycle of the pipeline.

    Attributes:
        registry_path (Path): Location of the SQLite artifact registry.
        registry (SQLiteArtifactRegistry): Interface to the metadata database.
    """

    def __init__(self):
        """Initialize the orchestrator and connect to the artifact registry."""
        self.registry_path = config.PROCESSED_DATA_DIR / "registry.db"
        # Ensure the directory exists before connecting
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        self.registry = SQLiteArtifactRegistry(self.registry_path)

    @staticmethod
    def get_input_hash(stage_name: str, consumes: List[str]) -> str:
        """
        Calculate a deterministic hash of all inputs AND code for a specific stage.

        This hash serves as the 'Cache Key'. It combines:
        1. The Stage Name.
        2. The Content Hash of input files.
        3. The Source Code of the function (to invalidate cache on logic changes).
        4. The Global Configuration state.

        Args:
            stage_name: The name of the stage (e.g., 'normalization').
            consumes: List of input keys defined in the DAG (e.g., ['raw_data']).

        Returns:
            str: A SHA-256 hash string representing the input state.
        """
        # 1. Start with Stage Name
        hashes = [stage_name]

        # 2. Hash the Source Code of the Node Function
        # If you change the logic in 'normalization_stage', the hash changes.
        try:
            node_fn = PIPELINE_DAG[stage_name]["fn"]
            hashes.append(inspect.getsource(node_fn))
        except (OSError, TypeError):
            # Fallback if source cannot be retrieved (e.g., interactive shell)
            logger.warning(f"Could not hash source code for {stage_name}")

        # 3. Resolve input paths based on 'consumes' list
        path_map = {
            "raw_data": [config.raw_gs_path, config.RAW_MI_BAND_DATA_DIR],
            "normalized_data": [config.norm_bp_path, config.norm_hr_path, config.norm_sleep_path],
            "validated_data": [config.val_bp_path, config.val_hr_path, config.val_sleep_path],
        }

        for input_key in consumes:
            paths = path_map.get(input_key, [])
            for p in paths:
                if isinstance(p, Path):
                    if p.is_file():
                        hashes.append(hash_file(p))
                    elif p.is_dir():
                        # Hash all files in directory, sorted by name for determinism
                        # We only care about CSVs for now
                        if p.exists():
                            for sub in sorted(p.glob("*.csv")):
                                hashes.append(hash_file(sub))

        # 4. Hash the configuration state
        # This ensures that changing a setting (e.g., threshold) invalidates the cache
        hashes.append(str(config))

        return hash_strings(hashes)

    def register_output(self, stage_name: str, produces: List[str], input_hash: str, run_id: str) -> None:
        """
        Register the output files of a successfully completed stage.

        Args:
            stage_name: Name of the stage that produced the artifacts.
            produces: List of output keys defined in the DAG.
            input_hash: The hash of the inputs used to generate these outputs.
            run_id: Unique identifier for the current pipeline run.
        """
        # Map 'produces' keys to actual file paths
        output_map = {
            "raw_data": [config.raw_gs_path],  # MiBand is a dir, handled separately if needed
            "normalized_data": [config.norm_bp_path, config.norm_hr_path, config.norm_sleep_path],
            "validated_data": [config.val_bp_path, config.val_hr_path, config.val_sleep_path],
            "daily_metrics": [config.merged_path]
        }

        for prod_key in produces:
            paths = output_map.get(prod_key, [])
            for p in paths:
                if not p.exists():
                    logger.warning(f"Stage '{stage_name}' claimed to produce {p}, but it is missing.")
                    continue

                # Create a unique Artifact ID (e.g., 'normalization_bp_hr_normalized')
                art_id = f"{stage_name}_{p.stem}"
                content_hash = hash_file(p)

                # Calculate next version based on existing history
                next_ver = self.registry.next_version(art_id)

                # Construct the Artifact object
                artifact = Artifact(
                    id=art_id,
                    version=next_ver,
                    content_hash=content_hash,
                    path=p,
                    type=prod_key,
                    format=p.suffix.strip('.'),
                    created_by_stage=stage_name,
                    created_by_run=run_id,
                    inputs=[input_hash],  # Link output to the specific input hash
                    metadata={
                        "run_ts": datetime.now(timezone.utc).isoformat(),
                        "file_size_bytes": p.stat().st_size
                    }
                )

                self.registry.register(artifact)
                logger.info(f"Registered artifact: {art_id}:{next_ver} (Hash: {content_hash[:8]}...)")

    def run(self, resume: bool = False, skip_ingestion: bool = False) -> None:
        """
        Execute the pipeline DAG.

        Args:
            resume: If True, attempts to resume from the last successful state.
            skip_ingestion: If True, skips the 'ingestion' stage.
        """
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Starting Pipeline Run: {run_id}")

        # 1. Get Execution Order via Topological Sort
        try:
            order = topo_sort(PIPELINE_DAG)
            logger.info(f"Execution Order: {' -> '.join(order)}")
        except Exception as e:
            logger.critical(f"DAG Error: {e}")
            return

        # 2. Execute Stages
        for stage_name in order:
            # --- Skip Logic ---
            if skip_ingestion and stage_name == "ingestion":
                logger.info("Skipping ingestion stage as requested via CLI.")
                continue

            node = PIPELINE_DAG[stage_name]
            logger.info(f"--- Stage: {stage_name} ---")

            # A. Calculate Input Hash (The "Cache Key")
            input_hash = self.get_input_hash(stage_name, node["consumes"])

            # B. Check Cache (Future Implementation)
            cached_artifact = self.registry.get_by_input_hash(input_hash)

            if cached_artifact:
                # Critical Safety Check: Does the output file actually exist on disk?
                # If we skip running, but the file was deleted, downstream stages will crash.
                if cached_artifact.path.exists():
                    logger.info(
                        f"✅ Cache Hit for {stage_name}. Skipping execution. (Artifact: {cached_artifact.version})")
                    continue
                else:
                    logger.warning(
                        f"⚠️ Cache Hit for {stage_name}, but file {cached_artifact.path} is missing. Re-running.")

            # C. Run Node
            try:
                # Execute the function from nodes.py
                result = node["fn"]()
                logger.info(f"Stage '{stage_name}' completed. Result: {result}")

                # D. Register Outputs
                self.register_output(stage_name, node["produces"], input_hash, run_id)

            except Exception as e:
                logger.error(f"Stage '{stage_name}' failed: {e}", exc_info=True)
                if not resume:
                    logger.error("Pipeline aborted due to failure.")
                    break


def run_pipeline(resume: bool = False, skip_ingestion: bool = False):
    """Entry point for the CLI."""
    orch = PipelineOrchestrator()
    orch.run(resume=resume, skip_ingestion=skip_ingestion)