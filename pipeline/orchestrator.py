"""
Pipeline Orchestrator.

This module serves as the execution engine for the Personal Data Platform.
It manages the lifecycle of a pipeline run, including dependency resolution,
cache-aware execution (idempotency), and artifact registration.
"""

from typing import List
from pathlib import Path
from datetime import datetime, timezone

from config.settings import config
from config.logging import get_logger
from pipeline.dag import PIPELINE_DAG
from pipeline.dag_executor import topo_sort
from pipeline.registry_sqlite import SQLiteArtifactRegistry
from pipeline.artifacts import Artifact
from pipeline.hash_utils import hash_file, hash_strings, hash_source

logger = get_logger("orchestrator")


class PipelineOrchestrator:
    """
    Coordinates the execution of pipeline stages defined in the DAG.

    Attributes:
        registry_path (Path): Path to the SQLite database for artifact metadata.
        registry (SQLiteArtifactRegistry): Interface for metadata persistence.
    """

    def __init__(self):
        """Initialize the orchestrator and the artifact registry."""
        self.registry_path = config.PROCESSED_DATA_DIR / "registry.db"
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        self.registry = SQLiteArtifactRegistry(self.registry_path)

    @staticmethod
    def get_input_hash(stage_name: str, consumes: List[str]) -> str:
        """
        Calculate a deterministic hash representing the state of a stage's inputs.

        The hash includes:
        1. The stage identifier.
        2. Source code of logic classes/functions (logic_hooks).
        3. Source code of the node wrapper function.
        4. Content hashes of all input files/directories.
        5. The current application configuration state.

        Args:
            stage_name: Name of the stage being hashed.
            consumes: List of input categories (e.g., 'raw_data').

        Returns:
            str: SHA-256 hash string.
        """
        node = PIPELINE_DAG[stage_name]
        hashes = [stage_name]

        # 1. Hash Logic Hooks (Business logic classes/functions)
        for hook in node.get("logic_hooks", []):
            hashes.append(hash_source(hook))

        # 2. Hash the Node Wrapper Function
        hashes.append(hash_source(node["fn"]))

        # 3. Hash Input Data
        path_map = {
            "raw_data": [config.raw_gs_path, config.RAW_MI_BAND_DATA_DIR],
            "normalized_data": [config.norm_bp_path, config.norm_hr_path, config.norm_sleep_path],
            "validated_data": [config.val_bp_path, config.val_hr_path, config.val_sleep_path],
        }

        for input_key in consumes:
            for p in path_map.get(input_key, []):
                if isinstance(p, Path) and p.exists():
                    if p.is_file():
                        hashes.append(hash_file(p))
                    elif p.is_dir():
                        # Sort for deterministic directory hashing
                        for sub in sorted(p.glob("*.csv")):
                            hashes.append(hash_file(sub))

        # 4. Hash Global Configuration
        hashes.append(str(config))

        return hash_strings(hashes)

    def register_output(self, stage_name: str, produces: List[str], input_hash: str, run_id: str) -> None:
        """
        Register artifacts produced by a stage into the registry.

        Args:
            stage_name: Name of the stage that produced the artifacts.
            produces: List of output categories.
            input_hash: The input state hash that generated these outputs.
            run_id: Unique identifier for the current execution.
        """
        output_map = {
            "raw_data": [config.raw_gs_path],
            "normalized_data": [config.norm_bp_path, config.norm_hr_path, config.norm_sleep_path],
            "validated_data": [config.val_bp_path, config.val_hr_path, config.val_sleep_path],
            "daily_metrics": [config.merged_path]
        }

        for prod_key in produces:
            for p in output_map.get(prod_key, []):
                if not p.exists():
                    logger.warning(f"Stage '{stage_name}' expected to produce {p}, but file is missing.")
                    continue

                art_id = f"{stage_name}_{p.stem}"
                content_hash = hash_file(p)
                next_ver = self.registry.next_version(art_id)

                artifact = Artifact(
                    id=art_id,
                    version=next_ver,
                    content_hash=content_hash,
                    path=p,
                    type=prod_key,
                    format=p.suffix.strip('.'),
                    created_by_stage=stage_name,
                    created_by_run=run_id,
                    inputs=[input_hash],
                    metadata={
                        "run_ts": datetime.now(timezone.utc).isoformat(),
                        "file_size_bytes": p.stat().st_size
                    }
                )

                self.registry.register(artifact)
                logger.info(f"Registered artifact: {art_id}:{next_ver}")

    def run(self, resume: bool = False, skip_ingestion: bool = False) -> None:
        """
        Execute the pipeline DAG in topological order.

        Args:
            resume: Placeholder for future resumable state logic.
            skip_ingestion: If True, bypasses the ingestion stage.
        """
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Starting Pipeline Run: {run_id}")

        try:
            order = topo_sort(PIPELINE_DAG)
            logger.info(f"Execution Order: {' -> '.join(order)}")
        except Exception as e:
            logger.critical(f"DAG Resolution Error: {e}")
            return

        for stage_name in order:
            if skip_ingestion and stage_name == "ingestion":
                logger.info("Skipping ingestion stage as requested.")
                continue

            node = PIPELINE_DAG[stage_name]
            logger.info(f"--- Stage: {stage_name} ---")

            # 1. Idempotency Check (Cache Lookup)
            input_hash = self.get_input_hash(stage_name, node["consumes"])
            cached_artifact = self.registry.get_by_input_hash(input_hash)

            if cached_artifact:
                if cached_artifact.path.exists():
                    logger.info(f"✅ Cache Hit for {stage_name}. Skipping. (Artifact: {cached_artifact.version})")
                    continue
                else:
                    logger.warning(f"⚠️ Cache Hit for {stage_name}, but file {cached_artifact.path} is missing. Re-running.")

            # 2. Execution
            try:
                result = node["fn"]()
                logger.info(f"Stage '{stage_name}' completed. Result: {result}")

                # 3. Registration
                self.register_output(stage_name, node["produces"], input_hash, run_id)

            except Exception as e:
                logger.error(f"Stage '{stage_name}' failed: {e}", exc_info=True)
                if not resume:
                    logger.error("Pipeline aborted.")
                    break


def run_pipeline(resume: bool = False, skip_ingestion: bool = False):
    """Entry point for pipeline execution."""
    orch = PipelineOrchestrator()
    orch.run(resume=resume, skip_ingestion=skip_ingestion)