"""
Pipeline Orchestrator.
Handles topological execution, cache-aware skipping, and atomic artifact registration.
"""
from typing import List, Optional, Dict
from pathlib import Path
from datetime import datetime, timezone
from config.settings import config
from config.logging import get_logger
from pipeline.dag import PIPELINE_DAG
from pipeline.dag_executor import topo_sort
from pipeline.registry_sqlite import SQLiteArtifactRegistry
from pipeline.artifacts import Artifact
from pipeline.hash_utils import hash_file, hash_strings, hash_source
from pipeline.pipeline_state import PipelineState

logger = get_logger("orchestrator")


class PipelineOrchestrator:
    def __init__(self):
        self.registry_path = config.PROCESSED_DATA_DIR / "registry.db"
        # Ensure registry directory exists
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        self.registry = SQLiteArtifactRegistry(self.registry_path)
        self.state = PipelineState()

    @staticmethod
    def get_input_hash(stage_name: str, consumes: List[str]) -> str:
        """Computes a unique hash for the stage's input state (Data + Code + Config)."""
        node = PIPELINE_DAG[stage_name]
        hashes = [stage_name]

        # 1. Hash Logic Hooks (Classes/Functions defined in DAG)
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

        # 4. Hash Configuration & Environment
        hashes.append(str(config))
        pyproject_path = Path(config.BASE_DIR) / "pyproject.toml"
        if pyproject_path.exists():
            hashes.append(hash_file(pyproject_path))

        return hash_strings(hashes)

    def register_output(self, stage_name: str, artifacts: Dict[str, List[Path]], input_hash: str, run_id: str):
        """
        Finalizes temporary files and registers them in the metadata database.

        Args:
            stage_name: Name of the DAG stage.
            artifacts: Map of artifact type to list of temporary file paths.
            input_hash: The hash of the inputs that produced these artifacts.
            run_id: Unique ID for the current pipeline run.
        """
        for art_type, tmp_paths in artifacts.items():
            for tmp_path in tmp_paths:
                # 1. Determine final path (e.g., data.csv.abcd.tmp -> data.csv)
                # We split at the first '.csv' to handle our UUID suffix pattern
                # Assumption: files are named like 'name.csv.<uuid>.tmp'
                if ".csv" in tmp_path.name:
                    stem_part = tmp_path.name.split('.csv')[0]
                    final_path = tmp_path.parent / (stem_part + ".csv")
                else:
                    # Fallback if naming convention varies
                    final_path = tmp_path.with_suffix("").with_suffix("")

                # 2. Atomic Commit (Rename)
                if tmp_path.exists():
                    tmp_path.replace(final_path)
                    logger.debug(f"Finalized: {final_path.name}")

                # 3. Registry Entry
                art_id = f"{stage_name}_{final_path.stem}"
                content_hash = hash_file(final_path)
                next_ver = self.registry.next_version(art_id)

                artifact = Artifact(
                    id=art_id,
                    version=next_ver,
                    content_hash=content_hash,
                    path=final_path,
                    type=art_type,
                    format="csv",
                    created_by_stage=stage_name,
                    created_by_run=run_id,
                    inputs=[input_hash],
                    metadata={"run_ts": datetime.now(timezone.utc).isoformat()}
                )
                self.registry.register(artifact)
                logger.info(f"Registered artifact: {art_id}:{next_ver}")

    def run(self, resume: bool = False, start_stage: Optional[str] = None) -> None:
        """Executes the DAG with cache-aware skipping and partial run support."""
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Starting Pipeline Run: {run_id}")

        # 1. Resolve Execution Scope
        try:
            full_order = topo_sort(PIPELINE_DAG)
            if start_stage:
                if start_stage not in full_order:
                    raise ValueError(f"Invalid start_stage: {start_stage}")
                # Slice the list from the start_stage index to the end
                execution_order = full_order[full_order.index(start_stage):]
            else:
                execution_order = full_order

            logger.info(f"Execution Scope: {' -> '.join(execution_order)}")
        except Exception as e:
            logger.critical(f"DAG Error: {e}")
            return

        # 2. Execution Loop
        for stage_name in execution_order:
            node = PIPELINE_DAG[stage_name]
            logger.info(f"--- Stage: {stage_name} ---")

            # A. Identity Check (Cache Lookup)
            input_hash = self.get_input_hash(stage_name, node["consumes"])
            cached = self.registry.get_by_input_hash(input_hash)

            if cached and cached.path.exists():
                logger.info(f"✅ Cache Hit for {stage_name}. (Artifact: {cached.version})")
                self.state.mark_passed(stage_name, gate_passed=True)
                continue

            # B. Resume Filter (Skip if Status is Passed AND Resume requested)
            if resume and self.state.is_done(stage_name):
                logger.info(f"⏩ Skipping {stage_name} (Resume: Status is Passed)")
                continue

            # C. Execution
            self.state.mark_running(stage_name)
            try:
                # Execute Node (returns dict with 'artifacts' and 'metrics')
                result = node["fn"]()

                # Register Outputs (Atomic Commit)
                self.register_output(stage_name, result.get("artifacts", {}), input_hash, run_id)

                # Update State
                self.state.mark_passed(stage_name, sources=result.get("metrics"))
                logger.info(f"Stage '{stage_name}' completed successfully.")

            except Exception as e:
                self.state.mark_failed(stage_name, str(e))
                logger.error(f"Stage '{stage_name}' failed: {e}", exc_info=True)
                if not resume:
                    logger.error("Pipeline aborted due to failure.")
                    break


def run_pipeline(resume: bool = False, start_stage: Optional[str] = None):
    """
    Entry point for the CLI.
    Instantiates the orchestrator and triggers the run.
    """
    orch = PipelineOrchestrator()
    orch.run(resume=resume, start_stage=start_stage)