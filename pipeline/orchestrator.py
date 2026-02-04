from pipeline.dag import PIPELINE_DAG
from pipeline.dag_executor import topo_sort
from pipeline.pipeline_state import PipelineState
from config.logging import get_logger

logger = get_logger("orchestrator")


def can_execute(stage: str, dag: dict, state: PipelineState) -> bool:
    for dep in dag[stage]["depends_on"]:
        if not state.is_done(dep):
            return False
    return True


def run_pipeline(resume: bool = False):
    state = PipelineState()
    order = topo_sort(PIPELINE_DAG)

    logger.info(f"Pipeline order: {order}")

    for stage in order:
        node = PIPELINE_DAG[stage]

        # Resume logic
        if resume and state.is_done(stage):
            logger.info(f"⏩ Skipping {stage} (already passed)")
            continue

        if not can_execute(stage, PIPELINE_DAG, state):
            raise RuntimeError(f"Dependency violation for stage: {stage}")

        logger.info(f"▶ Running stage: {stage}")
        state.mark_running(stage)

        try:
            result = node["fn"]()

            # Store metadata if returned
            if isinstance(result, dict):
                state.mark_passed(stage, sources=result, gate_passed=True)
            elif isinstance(result, int):
                state.mark_passed(stage, rows=result, gate_passed=True)
            else:
                state.mark_passed(stage, gate_passed=True)

            logger.info(f"✅ {stage} passed")

        except Exception as e:
            state.mark_failed(stage, str(e))
            logger.critical(f"❌ {stage} failed: {e}")
            raise

    logger.info("🎯 Pipeline fully complete")
