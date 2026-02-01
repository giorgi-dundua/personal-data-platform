from config.settings import config
from config.logging import setup_logging, get_logger

setup_logging()
logger = get_logger("bootstrap")

logger.info("Application starting")
logger.info(f"ENV={config.ENV}")
logger.info(f"DEBUG={config.DEBUG}")
logger.info(f"DATA_DIR={config.DATA_DIR}")
logger.info(f"MED_START_DATE={config.MED_START_DATE}")

print("System bootstrapped")
