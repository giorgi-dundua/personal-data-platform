import logging
from datetime import datetime
from config.settings import config, BASE_DIR

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / f"{config.ENV}_{datetime.now().strftime('%Y-%m-%d')}.log"


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG if config.DEBUG else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )


def get_logger(name: str):
    return logging.getLogger(name)
