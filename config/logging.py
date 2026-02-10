import logging
import sys
from datetime import datetime
from config.settings import config

LOG_DIR = config.BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / f"{config.ENV}_{datetime.now().strftime('%Y-%m-%d')}.log"


def setup_logging():
    # Force UTF-8 for the FileHandler to support emojis in .log files
    file_h = logging.FileHandler(LOG_FILE, encoding="utf-8")

    # Force UTF-8 for the Terminal output (StreamHandler)
    # This prevents the CP1252 'charmap' error on Windows
    stream_h = logging.StreamHandler(sys.stdout)

    logging.basicConfig(
        level=logging.DEBUG if config.DEBUG else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[file_h, stream_h]
    )


def get_logger(name: str):
    return logging.getLogger(name)
