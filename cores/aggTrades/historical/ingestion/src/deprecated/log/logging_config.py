import logging
from threading import Lock

# Configure log
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

log_lock = Lock()


def log_safe(message, level=logging.INFO):
    with log_lock:
        logging.log(level, message)
