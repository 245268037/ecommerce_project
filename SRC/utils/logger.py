import logging
import os


CURRENT_DIR = os.path.dirname(__file__)
BASE_DIR = os.path.dirname(
    os.path.dirname(
        CURRENT_DIR
    )
)
LOG_DIR = os.path.join(
    BASE_DIR,
    "logs"
)

os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "etl.log")

logging.basicConfig(
    filename= LOG_FILE,
    level=logging.INFO,
    encoding='utf-8',
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    filemode='a'
)

logger = logging.getLogger(__name__)


print(logging.getLogger().handlers)