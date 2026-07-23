import time
from utils.logger import logger


class Timer:

    def __init__(self):
        self.start = None

    def begin(self):
        self.start = time.time()

    def end(self):
        if self.start is None:
            logger.warning(
                "Timer尚未开始，无法计算耗时"
            )
            return None

        elapsed_seconds = (
            time.time() - self.start
        )

        logger.info(
            f"任务耗时：{elapsed_seconds:.2f}秒"
        )

        return elapsed_seconds
