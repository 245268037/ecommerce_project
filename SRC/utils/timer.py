import time

class Timer:
    def __init__(self):
        self.start = None

    def begin(self):
        self.start = time.time()

    def end(self):
        print(
            f"耗时{time.time() - self.start:.2f}秒"
        )