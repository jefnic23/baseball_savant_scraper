import threading
import time


class RateLimiter:
    def __init__(self, interval_seconds: float):
        self.interval = interval_seconds
        self.lock = threading.Lock()
        self.next_time = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            if now < self.next_time:
                time.sleep(self.next_time - now)
            self.next_time = time.monotonic() + self.interval
