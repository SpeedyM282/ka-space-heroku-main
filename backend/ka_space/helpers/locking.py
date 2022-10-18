import logging
import time
from datetime import datetime, timedelta

import redis

from django.conf import settings

logger = logging.getLogger(__name__)


class Locking(object):
    def __init__(self, key, timeout=60, expire=60 * 5, minimum_life=0):
        self.redis = redis.from_url(settings.BROKER_URL, decode_responses=True)
        assert self.redis.ping(), "No connection to Redis Server"

        self.key = key
        self.timeout = timeout
        self.expire = expire
        self.minimum_life = minimum_life  # минимальное время удержания

        self.flag_locked = f"flag_{self.key}_locked"
        self.flag_state = f"flag_{self.key}_state"

        self.release_at = time.time()

    def is_locked(self):
        value = self.redis.get(self.flag_locked)
        return value is not None and float(value) + self.expire > time.time()

    def acquire(self, timeout=None, expire=None):
        timeout = timeout or self.timeout
        expire = expire or self.expire
        wait = time.time() + timeout
        sleep = 0.1

        while self.is_locked() and wait > time.time():
            time.sleep(sleep)

        if self.is_locked():
            raise ErrorIsLocked(f"Locked: {self.flag_locked}")

        self.release_at = time.time() + self.minimum_life
        return self.redis.set(
            self.flag_locked,
            time.time(),
            ex=expire,
        )

    def release(self):
        # if self.is_locked():
        #    raise ErrorNotLocked(f"Not Locked: {self.flag_locked}")
        if (self.release_at - time.time()) > 0:
            # ждем, чтобы отпустить с задержкой
            time.sleep(self.release_at - time.time())

        return self.redis.delete(self.flag_locked)

    def set_state(self, value):
        return self.redis.set(self.flag_state, value, ex=self.expire)

    def state(self):
        return {
            "is_locked": self.is_locked(),
            "locked": self.redis.get(self.flag_locked),
            "state": self.redis.get(self.flag_state),
        }


class ErrorIsLocked(Exception):
    pass


class ErrorNotLocked(Exception):
    pass
