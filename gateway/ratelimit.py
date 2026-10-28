"""Rate limiting with the token bucket algorithm (one bucket per user, kept in memory).

A bucket holds at most `burst` tokens and gets `per_minute / 60` new tokens every second.
Every request takes one token. When the bucket is empty the request is refused with HTTP 429.
"""
import threading
import time


class RateLimiter:
    def __init__(self, per_minute, burst):
        self.rate = per_minute / 60.0
        self.burst = burst
        self._buckets = {}                 # key -> [tokens, last_time]
        self._lock = threading.Lock()

    def check(self, key, per_minute=None, burst=None):
        """Returns (allowed, remaining_tokens, seconds_until_next_token).
        per_minute / burst override the default limits (the admin can give a user its own limits)."""
        rate = per_minute / 60.0 if per_minute else self.rate
        burst = burst or self.burst
        now = time.monotonic()
        with self._lock:
            tokens, last = self._buckets.get(key, (float(burst), now))
            tokens = min(float(burst), tokens + (now - last) * rate)
            if tokens >= 1:
                self._buckets[key] = (tokens - 1, now)
                return True, int(tokens - 1), 0.0
            self._buckets[key] = (tokens, now)
            return False, 0, (1 - tokens) / rate if rate > 0 else 60.0
