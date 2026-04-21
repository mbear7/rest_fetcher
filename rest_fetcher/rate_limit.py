from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from .exceptions import SchemaError

ClockFn = Callable[[], float]
SleepFn = Callable[[float], None]
OnLimit = Literal['wait', 'raise']


@dataclass
class _TokenBucketConfig:
    strategy: Literal['token_bucket']
    requests_per_second: float
    burst: int
    on_limit: OnLimit

    # Injectable for tests; defaults apply if not provided.
    clock: ClockFn | None = None
    sleep: SleepFn | None = None


class TokenBucket:
    def __init__(
        self, rps: float, burst: int, *, clock: ClockFn | None = None, sleep: SleepFn | None = None
    ) -> None:
        if rps <= 0:
            raise SchemaError('rate_limit.requests_per_second must be > 0')
        if burst <= 0:
            raise SchemaError('rate_limit.burst must be > 0')

        self._rps = float(rps)
        self._capacity = int(burst)
        self._tokens = float(burst)
        self._clock: ClockFn = clock or time.monotonic
        self._sleep: SleepFn = sleep or time.sleep
        self._last = self._clock()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> float:
        """Return required wait time in seconds to acquire `tokens` (>= 0).

        The token reservation happens inside the lock on all paths.
        Tokens may go negative; the negative balance represents debt.
        """
        if tokens <= 0:
            return 0.0

        with self._lock:
            now = self._clock()
            elapsed = now - self._last
            if elapsed > 0:
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rps)
                self._last = now

            # reserve immediately (may go negative)
            self._tokens -= tokens
            if self._tokens >= 0:
                return 0.0
            return (-self._tokens) / self._rps

    def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        self._sleep(seconds)


def build_token_bucket(cfg: dict[str, Any]) -> _TokenBucketConfig:
    if cfg is None:
        raise SchemaError('rate_limit config must not be None here')

    strategy = cfg.get('strategy', 'token_bucket')
    if strategy != 'token_bucket':
        raise SchemaError("rate_limit.strategy must be 'token_bucket'")

    rps = cfg.get('requests_per_second')
    burst = cfg.get('burst')
    on_limit: OnLimit = cfg.get('on_limit', 'wait')

    if rps is None:
        raise SchemaError('rate_limit.requests_per_second is required')
    if not isinstance(rps, (int, float)) or rps <= 0:
        raise SchemaError('rate_limit.requests_per_second must be a positive float (> 0)')

    if burst is None:
        raise SchemaError('rate_limit.burst is required')
    if not isinstance(burst, int) or burst <= 0:
        raise SchemaError('rate_limit.burst must be a positive int (> 0)')

    if on_limit not in ('wait', 'raise'):
        raise SchemaError("rate_limit.on_limit must be 'wait' or 'raise'")

    clock = cfg.get('clock')
    sleep = cfg.get('sleep')
    if clock is not None and not callable(clock):
        raise SchemaError('rate_limit.clock must be callable if provided')
    if sleep is not None and not callable(sleep):
        raise SchemaError('rate_limit.sleep must be callable if provided')

    return _TokenBucketConfig(
        strategy='token_bucket',
        requests_per_second=float(rps),
        burst=int(burst),
        on_limit=on_limit,
        clock=clock,
        sleep=sleep,
    )
