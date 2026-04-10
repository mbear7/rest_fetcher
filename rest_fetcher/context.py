from __future__ import annotations

import time

from .exceptions import DeadlineExceeded
from .types import StopSignal


class OperationContext:
    """
    shared operation state passed through the request execution and pagination layers.
    created once per fetch()/stream() call; not reused across calls.

    tracks:
        started_at:    time.monotonic() at construction — deadline checks use this
        request_count: total http attempts made (first try + all retries)
        page_count:    completed pages (response received, parsed, callbacks done)

    limits (all optional / None means unlimited):
        max_pages:    return StopSignal before fetching the page that would exceed this
        max_requests: return StopSignal before the attempt that would exceed this
        time_limit: raise DeadlineExceeded when elapsed >= this, checked before each
                      page fetch and before each http attempt (so backoff counts toward it)

    all times use time.monotonic() — immune to wall-clock adjustments.
    """

    __slots__ = (
        'started_at',
        'request_count',
        'page_count',
        'max_pages',
        'max_requests',
        'time_limit',
    )

    def __init__(
        self,
        max_pages: int | None = None,
        max_requests: int | None = None,
        time_limit: float | None = None,
    ) -> None:
        self.started_at: float = time.monotonic()
        self.request_count: int = 0
        self.page_count: int = 0
        self.max_pages = max_pages
        self.max_requests = max_requests
        self.time_limit = time_limit

    # ------------------------------------------------------------------
    # counters — called after the action completes
    # ------------------------------------------------------------------

    def record_request(self) -> None:
        "increment request_count after an http attempt is issued"
        self.request_count += 1

    def record_page(self) -> None:
        "increment page_count after all callbacks for a page have completed"
        self.page_count += 1

    # ------------------------------------------------------------------
    # guards — called BEFORE the action that would exceed the limit
    # ------------------------------------------------------------------

    def elapsed(self) -> float:
        return time.monotonic() - self.started_at

    def check_deadline(self) -> None:
        """raise DeadlineExceeded if time_limit is set and elapsed >= limit"""
        if self.time_limit is None:
            return
        e = self.elapsed()
        if e >= self.time_limit:
            raise DeadlineExceeded(limit=self.time_limit, elapsed=e)

    def check_max_requests(self) -> StopSignal | None:
        """return StopSignal if the next attempt would exceed max_requests"""
        if self.max_requests is None:
            return None
        if self.request_count >= self.max_requests:
            return StopSignal(
                kind='max_requests',
                limit=self.max_requests,
                observed=self.request_count,
            )
        return None

    def check_max_pages(self) -> StopSignal | None:
        """return StopSignal if the next page fetch would exceed max_pages"""
        if self.max_pages is None:
            return None
        if self.page_count >= self.max_pages:
            return StopSignal(
                kind='max_pages',
                limit=self.max_pages,
                observed=self.page_count,
            )
        return None
