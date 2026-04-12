from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from .events import EventSource, PaginationEvent
from .types import PageCycleOutcome, StopSignal, StreamSummary


@dataclass
class _RunState:
    mock_idx: int = 0
    playback_pages: list[Any] = field(default_factory=list)
    page_state: dict[str, Any] = field(default_factory=dict)
    event_source: EventSource = 'live'
    endpoint_name: str | None = None
    emit_event: Callable[[PaginationEvent], None] = lambda event: None
    requests_so_far: int = 0
    pages_so_far: int = 0
    retries_so_far: int = 0
    proactive_waits_so_far: int = 0
    proactive_wait_seconds_so_far: float = 0.0
    reactive_waits_so_far: int = 0
    reactive_wait_seconds_so_far: float = 0.0
    retry_wait_seconds_so_far: float = 0.0
    rate_limit_wait_seconds_so_far: float = 0.0
    adaptive_waits_so_far: int = 0
    adaptive_wait_seconds_so_far: float = 0.0
    static_wait_seconds_so_far: float = 0.0
    errors_so_far: int = 0
    bytes_received_so_far: int = 0
    retry_bytes_received_so_far: int = 0
    run_started_mono: float = field(default_factory=time.monotonic)
    last_request_started_mono: float | None = None
    previous_request_started_mono: float | None = None
    # per-page cycle accumulators — reset by reset_page_cycle(), read by build_page_cycle_outcome()
    _page_attempts: int = 0
    _page_cycle_started_mono: float | None = None
    _page_cycle_last_response_mono: float | None = None
    _page_retry_bytes_received: int = 0
    # last on_error outcome for the current page cycle — reset at cycle start
    _last_error: Exception | None = None
    _last_error_action: str | None = None  # 'skip' | 'stop' | None

    def reset_page_cycle(self) -> None:
        self._page_attempts = 0
        self._page_cycle_started_mono = None
        self._page_cycle_last_response_mono = None
        self._page_retry_bytes_received = 0
        self._last_error = None
        self._last_error_action = None

    def record_attempt_elapsed(self, elapsed_ms: float | None, *, now: float | None = None) -> None:
        if now is not None:
            self._page_cycle_last_response_mono = now

    def mark_request_start(self, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        if self._page_cycle_started_mono is None:
            self._page_cycle_started_mono = now
        self._page_attempts += 1
        self.previous_request_started_mono = self.last_request_started_mono
        self.last_request_started_mono = now
        self.requests_so_far += 1

    def mark_retry(self) -> None:
        self.retries_so_far += 1

    def mark_page_complete(self) -> None:
        self.pages_so_far += 1

    def mark_wait(self, wait_type: str, seconds: float, *, cause: str | None = None) -> None:
        if wait_type == 'proactive':
            self.proactive_waits_so_far += 1
            self.proactive_wait_seconds_so_far += seconds
            self.rate_limit_wait_seconds_so_far += seconds
        elif wait_type == 'reactive':
            self.reactive_waits_so_far += 1
            self.reactive_wait_seconds_so_far += seconds
            if cause == 'backoff':
                self.retry_wait_seconds_so_far += seconds
            elif cause in {'retry_after', 'min_delay'}:
                self.rate_limit_wait_seconds_so_far += seconds
            else:
                raise ValueError(f'unknown reactive wait cause: {cause!r}')
        elif wait_type == 'adaptive':
            self.adaptive_waits_so_far += 1
            self.adaptive_wait_seconds_so_far += seconds
        elif wait_type == 'static':
            self.static_wait_seconds_so_far += seconds
        else:
            raise ValueError(f'unknown wait_type: {wait_type!r}')

    def mark_error(self) -> None:
        self.errors_so_far += 1

    def mark_bytes_received(self, n: int) -> None:
        self.bytes_received_so_far += n

    def mark_retry_bytes_received(self, n: int) -> None:
        self.retry_bytes_received_so_far += n
        self._page_retry_bytes_received += n

    def is_retry_attempt(self) -> bool:
        return self._page_attempts > 1

    def current_page_retry_bytes_received(self) -> int:
        return self._page_retry_bytes_received

    def elapsed_seconds(self, now: float | None = None) -> float:
        if now is None:
            now = time.monotonic()
        return max(0.0, now - self.run_started_mono)

    def seconds_since_last_request(self, now: float | None = None) -> float | None:
        if self.previous_request_started_mono is None:
            return None
        if now is None:
            now = time.monotonic()
        return max(0.0, now - self.previous_request_started_mono)

    def expose(self, page_state: dict[str, Any], now: float | None = None) -> None:
        page_state.update(self.progress_fields(now))

    def progress_fields(self, now: float | None = None) -> dict[str, Any]:
        now = time.monotonic() if now is None else now
        return {
            'requests_so_far': self.requests_so_far,
            'pages_so_far': self.pages_so_far,
            'retries_so_far': self.retries_so_far,
            'elapsed_seconds_so_far': self.elapsed_seconds(now),
            'proactive_waits_so_far': self.proactive_waits_so_far,
            'proactive_wait_seconds_so_far': self.proactive_wait_seconds_so_far,
            'reactive_waits_so_far': self.reactive_waits_so_far,
            'reactive_wait_seconds_so_far': self.reactive_wait_seconds_so_far,
            'adaptive_waits_so_far': self.adaptive_waits_so_far,
            'adaptive_wait_seconds_so_far': self.adaptive_wait_seconds_so_far,
            'errors_so_far': self.errors_so_far,
            'bytes_received_so_far': self.bytes_received_so_far,
            'retry_bytes_received_so_far': self.retry_bytes_received_so_far,
            'seconds_since_last_request': self.seconds_since_last_request(now),
        }

    def request_end_event_data(
        self,
        *,
        status_code: int | None,
        elapsed_ms: float,
        bytes_received: int = 0,
        retry_bytes_received: int = 0,
        now: float | None = None,
    ) -> dict[str, Any]:
        self.record_attempt_elapsed(elapsed_ms, now=now)
        return {
            'status_code': status_code,
            'elapsed_ms': elapsed_ms,
            'bytes_received': bytes_received,
            'retry_bytes_received': retry_bytes_received,
            **self.progress_fields(now),
        }

    def build_page_cycle_outcome(
        self,
        *,
        status_code: int | None,
        error: Exception | None,
        stop_signal: StopSignal | None,
    ) -> PageCycleOutcome:
        kind: Literal['success', 'skipped', 'stopped'] = 'success'
        if stop_signal is not None:
            kind = 'stopped'
        elif error is not None and self._last_error_action == 'skip':
            kind = 'skipped'
        cycle_elapsed_ms = None
        if (
            self._page_cycle_started_mono is not None
            and self._page_cycle_last_response_mono is not None
        ):
            cycle_elapsed_ms = max(
                0.0, (self._page_cycle_last_response_mono - self._page_cycle_started_mono) * 1000.0
            )
        return PageCycleOutcome(
            kind=kind,
            status_code=status_code,
            error=error,
            stop_signal=stop_signal,
            attempts_for_page=self._page_attempts,
            cycle_elapsed_ms=cycle_elapsed_ms,
        )

    def page_parsed_event_data(
        self, *, status_code: int | None = None, now: float | None = None
    ) -> dict[str, Any]:
        data = self.progress_fields(now)
        data['status_code'] = status_code
        data['pages_so_far'] = self.pages_so_far + 1
        return data

    def stop_event_data(self, stop: StopSignal, now: float | None = None) -> dict[str, Any]:
        data = self.progress_fields(now)
        data.update(
            {
                'stop': {
                    'kind': stop.kind,
                    'limit': stop.limit,
                    'observed': stop.observed,
                },
                'stop_kind': stop.kind,
                'observed': stop.observed,
                'limit': stop.limit,
            }
        )
        return data

    def wait_event_data(
        self,
        wait_type: str,
        *,
        wait_ms: float,
        now: float | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        data = {'wait_type': wait_type, 'wait_ms': wait_ms}
        if wait_type == 'proactive':
            data.update(
                {
                    'proactive_waits_so_far': self.proactive_waits_so_far,
                    'proactive_wait_seconds_so_far': self.proactive_wait_seconds_so_far,
                }
            )
        elif wait_type == 'reactive':
            data.update(
                {
                    'reactive_waits_so_far': self.reactive_waits_so_far,
                    'reactive_wait_seconds_so_far': self.reactive_wait_seconds_so_far,
                }
            )
        elif wait_type == 'adaptive':
            data.update(
                {
                    'adaptive_waits_so_far': self.adaptive_waits_so_far,
                    'adaptive_wait_seconds_so_far': self.adaptive_wait_seconds_so_far,
                }
            )
        else:
            raise ValueError(f'unknown wait_type: {wait_type!r}')
        if extra:
            data.update(extra)
        return data

    def build_summary(self, stop: StopSignal | None = None) -> StreamSummary:
        retry_wait_seconds = self.retry_wait_seconds_so_far
        rate_limit_wait_seconds = self.rate_limit_wait_seconds_so_far
        adaptive_wait_seconds = self.adaptive_wait_seconds_so_far
        static_wait_seconds = self.static_wait_seconds_so_far
        return StreamSummary(
            pages=self.pages_so_far,
            requests=self.requests_so_far,
            stop=stop,
            endpoint=self.endpoint_name,
            source=self.event_source,
            retries=self.retries_so_far,
            elapsed_seconds=self.elapsed_seconds(),
            retry_wait_seconds=retry_wait_seconds,
            rate_limit_wait_seconds=rate_limit_wait_seconds,
            adaptive_wait_seconds=adaptive_wait_seconds,
            static_wait_seconds=static_wait_seconds,
            total_wait_seconds=retry_wait_seconds
            + rate_limit_wait_seconds
            + adaptive_wait_seconds
            + static_wait_seconds,
            bytes_received=self.bytes_received_so_far,
            retry_bytes_received=self.retry_bytes_received_so_far,
        )
