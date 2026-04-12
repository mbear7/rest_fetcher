from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Literal

EventSource = Literal['live', 'playback']

# internal registry of all event kinds emitted by the library.
# used for on_event_kinds validation — not exported.
_KNOWN_EVENT_KINDS: frozenset[str] = frozenset(
    {
        'adaptive_wait_end',
        'callback_error',
        'error',
        'page_parsed',
        'rate_limit_exceeded',
        'rate_limit_wait_end',
        'rate_limit_wait_start',
        'request_end',
        'request_start',
        'retry',
        'stopped',
    }
)


def _safe_time() -> float:
    try:
        return time.time()
    except Exception:
        return 0.0


def _safe_mono() -> float:
    try:
        return time.monotonic()
    except Exception:
        return 0.0


@dataclass(frozen=True)
class PaginationEvent:
    kind: str
    source: EventSource
    ts: float
    mono: float
    endpoint: str | None = None
    url: str | None = None
    request_index: int | None = None
    attempt: int | None = None
    page_index: int | None = None
    data: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable dict of this event.

        ``mono`` is omitted — it is a process-local monotonic timestamp with no
        meaning outside the current run. All other fields are included, with
        ``None`` values preserved so the key set is stable across event kinds.
        """
        return {
            'kind': self.kind,
            'source': self.source,
            'ts': self.ts,
            'endpoint': self.endpoint,
            'url': self.url,
            'request_index': self.request_index,
            'attempt': self.attempt,
            'page_index': self.page_index,
            'data': self.data,
        }


def now_event(
    *,
    kind: str,
    source: EventSource,
    endpoint: str | None = None,
    url: str | None = None,
    request_index: int | None = None,
    attempt: int | None = None,
    page_index: int | None = None,
    data: dict[str, Any] | None = None,
    ts: float | None = None,
    mono: float | None = None,
) -> PaginationEvent:
    return PaginationEvent(
        kind=kind,
        source=source,
        ts=_safe_time() if ts is None else ts,
        mono=_safe_mono() if mono is None else mono,
        endpoint=endpoint,
        url=url,
        request_index=request_index,
        attempt=attempt,
        page_index=page_index,
        data=data,
    )
