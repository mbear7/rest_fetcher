from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .types import StreamSummary

_UNKNOWN_ENDPOINT = '<unknown>'


@dataclass
class _EndpointAccum:
    runs: int = 0
    failed_runs: int = 0
    requests: int = 0
    retries: int = 0
    pages: int = 0
    bytes_received: int = 0
    retry_bytes_received: int = 0
    wait_seconds: float = 0.0
    elapsed_seconds: float = 0.0


@dataclass(frozen=True)
class EndpointMetrics:
    runs: int = 0
    failed_runs: int = 0
    requests: int = 0
    retries: int = 0
    pages: int = 0
    bytes_received: int = 0
    retry_bytes_received: int = 0
    wait_seconds: float = 0.0
    elapsed_seconds: float = 0.0


@dataclass(frozen=True)
class MetricsSummary:
    total_runs: int = 0
    total_failed_runs: int = 0
    total_requests: int = 0
    total_retries: int = 0
    total_pages: int = 0
    total_bytes_received: int = 0
    total_retry_bytes_received: int = 0
    total_wait_seconds: float = 0.0
    total_elapsed_seconds: float = 0.0
    by_endpoint: dict[str, EndpointMetrics] = field(default_factory=dict)


class MetricsSession:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._total_runs = 0
        self._total_failed_runs = 0
        self._total_requests = 0
        self._total_retries = 0
        self._total_pages = 0
        self._total_bytes_received = 0
        self._total_retry_bytes_received = 0
        self._total_wait_seconds = 0.0
        self._total_elapsed_seconds = 0.0
        self._by_endpoint: dict[str, _EndpointAccum] = {}

    def __repr__(self) -> str:
        snap = self.summary()
        return (
            'MetricsSession('
            f'total_runs={snap.total_runs}, '
            f'total_failed_runs={snap.total_failed_runs}, '
            f'total_requests={snap.total_requests}, '
            f'total_retries={snap.total_retries}, '
            f'total_pages={snap.total_pages}, '
            f'total_bytes_received={snap.total_bytes_received}, '
            f'total_retry_bytes_received={snap.total_retry_bytes_received}, '
            f'total_wait_seconds={snap.total_wait_seconds}, '
            f'total_elapsed_seconds={snap.total_elapsed_seconds}, '
            f'by_endpoint={{{len(snap.by_endpoint)} endpoint(s)}}'
            ')'
        )

    def _snapshot_unlocked(self) -> MetricsSummary:
        by_endpoint = {
            k: EndpointMetrics(
                runs=ep.runs,
                failed_runs=ep.failed_runs,
                requests=ep.requests,
                retries=ep.retries,
                pages=ep.pages,
                bytes_received=ep.bytes_received,
                retry_bytes_received=ep.retry_bytes_received,
                wait_seconds=ep.wait_seconds,
                elapsed_seconds=ep.elapsed_seconds,
            )
            for k, ep in self._by_endpoint.items()
        }
        return MetricsSummary(
            total_runs=self._total_runs,
            total_failed_runs=self._total_failed_runs,
            total_requests=self._total_requests,
            total_retries=self._total_retries,
            total_pages=self._total_pages,
            total_bytes_received=self._total_bytes_received,
            total_retry_bytes_received=self._total_retry_bytes_received,
            total_wait_seconds=self._total_wait_seconds,
            total_elapsed_seconds=self._total_elapsed_seconds,
            by_endpoint=by_endpoint,
        )

    def summary(self) -> MetricsSummary:
        with self._lock:
            return self._snapshot_unlocked()

    def reset(self) -> MetricsSummary:
        with self._lock:
            snap = self._snapshot_unlocked()
            self._total_runs = 0
            self._total_failed_runs = 0
            self._total_requests = 0
            self._total_retries = 0
            self._total_pages = 0
            self._total_bytes_received = 0
            self._total_retry_bytes_received = 0
            self._total_wait_seconds = 0.0
            self._total_elapsed_seconds = 0.0
            self._by_endpoint = {}
            return snap

    def _record(self, summary: StreamSummary, *, failed: bool) -> None:
        with self._lock:
            self._total_runs += 1
            if failed:
                self._total_failed_runs += 1
            self._total_requests += summary.requests
            self._total_retries += summary.retries
            self._total_pages += summary.pages
            self._total_bytes_received += summary.bytes_received
            self._total_retry_bytes_received += summary.retry_bytes_received
            self._total_wait_seconds += summary.total_wait_seconds
            self._total_elapsed_seconds += summary.elapsed_seconds

            key = summary.endpoint if summary.endpoint is not None else _UNKNOWN_ENDPOINT
            if key not in self._by_endpoint:
                self._by_endpoint[key] = _EndpointAccum()
            ep = self._by_endpoint[key]
            ep.runs += 1
            if failed:
                ep.failed_runs += 1
            ep.requests += summary.requests
            ep.retries += summary.retries
            ep.pages += summary.pages
            ep.bytes_received += summary.bytes_received
            ep.retry_bytes_received += summary.retry_bytes_received
            ep.wait_seconds += summary.total_wait_seconds
            ep.elapsed_seconds += summary.elapsed_seconds
