from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from types import MappingProxyType
from typing import Any

import requests

from ._fetch_job import _FetchJob
from ._job_config import _LOG_LEVELS, _has_token_bucket_config
from .auth import build_auth_handler
from .exceptions import SchemaError
from .metrics import MetricsSession
from .rate_limit import TokenBucket, build_token_bucket
from .schema import validate
from .types import (
    FetchResult,
    StreamItem,
    StreamRun,
    StreamSummary,
)

logger = logging.getLogger('rest_fetcher.client')


class _StreamRunImpl:
    def __init__(self, iterator: Iterator[StreamItem], summary_box: dict[str, StreamSummary]):
        self._iterator = iterator
        self._summary_box = summary_box

    @property
    def summary(self) -> StreamSummary | None:
        return self._summary_box.get('summary')

    def __iter__(self) -> Iterator[StreamItem]:
        return self

    def __next__(self) -> StreamItem:
        return next(self._iterator)


class APIClient:
    """
    main entry point. constructed once per api host, reused across calls.

    IMPORTANT — timeouts:
        requests has no default timeout. without an explicit timeout, requests will
        hang indefinitely on a slow or unresponsive host. always set a timeout:
            APIClient({'base_url': ..., 'timeout': 30, ...})
        per-endpoint override: {'path': '/slow', 'timeout': 120}
        tuple form for connect vs read: 'timeout': (5, 60)  # (connect, read)

    basic usage:
        client = APIClient({
            'base_url': 'https://api.example.com/v1',
            'auth': {'type': 'bearer', 'token': 'my-token'},
            'endpoints': {
                'list_users': {
                    'method': 'GET',
                    'path': '/users',
                    'pagination': cursor_pagination('cursor', 'meta.next_cursor')
                }
            }
        })

        # fetch all pages into memory
        users = client.fetch('list_users')

        # or stream page by page
        for page in client.stream('list_users'):
            save_to_db(page)

        # pass call-time overrides for params, headers, body
        users = client.fetch('list_users', params={'status': 'active'})
    """

    def __init__(self, schema):
        self._schema = validate(schema)
        self._session = requests.Session()

        metrics_cfg = self._schema.get('metrics')
        if metrics_cfg is True:
            self.metrics: MetricsSession | None = MetricsSession()
        elif metrics_cfg in (False, None):
            self.metrics = None
        else:
            self.metrics = metrics_cfg

        # apply session_config (verify, cert, proxies, max_redirects)
        session_config = schema.get('session_config', {})
        if 'verify' in session_config:
            self._session.verify = session_config['verify']
        if 'cert' in session_config:
            self._session.cert = session_config['cert']
        if 'proxies' in session_config:
            self._session.proxies.update(session_config['proxies'])
        if 'max_redirects' in session_config:
            self._session.max_redirects = session_config['max_redirects']

        # apply schema-level headers to session so every request inherits them
        if 'headers' in schema:
            self._session.headers.update(schema['headers'])

        self._timeout = schema.get('timeout', 30)
        self._state = dict(schema.get('state', {}))
        self._config_view = MappingProxyType(self._state)
        self._auth = build_auth_handler(
            schema.get('auth'), config_view=self._config_view, timeout=self._timeout
        )
        self._retry_config = schema.get('retry')
        self._token_buckets: dict[tuple[str, str | None], TokenBucket] = {}
        self._token_bucket_lock = threading.Lock()

        log_level_key = schema.get('log_level', 'medium')
        self._log_level = _LOG_LEVELS.get(log_level_key, logging.INFO)
        logger.setLevel(self._log_level)

        logger.info(
            'APIClient ready — base_url=%s endpoints=%s',
            schema['base_url'],
            list(schema['endpoints'].keys()),
        )

    def _get_shared_token_bucket(
        self, endpoint_name: str, rate_limit_source: str, rate_limit_cfg: dict[str, Any] | None
    ) -> TokenBucket | None:
        if not _has_token_bucket_config(rate_limit_cfg):
            return None
        key: tuple[str, str | None]
        if rate_limit_source == 'client':
            key = ('client', None)
        elif rate_limit_source == 'endpoint':
            key = ('endpoint', endpoint_name)
        else:
            return None
        with self._token_bucket_lock:
            bucket = self._token_buckets.get(key)
            if bucket is None:
                assert rate_limit_cfg is not None
                cfg = build_token_bucket(rate_limit_cfg)
                bucket = TokenBucket(
                    cfg.requests_per_second, cfg.burst, clock=cfg.clock, sleep=cfg.sleep
                )
                self._token_buckets[key] = bucket
            return bucket

    def _make_job(self, endpoint_name, call_params):
        "creates a fresh _FetchJob for a single call"
        if endpoint_name not in self._schema['endpoints']:
            raise SchemaError(f'unknown endpoint: {endpoint_name!r}')
        return _FetchJob(
            self._schema,
            endpoint_name,
            self._session,
            self._auth,
            self._retry_config,
            call_params,
            dict(self._state),
            self._get_shared_token_bucket,
            self.metrics,
        )

    def fetch(self, endpoint_name: str, **call_params: Any) -> FetchResult:
        """
        fetches the endpoint and returns the final parsed/processed result.

        In paginated mode this is normally the aggregated page result.
        In non-paginated mode this is normally the single parsed page result.
        If on_complete is configured in fetch() mode, its return value becomes
        the final fetch() result.

        call_params kwargs: params, headers, body, form, files, path_params,
                            canonical_parser, response_format, csv_delimiter,
                            encoding, scrub_headers, scrub_query_params, rate_limit,
                            max_pages, max_requests, time_limit
            client.fetch('list_users', params={'status': 'active'})
            client.fetch('events', max_pages=10, time_limit=30.0)
        """
        max_pages = call_params.pop('max_pages', None)
        max_requests = call_params.pop('max_requests', None)
        time_limit = call_params.pop('time_limit', None)
        return self._make_job(endpoint_name, call_params)._collect(
            max_pages=max_pages, max_requests=max_requests, time_limit=time_limit
        )

    def fetch_pages(self, endpoint_name: str, **call_params: Any) -> list[StreamItem]:
        """Materialize stream() into a list — always returns a list of yielded page items.

        fetch_pages() is equivalent to ``list(client.stream(endpoint_name, **call_params))``.
        Unlike fetch(), the result is always a list regardless of how many pages were returned.
        fetch() unwraps single-page results to the bare page value; fetch_pages() never does.

        on_complete follows stream semantics here: it fires at normal completion with
        ``(StreamSummary, state)`` and its return value is ignored. There is no
        data-transformation via on_complete — use fetch() if you need that behavior.

        optional safety caps:
            max_pages, max_requests — stop cleanly and preserve collected pages.
            time_limit — remains destructive and raises DeadlineExceeded.
        """
        max_pages = call_params.pop('max_pages', None)
        max_requests = call_params.pop('max_requests', None)
        time_limit = call_params.pop('time_limit', None)
        return list(
            self._make_job(endpoint_name, call_params)._generate(
                max_pages=max_pages, max_requests=max_requests, time_limit=time_limit
            )
        )

    def stream(self, endpoint_name: str, **call_params: Any) -> Iterator[StreamItem]:
        """
        returns a generator that yields parsed/processed page values incrementally.

        The yielded value shape depends on response_parser and on_response
        transformations; it is not limited to dict/list payloads. If on_complete
        is configured in stream() mode, it receives StreamSummary at normal
        completion and its return value is ignored.

            for page in client.stream('list_events', params={'type': 'click'}):
                load_to_warehouse(page)

        optional safety caps:
            max_pages, max_requests — stop cleanly and preserve yielded pages.
            time_limit — remains destructive and raises DeadlineExceeded.
        """
        max_pages = call_params.pop('max_pages', None)
        max_requests = call_params.pop('max_requests', None)
        time_limit = call_params.pop('time_limit', None)
        return self._make_job(endpoint_name, call_params)._generate(
            max_pages=max_pages, max_requests=max_requests, time_limit=time_limit
        )

    def stream_run(self, endpoint_name: str, **call_params: Any) -> StreamRun:
        """streams pages and exposes a completion summary after exhaustion.

        stream_run() returns an iterable wrapper. Iterate it like stream(), then
        inspect run.summary (StreamSummary | None).

        Notes:
            - summary is set only on normal completion (natural exhaustion or
              non-destructive caps like max_pages/max_requests).
            - if you stop iterating early, or if a destructive error occurs
              (e.g. DeadlineExceeded), summary remains None.
        """
        max_pages = call_params.pop('max_pages', None)
        max_requests = call_params.pop('max_requests', None)
        time_limit = call_params.pop('time_limit', None)

        job = self._make_job(endpoint_name, call_params)
        summary_box: dict[str, StreamSummary] = {}

        def summary_sink(summary: StreamSummary) -> None:
            summary_box['summary'] = summary

        iterator = job._generate(
            max_pages=max_pages,
            max_requests=max_requests,
            time_limit=time_limit,
            summary_sink=summary_sink,
        )
        return _StreamRunImpl(iterator, summary_box)

    def close(self) -> None:
        "closes the underlying requests session. call when done, or use as context manager."
        self._session.close()

    def __enter__(self) -> APIClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
