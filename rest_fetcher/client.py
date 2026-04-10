from __future__ import annotations

import inspect
import logging
import re
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal

import requests

from .auth import build_auth_handler
from .callbacks import safe_call
from .context import OperationContext
from .events import EventSource, PaginationEvent, now_event
from .exceptions import (
    CallbackError,
    PlaybackError,
    RateLimitExceeded,
    RequestError,
    ResponseError,
    SchemaError,
)
from .metrics import MetricsSession
from .pagination import CycleRunner, StateView, build_cycle_runner
from .parsing import default_parse_response, serialize_response_for_playback
from .playback import PlaybackHandler, build_playback_handler, deserialize_playback_response
from .rate_limit import TokenBucket, build_token_bucket
from .retry import build_retry_handler
from .schema import merge_dicts, resolve_endpoint, validate
from .types import (
    CanonicalParserFn,
    FetchResult,
    PageCycleOutcome,
    ResponseParser,
    StopSignal,
    StreamItem,
    StreamRun,
    StreamSummary,
)

logger = logging.getLogger('rest_fetcher.client')

RequestKwargs = dict[str, Any]
HeaderMap = dict[str, Any]
TimeoutValue = float | tuple[float, float]

_LOG_LEVELS = {
    'none':    logging.CRITICAL + 1,
    'error':   logging.ERROR,
    'medium':  logging.INFO,
    'verbose': logging.DEBUG,
}

_MISSING = object()


def _enrich_exc(exc: RequestError, endpoint: str, method: str, url: str) -> None:
    'attach request context to a RequestError if not already set.'
    if exc.endpoint is None:
        exc.endpoint = endpoint
    if exc.method is None:
        exc.method = method
    if exc.url is None:
        exc.url = url


# default set of header names (lowercase) whose values are always redacted in logs
_SCRUB_HEADERS_DEFAULT = {
    'authorization', 'x-api-key', 'x-api-secret', 'x-auth-token',
    'api-key', 'proxy-authorization', 'cookie', 'set-cookie',
}

# substring patterns: any header key containing one of these (case-insensitive) is scrubbed
_SCRUB_PATTERNS = ('token', 'secret', 'password', 'key', 'auth')

_SCRUB_QUERY_PARAMS_DEFAULT = {
    'access_token', 'refresh_token', 'api_key', 'client_secret',
    'token', 'sig', 'signature',
}
_SCRUB_QUERY_PATTERNS = ('token', 'secret', 'key', 'sig', 'auth')


def _scrub(headers: HeaderMap, extra_scrub: list[str] | None = None) -> HeaderMap:
    '''
    redacts sensitive header values before logging.
    exact-match: _SCRUB_HEADERS_DEFAULT + schema scrub_headers list.
    pattern-match: any header key containing 'token', 'secret', 'password', 'key', 'auth'.
    '''
    exact = _SCRUB_HEADERS_DEFAULT | {h.lower() for h in (extra_scrub or [])}
    result = {}
    for k, v in headers.items():
        k_lower = k.lower()
        if k_lower in exact or any(p in k_lower for p in _SCRUB_PATTERNS):
            result[k] = '***'
        else:
            result[k] = v
    return result



def _scrub_recorded_url(url: str, extra_scrub: list[str] | None = None) -> str:
    '''
    redact sensitive query parameter values in recorded playback URLs.
    exact-match: built-in query defaults + schema scrub_query_params list.
    pattern-match: any query key containing one of _SCRUB_QUERY_PATTERNS.
    '''
    from urllib.parse import parse_qsl, urlencode, urlsplit

    if not url:
        return url
    exact = _SCRUB_QUERY_PARAMS_DEFAULT | {h.lower() for h in (extra_scrub or [])}
    parts = urlsplit(url)
    if not parts.query:
        return url
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    scrubbed = []
    for k, v in pairs:
        k_lower = k.lower()
        if k_lower in exact or any(p in k_lower for p in _SCRUB_QUERY_PATTERNS):
            scrubbed.append((k, '[REDACTED]'))
        else:
            scrubbed.append((k, v))
    encoded = urlencode(scrubbed, doseq=True).replace('%5B', '[').replace('%5D', ']')
    return parts._replace(query=encoded).geturl()


def _scrub_playback_envelope(
    envelope: dict[str, Any],
    extra_headers: list[str] | None = None,
    extra_query_params: list[str] | None = None,
) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        return envelope
    if 'request_headers' in envelope and isinstance(envelope.get('request_headers'), dict):
        scrubbed = _scrub(envelope['request_headers'], extra_headers)
        if scrubbed != envelope['request_headers']:
            envelope = {**envelope, 'request_headers': scrubbed}
    if envelope.get('url'):
        scrubbed_url = _scrub_recorded_url(envelope['url'], extra_query_params)
        if scrubbed_url != envelope['url']:
            envelope = {**envelope, 'url': scrubbed_url}
    return envelope


def _resolve(key: str, *dicts: dict[str, Any], default: Any = None) -> Any:
    '''resolve a config value by priority, skipping None.

    priority order is left-to-right; the first non-None value wins.
    '''
    for d in dicts:
        v = d.get(key)
        if v is not None:
            return v
    return default


def _resolve_explicit(key: str, *dicts: dict[str, Any], default: Any = None) -> Any:
    '''resolve a config value by priority, preserving explicit None.

    priority order is left-to-right; the first dict that defines the key wins,
    even when the value is None. This is used for settings where `None` is an
    intentional override rather than "keep searching".
    '''
    for d in dicts:
        if key in d:
            return d[key]
    return default


_TOKEN_BUCKET_KEYS = {'strategy', 'requests_per_second', 'burst', 'on_limit', 'clock', 'sleep'}


def _has_token_bucket_config(cfg: dict[str, Any] | None) -> bool:
    return isinstance(cfg, dict) and any(k in cfg for k in _TOKEN_BUCKET_KEYS)


def _parser_arity(fn):
    '''
    inspects callable signature once at construction time to determine whether
    the response parser expects 1 arg (response) or 2 args (response, parsed).
    handles functions, lambdas, methods, and callable instances.
    returns 1 or 2; defaults to 1 on any inspection failure.
    '''
    try:
        sig = inspect.signature(fn)
        # count all positional-capable parameters (required or optional) —
        # a parser declaring 'def p(resp, parsed=None)' intends to receive parsed
        positional_capable = [
            p for p in sig.parameters.values()
            if p.kind in (inspect.Parameter.POSITIONAL_ONLY,
                          inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        # *args can also accept a second positional argument
        has_var_positional = any(
            p.kind is inspect.Parameter.VAR_POSITIONAL
            for p in sig.parameters.values()
        )
        return 2 if (len(positional_capable) >= 2 or has_var_positional) else 1
    except (ValueError, TypeError):
        return 1




@dataclass(frozen=True)
class _ResolvedJobConfig:
    base_url: str
    extra_scrub: list[str]
    extra_scrub_query: list[str]
    log_level: int
    debug: bool
    timeout: TimeoutValue
    response_format: str
    csv_delimiter: str
    encoding: str
    response_parser: ResponseParser | None
    canonical_parser: CanonicalParserFn | None
    runner: CycleRunner
    on_error: Callable[..., Any] | None
    on_event: Callable[[PaginationEvent], None] | None
    on_event_kinds: frozenset[str] | None
    on_request: Callable[..., Any] | None
    rate_limit: dict[str, Any] | None
    rate_limit_source: str
    rate_limit_disabled: bool
    retry_config: Any
    mock: Any
    playback: PlaybackHandler | None
    record_as_bytes: bool


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

    def request_end_event_data(self, *, status_code: int | None, elapsed_ms: float, bytes_received: int = 0, retry_bytes_received: int = 0, now: float | None = None) -> dict[str, Any]:
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
        from .types import PageCycleOutcome
        kind: Literal['success', 'skipped', 'stopped'] = 'success'
        if stop_signal is not None:
            kind = 'stopped'
        elif error is not None and self._last_error_action == 'skip':
            kind = 'skipped'
        cycle_elapsed_ms = None
        if self._page_cycle_started_mono is not None and self._page_cycle_last_response_mono is not None:
            cycle_elapsed_ms = max(0.0, (self._page_cycle_last_response_mono - self._page_cycle_started_mono) * 1000.0)
        return PageCycleOutcome(
            kind=kind,
            status_code=status_code,
            error=error,
            stop_signal=stop_signal,
            attempts_for_page=self._page_attempts,
            cycle_elapsed_ms=cycle_elapsed_ms,
        )

    def page_parsed_event_data(self, *, status_code: int | None = None, now: float | None = None) -> dict[str, Any]:
        data = self.progress_fields(now)
        data['status_code'] = status_code
        data['pages_so_far'] = self.pages_so_far + 1
        return data

    def stop_event_data(self, stop: StopSignal, now: float | None = None) -> dict[str, Any]:
        data = self.progress_fields(now)
        data.update({
            'stop': {
                'kind': stop.kind,
                'limit': stop.limit,
                'observed': stop.observed,
            },
            'stop_kind': stop.kind,
            'observed': stop.observed,
            'limit': stop.limit,
        })
        return data

    def wait_event_data(self, wait_type: str, *, wait_ms: float, now: float | None = None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        data = {'wait_type': wait_type, 'wait_ms': wait_ms}
        if wait_type == 'proactive':
            data.update({
                'proactive_waits_so_far': self.proactive_waits_so_far,
                'proactive_wait_seconds_so_far': self.proactive_wait_seconds_so_far,
            })
        elif wait_type == 'reactive':
            data.update({
                'reactive_waits_so_far': self.reactive_waits_so_far,
                'reactive_wait_seconds_so_far': self.reactive_wait_seconds_so_far,
            })
        elif wait_type == 'adaptive':
            data.update({
                'adaptive_waits_so_far': self.adaptive_waits_so_far,
                'adaptive_wait_seconds_so_far': self.adaptive_wait_seconds_so_far,
            })
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
            total_wait_seconds=retry_wait_seconds + rate_limit_wait_seconds + adaptive_wait_seconds + static_wait_seconds,
            bytes_received=self.bytes_received_so_far,
            retry_bytes_received=self.retry_bytes_received_so_far,
        )


@dataclass(frozen=True)
class _RequestOutcome:
    parsed_body: Any
    parsed: Any
    headers: HeaderMap
    request_kwargs: RequestKwargs


class _FetchJob:
    '''
    runtime object for a single endpoint fetch — created per call by APIClient, not reused.
    orchestrates auth, request building, retry, response parsing,
    pagination, and all user callbacks.
    '''

    def __init__(
        self,
        client_schema: dict[str, Any],
        endpoint_name: str,
        session: requests.Session,
        auth_handler: Any,
        retry_config: Any,
        call_params: dict[str, Any] | None = None,
        initial_state: dict[str, Any] | None = None,
        shared_token_bucket_provider: Callable[[str, str, dict[str, Any] | None], TokenBucket | None] | None = None,
        metrics_session: MetricsSession | None = None,
    ) -> None:
        self._endpoint_name = endpoint_name
        self._session = session
        self._auth = auth_handler
        self._call_params = call_params or {}
        self._retry_config = retry_config
        self._initial_state = dict(initial_state or {})
        self._shared_token_bucket_provider = shared_token_bucket_provider
        self._metrics_session = metrics_session

        self._cfg = resolve_endpoint(client_schema, endpoint_name)
        resolved = self._resolve_config(client_schema, self._cfg, self._call_params)

        self._rate_limit = resolved.rate_limit
        self._rate_limit_source = resolved.rate_limit_source
        self._token_bucket = None
        self._rate_limit_cfg = build_token_bucket(self._rate_limit) if self._rate_limit is not None and _has_token_bucket_config(self._rate_limit) else None
        self._rate_limit_rps = self._rate_limit_cfg.requests_per_second if self._rate_limit_cfg is not None else None
        self._rate_limit_burst = self._rate_limit_cfg.burst if self._rate_limit_cfg is not None else None
        self._rate_limit_on_limit = self._rate_limit_cfg.on_limit if self._rate_limit_cfg is not None else 'wait'

        self._base_url = resolved.base_url
        self._extra_scrub = resolved.extra_scrub
        self._extra_scrub_query = resolved.extra_scrub_query
        self._log_level = resolved.log_level
        self._debug = resolved.debug
        self._timeout = resolved.timeout
        self._response_format = resolved.response_format
        self._csv_delimiter = resolved.csv_delimiter
        self._encoding = resolved.encoding
        self._response_parser = resolved.response_parser
        self._canonical_parser = resolved.canonical_parser
        self._parser_arity = _parser_arity(self._response_parser) if self._response_parser else 1
        self._runner = resolved.runner
        self._on_error = resolved.on_error
        self._on_event = resolved.on_event
        self._on_event_kinds = resolved.on_event_kinds
        self._on_request = resolved.on_request
        self._event_error_logged = False
        self._mock = resolved.mock
        self._playback = resolved.playback
        self._record_as_bytes = resolved.record_as_bytes
        self._retry_config = resolved.retry_config
        retry_rate_limit = {'respect_retry_after': False, 'min_delay': 0.0} if resolved.rate_limit_disabled else resolved.rate_limit
        effective_retry_config = self._retry_config
        if effective_retry_config is None:
            effective_retry_config = dict(client_schema.get('retry', {}) or {})
            effective_retry_config.update({'max_attempts': 1, 'reactive_wait_on_terminal': True})
        elif effective_retry_config is _MISSING:
            effective_retry_config = None
        self._retry = build_retry_handler(effective_retry_config, retry_rate_limit)
        if self._shared_token_bucket_provider is not None and self._rate_limit_cfg is not None:
            self._token_bucket = self._shared_token_bucket_provider(self._endpoint_name, self._rate_limit_source, self._rate_limit)

    def _current_stop_signal(self, run_state: _RunState) -> StopSignal | None:
        stop = run_state.page_state.get('_stop_signal') if isinstance(run_state.page_state, dict) else None
        return stop if isinstance(stop, StopSignal) else None

    def _current_final_state(self, run_state: _RunState) -> StateView:
        page_state = run_state.page_state if isinstance(run_state.page_state, dict) else {}
        final_page_state = {
            k: v for k, v in page_state.items()
            if k not in {'_completed_pages', '_request', '_response_headers', '_stop_signal'}
        }
        return StateView(final_page_state)

    def _invoke_stream_on_complete(self, summary: StreamSummary, run_state: _RunState, initial_request: dict[str, Any]) -> None:
        if self._runner._on_complete is None:
            return
        final_state = self._current_final_state(run_state)
        page_request = run_state.page_state.get('_request') if isinstance(run_state.page_state, dict) else None
        callback_url = page_request.get('url') if isinstance(page_request, dict) else initial_request.get('url')
        try:
            safe_call(self._runner._on_complete, summary, final_state, name='on_complete')
        except CallbackError as exc:
            source = run_state.event_source
            run_state.emit_event(now_event(kind='callback_error', source=source, endpoint=getattr(run_state, 'endpoint_name', None), url=callback_url, data={'callback': 'on_complete', 'exception_type': type(exc).__name__, 'exception_msg': str(exc)}))
            raise

    def _record_metrics_summary(self, summary: StreamSummary, *, failed: bool) -> None:
        if self._metrics_session is None:
            return
        try:
            self._metrics_session._record(summary, failed=failed)
        except Exception:
            logger.exception('metrics session recording failed')

    def _resolve_config(
        self,
        client_schema: dict[str, Any],
        cfg: dict[str, Any],
        call_params: dict[str, Any],
    ) -> _ResolvedJobConfig:
        base_url = client_schema['base_url'].rstrip('/')

        extra_scrub = _resolve('scrub_headers', call_params, cfg, client_schema, default=[])
        extra_scrub_query = _resolve('scrub_query_params', call_params, cfg, client_schema, default=[])

        log_level_key = cfg.get('log_level', 'medium')
        log_level = _LOG_LEVELS.get(log_level_key, logging.INFO)
        debug = cfg.get('debug', False)
        timeout = cfg.get('timeout', client_schema.get('timeout', 30))

        response_format = _resolve('response_format', call_params, cfg, client_schema, default='auto')
        csv_delimiter = _resolve('csv_delimiter', call_params, cfg, client_schema, default=';')
        encoding = _resolve('encoding', call_params, cfg, client_schema, default='utf-8')
        call_rate_limit = _resolve_explicit('rate_limit', call_params, default=_MISSING)
        endpoint_rate_limit = _resolve_explicit('rate_limit', cfg, default=_MISSING)
        client_rate_limit = _resolve_explicit('rate_limit', client_schema, default=_MISSING)
        if call_rate_limit is not _MISSING:
            resolved_rate_limit = call_rate_limit
            rate_limit_source = 'call'
        elif endpoint_rate_limit is not _MISSING:
            resolved_rate_limit = endpoint_rate_limit
            rate_limit_source = 'endpoint'
        elif client_rate_limit is not _MISSING:
            resolved_rate_limit = client_rate_limit
            rate_limit_source = 'client'
        else:
            resolved_rate_limit = _MISSING
            rate_limit_source = 'missing'
        rate_limit_disabled = resolved_rate_limit is None
        rate_limit = None if resolved_rate_limit is _MISSING else resolved_rate_limit
        retry_config = cfg['retry'] if 'retry' in cfg else _MISSING

        response_parser = cfg.get('response_parser')
        canonical_parser = _resolve('canonical_parser', call_params, cfg, client_schema)
        pagination_cfg = cfg.get('pagination')

        # lifecycle hooks always come from endpoint config, never from pagination config.
        # pagination config owns only mechanics: next_request, delay, initial_params.
        #
        # built-in helpers tag their dicts with _rf_pagination_helper and include
        # on_response etc. for convenience. extract those hooks here so the runner
        # receives them alongside endpoint-level hooks. endpoint-level hooks win
        # over helper-provided hooks when both are set — including explicit None,
        # which suppresses the helper-provided hook.
        _helper_hooks: dict[str, Any] = {}
        if isinstance(pagination_cfg, dict) and pagination_cfg.get('_rf_pagination_helper'):
            for hk in ('on_response', 'on_page', 'on_complete', 'on_page_complete', 'update_state'):
                if hk in pagination_cfg:
                    _helper_hooks[hk] = pagination_cfg[hk]

        # endpoint-level presence wins (even if None). helper fallback only when key is absent.
        _hook_names = ('on_response', 'on_page', 'on_complete', 'on_page_complete', 'update_state')
        _lifecycle_hooks = {
            hk: cfg[hk] if hk in cfg else _helper_hooks.get(hk)
            for hk in _hook_names
        }

        if isinstance(pagination_cfg, dict):
            # extract only pagination-mechanics keys
            runner_cfg = {
                k: pagination_cfg[k]
                for k in ('next_request', 'delay', 'initial_params')
                if k in pagination_cfg
            }
            runner_cfg.update(_lifecycle_hooks)
        else:
            runner_cfg = {
                'next_request': lambda _parsed, _state: None,
                **_lifecycle_hooks,
            }
        runner = build_cycle_runner(runner_cfg)
        on_error = cfg.get('on_error')
        on_event = cfg.get('on_event') or client_schema.get('on_event')
        # on_event_kinds is already normalized to None or frozenset[str] by validation
        on_event_kinds = cfg.get('on_event_kinds', client_schema.get('on_event_kinds'))
        on_request = cfg.get('on_request')
        mock = cfg.get('mock')
        playback_cfg = cfg.get('playback') or {}
        playback = build_playback_handler(cfg.get('playback'))
        record_as_bytes = bool(playback_cfg.get('record_as_bytes', False))
        return _ResolvedJobConfig(
            base_url=base_url,
            extra_scrub=extra_scrub,
            extra_scrub_query=extra_scrub_query,
            log_level=log_level,
            debug=debug,
            timeout=timeout,
            response_format=response_format,
            csv_delimiter=csv_delimiter,
            encoding=encoding,
            response_parser=response_parser,
            canonical_parser=canonical_parser,
            runner=runner,
            on_error=on_error,
            on_event=on_event,
            on_event_kinds=on_event_kinds,
            on_request=on_request,
            rate_limit=rate_limit,
            rate_limit_source=rate_limit_source,
            rate_limit_disabled=rate_limit_disabled,
            retry_config=retry_config,
            mock=mock,
            playback=playback,
            record_as_bytes=record_as_bytes,
        )

    def _log(self, level, msg, *args):
        if level >= self._log_level:
            logger.log(level, f'[{self._endpoint_name}] {msg}', *args)

    def _emit_event(self, event: PaginationEvent) -> None:
        if not self._on_event:
            return
        if self._on_event_kinds is not None and event.kind not in self._on_event_kinds:
            return
        try:
            self._on_event(event)
        except Exception as exc:  # observability must not break the run
            if not self._event_error_logged:
                self._event_error_logged = True
                logger.warning('[%s] on_event raised and was ignored: %s', self._endpoint_name, exc)

    def _build_url(self):
        path = self._cfg.get('path', '').lstrip('/')
        # interpolate path_params from call time, e.g. path='/holidays/{year}/{country}'
        # called as client.fetch('holidays', path_params={'year': 2026, 'country': 'US'})
        path_params = self._call_params.get('path_params', {})
        if path_params:
            try:
                path = path.format(**path_params)
            except KeyError as e:
                raise SchemaError(
                    f'path template {path!r} missing path_param: {e}'
                ) from e
        # warn if unreplaced placeholders remain — catches the case where the caller
        # forgot path_params entirely; the request would go out with literal {id} in the URL
        remaining = re.findall(r'\{(\w+)\}', path)
        if remaining:
            logger.warning(
                'path %r still contains unreplaced placeholder(s): %s — did you forget path_params?',
                path, remaining
            )
        return f'{self._base_url}/{path}' if path else self._base_url

    def _build_initial_request(self):
        'assembles the first request kwargs from schema + call-time params'
        method = self._cfg.get('method', 'GET').upper()
        url = self._build_url()

        headers = merge_dicts(
            self._cfg.get('headers', {}),
            self._call_params.get('headers', {})
        )
        params = merge_dicts(
            self._cfg.get('params', {}),
            self._call_params.get('params', {})
        )

        # inject initial pagination params if built-in strategy provides them
        params = merge_dicts(self._runner.initial_params, params)

        body = merge_dicts(
            self._cfg.get('body', {}),
            self._call_params.get('body', {})
        )

        form = merge_dicts(
            self._cfg.get('form', {}),
            self._call_params.get('form', {})
        )

        if 'files' in self._call_params:
            files = self._call_params.get('files')
        else:
            files = self._cfg.get('files')

        request = {
            'method': method,
            'url': url,
            'headers': headers,
            'params': params,
        }
        if body and form:
            raise SchemaError(
                f'endpoint {self._endpoint_name!r}: body and form are mutually exclusive — '
                'use body for JSON (Content-Type: application/json) or '
                'form for url-encoded (Content-Type: application/x-www-form-urlencoded), not both'
            )
        if body and files:
            raise SchemaError(
                f'endpoint {self._endpoint_name!r}: body and files are mutually exclusive — '
                'use body for JSON (Content-Type: application/json) or '
                'files for multipart uploads, not both'
            )
        if body:
            request['json'] = body
        else:
            if form:
                request['data'] = form
            if files:
                request['files'] = files

        return request

    def _apply_on_request(self, request_kwargs: dict[str, Any], *, run_state: Any) -> dict[str, Any]:
        if self._on_request is None:
            return request_kwargs
        state = StateView(run_state.page_state if run_state is not None else {})
        result = safe_call(self._on_request, request_kwargs, state, name='on_request')
        if not isinstance(result, dict):
            raise CallbackError(
                f'on_request must return a dict of request kwargs, got {type(result).__name__} — did you forget to return the modified request?',
                callback_name='on_request',
                cause=None,
            )
        return result

    def _apply_rate_limit(self, url: str | None, *, run_state: _RunState | None = None) -> None:
        if self._rate_limit_cfg is None:
            return

        if self._token_bucket is None:
            cfg = self._rate_limit_cfg
            self._token_bucket = TokenBucket(cfg.requests_per_second, cfg.burst, clock=cfg.clock, sleep=cfg.sleep)

        wait_s = self._token_bucket.acquire(1.0)
        if wait_s <= 0:
            return

        planned_ms = wait_s * 1000.0
        self._emit_event(now_event(
            kind='rate_limit_wait_start',
            source='live',
            endpoint=self._endpoint_name,
            url=url,
            data={'planned_ms': planned_ms, 'rps': self._rate_limit_rps, 'burst': self._rate_limit_burst},
        ))

        if self._rate_limit_on_limit == 'raise':
            self._emit_event(now_event(
                kind='rate_limit_exceeded',
                source='live',
                endpoint=self._endpoint_name,
                url=url,
                data={'rps': self._rate_limit_rps, 'burst': self._rate_limit_burst},
            ))
            raise RateLimitExceeded('rate limit exceeded')

        start_wait = time.monotonic()
        self._token_bucket.sleep(wait_s)
        end_wait = time.monotonic()
        wait_ms = (end_wait - start_wait) * 1000.0
        if run_state is not None:
            run_state.mark_wait('proactive', end_wait - start_wait)

        self._emit_event(now_event(
            kind='rate_limit_wait_end',
            source='live',
            endpoint=self._endpoint_name,
            url=url,
            data=run_state.wait_event_data('proactive', wait_ms=wait_ms, extra={'planned_ms': planned_ms, 'rps': self._rate_limit_rps, 'burst': self._rate_limit_burst}) if run_state is not None else {'wait_type': 'proactive', 'wait_ms': wait_ms, 'planned_ms': planned_ms, 'rps': self._rate_limit_rps, 'burst': self._rate_limit_burst},
        ))

    def _execute_request(
        self,
        request_kwargs: dict[str, Any],
        ctx: Any = None,
        *,
        run_state: Any = None,
    ) -> _RequestOutcome | StopSignal:
        '''
        executes one http request through auth + retry layers.
        returns a _RequestOutcome. request_kwargs in the outcome are post-auth
        request kwargs — guaranteed to
        include any auth headers added by the auth handler.  used by CycleRunner
        to populate state["_request"] with the exact dict that was sent.

        ctx: optional OperationContext forwarded to RetryHandler for per-attempt
             limit checks (deadline, max_requests). mock path applies the same checks.
        '''
        if run_state is None:
            raise RuntimeError('internal error: run_state is required')
        request_kwargs = self._apply_on_request(request_kwargs, run_state=run_state)

        if self._auth:
            request_kwargs = self._auth.apply(request_kwargs)

        if self._debug or self._log_level <= logging.DEBUG:
            self._log(logging.DEBUG, 'request: %s %s headers=%s params=%s',
                request_kwargs['method'], request_kwargs['url'],
                _scrub(request_kwargs.get('headers', {}), self._extra_scrub),
                request_kwargs.get('params'))

        if self._mock is not None:
            # mock path is still part of the engine lifecycle; emit request events for observability
            # apply the same ctx checks the retry layer would apply for real requests
            if ctx is not None:
                ctx.check_deadline()
                stop = ctx.check_max_requests()
                if stop is not None:
                    return stop
                ctx.record_request()
            self._apply_rate_limit(request_kwargs.get('url'), run_state=run_state)

            start_mono = time.monotonic()
            run_state.mark_request_start(now=start_mono)
            self._emit_event(now_event(
                kind='request_start',
                source='live',
                endpoint=self._endpoint_name,
                url=request_kwargs.get('url'),
                data={'method': request_kwargs.get('method')},
                mono=start_mono,
            ))

            if self._playback and self._playback.should_save:
                raise PlaybackError('playback save is not supported with mock responses; re-record against a real HTTP response')

            parsed, hdrs = self._execute_mock(request_kwargs, run_state=run_state)

            end_mono = time.monotonic()
            elapsed_ms = (end_mono - start_mono) * 1000.0
            self._emit_event(now_event(
                kind='request_end',
                source='live',
                endpoint=self._endpoint_name,
                url=request_kwargs.get('url'),
                data=run_state.request_end_event_data(status_code=None, elapsed_ms=elapsed_ms, now=end_mono),
                mono=end_mono,
            ))

            return _RequestOutcome(parsed_body=parsed, parsed=parsed, headers=hdrs, request_kwargs=request_kwargs)

        # capture post-auth values locally — not on self, since _execute_request
        # is called once per page and instance state would just reflect the last page
        method = request_kwargs['method']
        url = request_kwargs['url']
        session_kwargs = {
            k: v for k, v in request_kwargs.items()
            if k not in ('method', 'url')
        }
        self._apply_rate_limit(url, run_state=run_state)

        def retryable(first_attempt_mono: float | None = None):
            # Request timing is tracked here, inside the retried operation, so each
            # attempt gets its own start marker while the outer request-cycle helper
            # remains responsible for the shared event/accounting envelope. The first
            # attempt reuses the outer request-cycle start so request_start events and
            # page-cycle timing share the same baseline; retries get a fresh marker.
            run_state.mark_request_start(now=first_attempt_mono)
            return self._session.request(method, url, timeout=self._timeout, **session_kwargs)

        return self._run_request_cycle(
            source='live',
            method=method,
            url=url,
            request_kwargs=request_kwargs,
            run_state=run_state,
            retryable=retryable,
            allow_playback=True,
            ctx=ctx,
        )

    def _handle_error_response(self, response, method=None, url=None, request_kwargs=None, run_state=None):
        """Handle policy for an HTTP error response by constructing RequestError locally.

        This method is not called from inside an active ``except`` block. When
        the selected action is ``'raise'``, it must therefore raise the locally
        constructed exception object directly with ``raise exc``. A bare
        ``raise`` here would fail with ``RuntimeError: No active exception to
        re-raise``.
        """
        # include response body in error message so API error details are visible
        try:
            body = response.json()
            # anthropic-style: {"type": "error", "error": {"type": "...", "message": "..."}}
            api_msg = (body.get('error', {}).get('message')
                       or body.get('message')
                       or body.get('detail')
                       or '')
        except Exception:
            api_msg = response.text[:300] if response.text else ''

        message = f'http {response.status_code} from {response.url}'
        if api_msg:
            message += f' — {api_msg}'

        exc = RequestError(
            message,
            status_code=response.status_code,
            response=response,
            endpoint=self._endpoint_name,
            method=method,
            url=url,
        )
        if run_state is not None:
            run_state.mark_error()
            run_state.expose(run_state.page_state)
        if self._on_error:
            # on_error is called directly (not via safe_call) on purpose.
            # its return value ('raise' / 'skip' / 'stop') is a control-flow signal,
            # and exceptions raised inside it are meant to propagate unchanged. routing
            # this through safe_call would wrap them in CallbackError and change the contract.
            state_for_error = (run_state.page_state if run_state is not None else {})
            error_headers = dict(response.headers)
            error_headers['_status_code'] = response.status_code
            state_for_error['_response_headers'] = error_headers
            action = self._on_error(exc, StateView(state_for_error))
            if action == 'skip':
                self._log(logging.WARNING, 'on_error returned skip for status %d',
                    response.status_code)
                if run_state is not None:
                    run_state._last_error = exc
                    run_state._last_error_action = 'skip'
                skip_headers = dict(response.headers)
                skip_headers['_status_code'] = response.status_code
                return _RequestOutcome(parsed_body={}, parsed={}, headers=skip_headers, request_kwargs=request_kwargs)
            elif action == 'raise':
                # This helper constructs RequestError locally and is not running
                # under an active `except` for that object. Use `raise exc` here.
                # Bare `raise` would try to re-raise a currently handled exception
                # and fail with `RuntimeError: No active exception to re-raise`.
                raise exc
            elif action == 'stop':
                if run_state is not None:
                    run_state._last_error = exc
                    run_state._last_error_action = 'stop'
                return StopSignal(kind='error_stop')
            elif action is None:
                raise CallbackError(
                    'on_error returned None — must return "raise", "skip", or "stop"',
                    callback_name='on_error',
                    cause=None,
                )
            else:
                raise CallbackError(
                    f'on_error must return "raise", "skip", or "stop", got {action!r}',
                    callback_name='on_error',
                    cause=None,
                )
        raise exc

    def _execute_mock(self, request_kwargs: dict[str, Any], *, run_state: _RunState) -> tuple[Any, dict[str, Any]]:
        # mock path bypasses HTTP entirely: no auth injection, no retry handling,
        # and no network-level request execution. it only feeds deterministic
        # prebuilt responses into the same downstream processing contract.
        self._log(logging.DEBUG, 'using mock response')
        if callable(self._mock):
            return self._mock(request_kwargs, run_state=run_state), {}
        idx = min(run_state.mock_idx, len(self._mock) - 1)
        run_state.mock_idx = idx + 1
        return self._mock[idx], {}

    def _process_response(
        self,
        response: Any,
        request_kwargs: dict[str, Any],
        *,
        allow_playback: bool = True,
        run_state: _RunState,
    ) -> _RequestOutcome:
        final_method = request_kwargs.get('method')
        final_url = request_kwargs.get('url')

        if self._debug:
            self._log(logging.DEBUG, 'raw response status=%d body=%s',
                response.status_code, response.text[:500])

        self._log(logging.INFO, 'status=%d url=%s', response.status_code, getattr(response, 'url', final_url))

        if not response.ok:
            return self._handle_error_response(response, final_method, final_url, request_kwargs, run_state=run_state)

        headers = dict(response.headers)
        headers['_status_code'] = response.status_code
        if self._canonical_parser is None:
            parsed_default = default_parse_response(
                response,
                self._response_format,
                csv_delimiter=self._csv_delimiter,
                encoding=self._encoding,
            )
        else:
            content = getattr(response, 'content', None)
            if not isinstance(content, (bytes, bytearray)):
                raise ResponseError('canonical_parser requires response.content bytes')
            ctx = {
                'response_format': self._response_format,
                'csv_delimiter': self._csv_delimiter,
                'encoding': self._encoding,
                'headers': dict(response.headers),
                'status_code': int(getattr(response, 'status_code', 0) or 0),
                'url': getattr(response, 'url', final_url) or final_url or '',
                'request_kwargs': dict(request_kwargs),
            }
            parsed_default = safe_call(self._canonical_parser, bytes(content), ctx, name='canonical_parser')
        if self._response_parser is None:
            parsed = parsed_default
        elif self._parser_arity == 2:
            parsed = safe_call(self._response_parser, response, parsed_default,
                                name='response_parser')
        else:
            parsed = safe_call(self._response_parser, response, name='response_parser')

        if allow_playback and self._playback and self._playback.should_save:
            playback_page = serialize_response_for_playback(
                response, self._response_format, request_kwargs=request_kwargs, encoding=self._encoding, record_as_bytes=self._record_as_bytes
            )
            playback_page = _scrub_playback_envelope(
                playback_page,
                extra_headers=self._extra_scrub,
                extra_query_params=self._extra_scrub_query,
            )
            run_state.playback_pages.append(playback_page)

        return _RequestOutcome(parsed_body=parsed_default, parsed=parsed, headers=headers, request_kwargs=request_kwargs)

    def _handle_request_cycle_error(
        self,
        exc: RequestError,
        *,
        source: EventSource,
        method: str,
        url: str,
        request_kwargs: dict[str, Any],
        run_state: _RunState,
        start_mono: float,
    ) -> _RequestOutcome | StopSignal:
        """Handle request-cycle errors from an active ``except`` path.

        This helper is only correct when called while an exception is already
        being handled. When the selected action is ``'raise'`` we intentionally
        use bare ``raise`` to preserve the original traceback. Using
        ``raise exc`` here would reset the visible raise site to this helper and
        make debugging worse.
        """
        run_state.mark_error()
        run_state.expose(run_state.page_state)
        _enrich_exc(exc, self._endpoint_name, method, url)
        end_mono = time.monotonic()
        elapsed_ms = (end_mono - start_mono) * 1000.0
        _bytes = len(exc.response.content) if exc.response is not None and hasattr(exc.response, 'content') else 0
        _retry_bytes = _bytes if run_state.is_retry_attempt() else 0
        if _bytes:
            run_state.mark_bytes_received(_bytes)
        if _retry_bytes:
            run_state.mark_retry_bytes_received(_retry_bytes)
        self._emit_event(now_event(kind='request_end', source=source, endpoint=self._endpoint_name, url=url,
                           data=run_state.request_end_event_data(status_code=exc.status_code, elapsed_ms=elapsed_ms, bytes_received=_bytes, retry_bytes_received=run_state.current_page_retry_bytes_received(), now=end_mono), mono=end_mono))
        if self._on_error:
            action = self._on_error(exc, StateView(run_state.page_state))
            if action == 'skip':
                run_state._last_error = exc
                run_state._last_error_action = 'skip'
                return _RequestOutcome(parsed_body={}, parsed={}, headers={}, request_kwargs=request_kwargs)
            elif action == 'raise':
                # This helper runs inside the active `except RequestError` path in
                # `_run_request_cycle`. Bare `raise` preserves the original
                # traceback from the failing request path; `raise exc` would rebase
                # the visible raise site onto this helper and lose that context.
                raise
            elif action == 'stop':
                run_state._last_error = exc
                run_state._last_error_action = 'stop'
                return StopSignal(kind='error_stop')
            elif action is None:
                raise CallbackError('on_error returned None — must return "raise", "skip", or "stop"', callback_name='on_error', cause=None)
            else:
                raise CallbackError(f'on_error must return "raise", "skip", or "stop", got {action!r}', callback_name='on_error', cause=None)
        raise

    def _finish_request_cycle(
        self,
        response: Any,
        *,
        source: EventSource,
        url: str,
        request_kwargs: dict[str, Any],
        run_state: _RunState,
        start_mono: float,
        allow_playback: bool,
    ) -> _RequestOutcome | StopSignal:
        end_mono = time.monotonic()
        elapsed_ms = (end_mono - start_mono) * 1000.0
        if isinstance(response, StopSignal):
            self._emit_event(now_event(
                kind='request_end',
                source=source,
                endpoint=self._endpoint_name,
                url=url,
                data=run_state.request_end_event_data(status_code=None, elapsed_ms=elapsed_ms, now=end_mono),
                mono=end_mono,
            ))
            return response

        _bytes = len(response.content)
        _retry_bytes = _bytes if run_state.is_retry_attempt() else 0
        run_state.mark_bytes_received(_bytes)
        if _retry_bytes:
            run_state.mark_retry_bytes_received(_retry_bytes)
        self._emit_event(now_event(kind='request_end', source=source, endpoint=self._endpoint_name, url=url,
                           data=run_state.request_end_event_data(status_code=getattr(response, 'status_code', None), elapsed_ms=elapsed_ms, bytes_received=_bytes, retry_bytes_received=run_state.current_page_retry_bytes_received(), now=end_mono), mono=end_mono))
        return self._process_response(response, request_kwargs, allow_playback=allow_playback, run_state=run_state)

    def _run_request_cycle(
        self,
        *,
        source: EventSource,
        method: str,
        url: str,
        request_kwargs: dict[str, Any],
        run_state: _RunState,
        retryable: Callable[[float | None], Any],
        allow_playback: bool,
        ctx: Any = None,
    ) -> _RequestOutcome | StopSignal:
        start_mono = time.monotonic()
        self._emit_event(now_event(kind='request_start', source=source, endpoint=self._endpoint_name, url=url,
                           data={'method': method}, mono=start_mono))

        def _on_retry(info):
            if info.get('current_attempt', 1) > 1:
                _retry_bytes = int(info.get('bytes_received', 0) or 0)
                if _retry_bytes:
                    run_state.mark_retry_bytes_received(_retry_bytes)
            run_state.mark_retry()
            self._emit_event(now_event(kind='retry', source=source, endpoint=self._endpoint_name, url=url,
                               data={**info, 'retries_so_far': run_state.retries_so_far}))

        def _on_wait(seconds, planned_seconds, cause):
            run_state.mark_wait('reactive', seconds, cause=cause)
            if cause in {'retry_after', 'min_delay'}:
                self._emit_event(now_event(
                    kind='rate_limit_wait_end',
                    source=source,
                    endpoint=self._endpoint_name,
                    url=url,
                    data=run_state.wait_event_data('reactive', wait_ms=seconds * 1000.0, extra={'planned_ms': planned_seconds * 1000.0, 'cause': cause}),
                ))

        first_attempt = True

        def attempt():
            nonlocal first_attempt
            first_attempt_mono = start_mono if first_attempt else None
            first_attempt = False
            return retryable(first_attempt_mono)

        try:
            response = self._retry.execute(attempt, ctx, on_retry=_on_retry, on_wait=_on_wait)
        except RequestError as exc:
            return self._handle_request_cycle_error(
                exc,
                source=source,
                method=method,
                url=url,
                request_kwargs=request_kwargs,
                run_state=run_state,
                start_mono=start_mono,
            )

        return self._finish_request_cycle(
            response,
            source=source,
            url=url,
            request_kwargs=request_kwargs,
            run_state=run_state,
            start_mono=start_mono,
            allow_playback=allow_playback,
        )

    def _playback_fetch(self, records_iter: Iterator[Any]) -> Callable[..., _RequestOutcome | StopSignal]:
        def fetch(request_kwargs: dict[str, Any], ctx: Any = None, *, run_state: _RunState) -> _RequestOutcome | StopSignal:
            method = request_kwargs['method']
            url = request_kwargs['url']

            def retryable(first_attempt_mono: float | None = None):
                # Playback reuses the same request-cycle envelope as live fetches,
                # but the response source is fixture deserialization rather than an
                # actual HTTP call. The first attempt reuses the outer request-cycle
                # start so page-cycle timing aligns with the request_start event;
                # retries get a fresh marker.
                run_state.mark_request_start(now=first_attempt_mono)
                try:
                    envelope = next(records_iter)
                except StopIteration as e:
                    raise PlaybackError('recorded responses exhausted before execution completed') from e
                if not isinstance(envelope, dict) or envelope.get('kind') != 'raw_response':
                    raise PlaybackError('playback files must contain raw response envelopes')
                return deserialize_playback_response(envelope)

            return self._run_request_cycle(
                source='playback',
                method=method,
                url=url,
                request_kwargs=request_kwargs,
                run_state=run_state,
                retryable=retryable,
                allow_playback=False,
                ctx=None,
            )

        return fetch

    def _save_playback(self, run_state: _RunState) -> None:
        if not self._playback:
            return
        if self._playback.should_save and run_state.playback_pages:
            self._playback.save(run_state.playback_pages)

    def _run(self, ctx=None, *, mode='stream', original_on_complete=None):
        'internal executor — yields according to fetch/stream mode'
        run_state = _RunState(endpoint_name=self._endpoint_name, emit_event=self._emit_event)
        # terminal_summary is the canonical per-run summary object. It may be
        # constructed on clean completion or during exception handling, but it
        # should describe the run only once and then feed every later consumer.
        terminal_summary: StreamSummary | None = None
        failed = True
        pending_exception: BaseException | None = None
        post_run_exception: BaseException | None = None

        initial_request = self._build_initial_request()
        runner = self._runner
        self._log(
            logging.INFO, 'starting fetch method=%s url=%s',
            initial_request['method'], initial_request['url'],
        )
        try:
            if self._playback and self._playback.should_load:
                records = self._playback.load()
                records_iter = iter(records)
                self._log(logging.INFO, 'running in playback mode with %d recorded response(s)', len(records))
                page_count = 0
                run_state.event_source = 'playback'
                try:
                    for page in runner.run(
                        self._playback_fetch(records_iter),
                        initial_request,
                        ctx=None,
                        run_state=run_state,
                        initial_state=self._initial_state,
                        mode=mode,
                    ):
                        page_count += 1
                        yield page
                except RateLimitExceeded:
                    raise
                except Exception as exc:
                    self._emit_event(now_event(
                        kind='error',
                        source='playback',
                        endpoint=self._endpoint_name,
                        url=initial_request.get('url'),
                        data={'exception_type': type(exc).__name__, 'exception_msg': str(exc)},
                    ))
                    raise

                try:
                    next(records_iter)
                except StopIteration:
                    pass
                else:
                    raise PlaybackError('extra recorded responses after execution completed')

                stop_signal = self._current_stop_signal(run_state)
                terminal_summary = run_state.build_summary(stop_signal)
                self._log(logging.INFO, 'playback complete — %d page(s)', page_count)
                return

            page_count = 0
            run_state.event_source = 'live'
            try:
                for page in runner.run(
                    self._execute_request,
                    initial_request,
                    ctx=ctx,
                    run_state=run_state,
                    initial_state=self._initial_state,
                    mode=mode,
                ):
                    page_count += 1
                    self._log(logging.DEBUG, 'yielding page %d', page_count)
                    yield page
            except RateLimitExceeded:
                raise
            except Exception as exc:
                self._emit_event(now_event(
                    kind='error',
                    source='live',
                    endpoint=self._endpoint_name,
                    url=initial_request.get('url'),
                    data={'exception_type': type(exc).__name__, 'exception_msg': str(exc)},
                ))
                raise

            stop_signal = self._current_stop_signal(run_state)
            terminal_summary = run_state.build_summary(stop_signal)
            self._log(logging.INFO, 'fetch complete — %d page(s)', page_count)
        except BaseException as exc:
            pending_exception = exc
            if terminal_summary is None:
                terminal_summary = run_state.build_summary(stop=None)
            # Fall through to the original active exception when no other action applies.
            raise
        finally:
            if terminal_summary is None:
                terminal_summary = run_state.build_summary(stop=None)

            should_persist_playback = not (self._playback and self._playback.should_load)
            can_run_on_complete = pending_exception is None and post_run_exception is None
            is_stream_mode = mode == 'stream'

            if should_persist_playback:
                try:
                    self._save_playback(run_state)
                except BaseException as exc:
                    if pending_exception is not None:
                        logger.exception('playback save failed during exception handling')
                    else:
                        post_run_exception = exc
                        can_run_on_complete = False

            # Metrics are recorded for every terminal path from the same canonical
            # run summary. User-facing completion hooks are narrower: they only run
            # after the generator/collector finished without a pending exception and
            # without a post-run failure such as playback save.
            if can_run_on_complete:
                if is_stream_mode:
                    try:
                        self._invoke_stream_on_complete(terminal_summary, run_state, initial_request)
                    except BaseException as exc:
                        post_run_exception = exc
                    else:
                        failed = False
                else:
                    failed = False

            self._record_metrics_summary(terminal_summary, failed=failed)
            runner._on_complete = original_on_complete
            if post_run_exception is not None:
                raise post_run_exception

    def _generate(self, max_pages=None, max_requests=None, time_limit=None, *, original_on_complete=None):
        'internal generator — yields one page at a time'
        ctx = OperationContext(max_pages, max_requests, time_limit)
        yield from self._run(ctx, mode='stream', original_on_complete=original_on_complete)

    def _collect(self, max_pages=None, max_requests=None, time_limit=None):
        'internal collector — returns all pages, unwraps single-page results'
        ctx = OperationContext(max_pages, max_requests, time_limit)
        pages = list(self._run(ctx, mode='fetch'))
        return pages[0] if len(pages) == 1 else pages


class _StreamRunImpl:
    def __init__(self, iterator: Iterator[StreamItem], summary_box: dict[str, StreamSummary]):
        self._iterator = iterator
        self._summary_box = summary_box

    @property
    def summary(self) -> StreamSummary | None:
        return self._summary_box.get('summary')

    @summary.setter
    def summary(self, value: StreamSummary | None) -> None:
        if value is None:
            self._summary_box.pop('summary', None)
        else:
            self._summary_box['summary'] = value

    def __iter__(self) -> Iterator[StreamItem]:
        return self

    def __next__(self) -> StreamItem:
        return next(self._iterator)


class APIClient:
    '''
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
    '''

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
        self._auth = build_auth_handler(schema.get('auth'), config_view=self._config_view, timeout=self._timeout)
        self._retry_config = schema.get('retry')
        self._token_buckets: dict[tuple[str, str | None], TokenBucket] = {}
        self._token_bucket_lock = threading.Lock()

        log_level_key = schema.get('log_level', 'medium')
        self._log_level = _LOG_LEVELS.get(log_level_key, logging.INFO)
        logger.setLevel(self._log_level)

        logger.info('APIClient ready — base_url=%s endpoints=%s',
            schema['base_url'],
            list(schema['endpoints'].keys())
        )


    def _get_shared_token_bucket(self, endpoint_name: str, rate_limit_source: str, rate_limit_cfg: dict[str, Any] | None) -> TokenBucket | None:
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
                bucket = TokenBucket(cfg.requests_per_second, cfg.burst, clock=cfg.clock, sleep=cfg.sleep)
                self._token_buckets[key] = bucket
            return bucket

    def _make_job(self, endpoint_name, call_params):
        'creates a fresh FetchJob for a single call'
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
        '''
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
        '''
        max_pages    = call_params.pop('max_pages',    None)
        max_requests = call_params.pop('max_requests', None)
        time_limit = call_params.pop('time_limit', None)
        return self._make_job(endpoint_name, call_params)._collect(
            max_pages=max_pages, max_requests=max_requests, time_limit=time_limit
        )

    def stream(self, endpoint_name: str, **call_params: Any) -> Iterator[StreamItem]:
        '''
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
        '''
        max_pages    = call_params.pop('max_pages',    None)
        max_requests = call_params.pop('max_requests', None)
        time_limit = call_params.pop('time_limit', None)
        return self._make_job(endpoint_name, call_params)._generate(
            max_pages=max_pages, max_requests=max_requests, time_limit=time_limit
        )

    def stream_run(self, endpoint_name: str, **call_params: Any) -> StreamRun:
        '''streams pages and exposes a completion summary after exhaustion.

        stream_run() returns an iterable wrapper. Iterate it like stream(), then
        inspect run.summary (StreamSummary | None).

        Notes:
            - summary is set only on normal completion (natural exhaustion or
              non-destructive caps like max_pages/max_requests).
            - if you stop iterating early, or if a destructive error occurs
              (e.g. DeadlineExceeded), summary remains None.
        '''
        max_pages    = call_params.pop('max_pages',    None)
        max_requests = call_params.pop('max_requests', None)
        time_limit = call_params.pop('time_limit', None)

        job = self._make_job(endpoint_name, call_params)
        summary_box: dict[str, StreamSummary] = {}
        original_on_complete = job._runner._on_complete

        def capture_complete(summary: StreamSummary, state: dict[str, Any]) -> Any:
            if original_on_complete is not None:
                result = original_on_complete(summary, state)
                summary_box['summary'] = summary
                return result
            summary_box['summary'] = summary
            return None

        job._runner._on_complete = capture_complete
        iterator = job._generate(
            max_pages=max_pages,
            max_requests=max_requests,
            time_limit=time_limit,
            original_on_complete=original_on_complete,
        )
        return _StreamRunImpl(iterator, summary_box)

    def close(self) -> None:
        'closes the underlying requests session. call when done, or use as context manager.'
        self._session.close()

    def __enter__(self) -> APIClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
