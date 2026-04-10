"""
Optional typing layer for rest_fetcher.

All TypedDicts are total=False (all keys optional at the type level) unless noted.
The runtime still validates required keys — these types are for IDE autocompletion
and mypy, not for replacing schema validation.

Usage:
    from rest_fetcher.types import ClientSchema, EndpointSchema, SchemaBuilder

    # with TypedDicts (still just dicts at runtime):
    schema: ClientSchema = {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'abc'},
        'endpoints': {'users': {'method': 'GET', 'path': '/users'}},
    }

    # with SchemaBuilder (returns the same plain dict):
    schema = (
        SchemaBuilder('https://api.example.com/v1')
        .bearer('my-token')
        .timeout(30)
        .endpoint('users', method='GET', path='/users')
        .build()
    )
    client = APIClient(schema)
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypedDict

from .events import EventSource

if TYPE_CHECKING:
    from .events import PaginationEvent
    from .metrics import MetricsSession

# callback/result aliases
# fetch()/stream() may return parser- and callback-shaped values; container shape is not fixed.
FetchResult = Any
StreamItem = Any

# (raw_response, state) -> page_data
OnResponseFn = Callable[[Any, dict], Any]

# (page_data, state) -> None  (side effect only)
OnPageFn = Callable[[Any, dict], None]

# completion callback contracts:
#   fetch()  -> (results, state) -> transformed_result
#   stream() -> (summary, state) -> ignored_return_value
# same schema key, different first argument by caller mode.
FetchOnCompleteFn = Callable[[FetchResult, dict], Any]
StreamOnCompleteFn = Callable[['StreamSummary', dict], Any]
OnCompleteFn = FetchOnCompleteFn | StreamOnCompleteFn

# (parsed_body, state) -> next_request_overrides | None (None = stop)
# parsed_body is the canonical parsed representation based on response_format,
# or the return value of canonical_parser if configured.
NextRequestFn = Callable[[Any, dict], dict | None]

# (page_payload, state) -> state_update_dict | None
UpdateStateFn = Callable[[Any, dict], dict | None]

# (exception, state) -> 'skip' | 'raise' | None
OnErrorAction = Literal['skip', 'raise', 'stop']
OnErrorFn = Callable[[Exception, dict], OnErrorAction | None]

# (outcome, state) -> float | None  (adaptive delay in seconds; None or 0 = no delay)
OnPageCompleteFn = Callable[['PageCycleOutcome', dict], float | None]

# (event) -> None  (observability only; must not affect control flow)
OnEventFn = Callable[['PaginationEvent'], None]

# parser(response) -> parsed_value
ParserFn1 = Callable[[Any], Any]

# parser(response, parsed) -> parsed_value
ParserFn2 = Callable[[Any, Any], Any]

ResponseParser = ParserFn1 | ParserFn2
ResponseParserFn = ResponseParser

# canonical_parser(content_bytes, context) -> parsed_body
# If provided in endpoint config, this runs before response_parser and its return
# value becomes the canonical parsed_body visible to pagination next_request().
CanonicalParserFn = Callable[[bytes, dict[str, Any]], Any]

# (request_kwargs, config) -> modified_request_kwargs
AuthCallbackFn = Callable[[dict, dict], dict]

# (state) -> token_string
TokenCallbackFn = Callable[[dict], str]

# (request_kwargs, state) -> modified_request_kwargs
OnRequestFn = Callable[[dict, dict], dict]
FilesValue = dict[str, Any] | list[tuple[str, Any]] | tuple[tuple[str, Any], ...]


StopKind = Literal['max_pages', 'max_requests', 'next_request_none', 'error_stop']


@dataclass(frozen=True)
class StopSignal:
    kind: StopKind
    limit: int | None = None
    observed: int | None = None


@dataclass(frozen=True)
class PageCycleOutcome:
    """Per-page cycle outcome passed to the on_page_complete hook.

    Fields:
        kind:             one of 'success', 'skipped', or 'stopped'
        status_code:      final observed HTTP status code for the cycle, if available
        error:            terminal error for the cycle, if any (None on success)
        stop_signal:      set when the cycle resolved via stop semantics
        attempts_for_page: total number of request attempts made for this page cycle
        cycle_elapsed_ms: wall-clock elapsed time from first attempt start to final
                          response for this page cycle, including retries and retry
                          backoff waits but excluding later static/adaptive delays

    Expected relationships:
        kind='success' normally implies error is None and stop_signal is None
        kind='skipped' normally implies error is not None
        kind='stopped' normally implies stop_signal is not None
    """

    kind: Literal['success', 'skipped', 'stopped']
    status_code: int | None
    error: Exception | None
    stop_signal: StopSignal | None
    attempts_for_page: int
    cycle_elapsed_ms: float | None


@dataclass(frozen=True)
class StreamSummary:
    pages: int
    requests: int
    stop: StopSignal | None = None
    endpoint: str | None = None
    source: EventSource = 'live'
    retries: int = 0
    elapsed_seconds: float = 0.0
    retry_wait_seconds: float = 0.0
    rate_limit_wait_seconds: float = 0.0
    adaptive_wait_seconds: float = 0.0
    static_wait_seconds: float = 0.0
    total_wait_seconds: float = 0.0
    bytes_received: int = 0
    retry_bytes_received: int = 0


class StreamRun(Protocol, Iterator[StreamItem]):
    """Iterable stream wrapper that exposes completion summary after exhaustion.

    stream_run() returns an object you can iterate over like stream(), and then
    read run.summary once iteration completes normally.
    """

    summary: StreamSummary | None

    def __next__(self) -> StreamItem: ...


# auth TypedDicts
class BearerAuthConfig(TypedDict, total=False):
    type: str  # 'bearer' (required at runtime)
    token: str
    token_callback: TokenCallbackFn


class BasicAuthConfig(TypedDict, total=False):
    type: str  # 'basic' (required at runtime)
    username: str  # required at runtime
    password: str  # required at runtime


class OAuth2AuthConfig(TypedDict, total=False):
    type: str  # 'oauth2' (required at runtime)
    token_url: str  # required at runtime
    client_id: str  # required at runtime
    client_secret: str  # required at runtime
    scope: str
    expiry_margin: int


class OAuth2PasswordAuthConfig(TypedDict, total=False):
    type: str  # 'oauth2_password' (required at runtime)
    token_url: str  # required at runtime
    client_id: str  # required at runtime
    client_secret: str  # required at runtime
    username: str  # required at runtime
    password: str  # required at runtime
    scope: str
    expiry_margin: int


class CallbackAuthConfig(TypedDict, total=False):
    type: str  # 'callback' (required at runtime)
    handler: AuthCallbackFn  # required at runtime


AuthConfig = (
    BearerAuthConfig
    | BasicAuthConfig
    | OAuth2AuthConfig
    | OAuth2PasswordAuthConfig
    | CallbackAuthConfig
)


# retry / rate_limit TypedDicts
class RetryConfig(TypedDict, total=False):
    max_attempts: int
    backoff: str | Callable[[int], float]  # 'exponential' | 'linear' | callable
    on_codes: list[int]
    base_delay: float
    max_delay: float
    jitter: bool | str
    max_retry_after: float
    reactive_wait_on_terminal: bool


class RateLimitConfig(TypedDict, total=False):
    respect_retry_after: bool
    min_delay: float
    strategy: Literal['token_bucket']
    requests_per_second: float
    burst: int
    on_limit: Literal['wait', 'raise']
    clock: Callable[[], float]
    sleep: Callable[[float], None]


# pagination TypedDict
class PaginationConfig(TypedDict, total=False):
    next_request: NextRequestFn  # required when pagination is enabled
    delay: float
    initial_params: dict[str, Any]


PathLike = str | Sequence[str | int]
MockFn = Callable[[dict[str, Any], Any], Any]
MockConfig = list[dict[str, Any]] | MockFn


class PlaybackConfig(TypedDict, total=False):
    path: str
    mode: str
    record_as_bytes: bool


class CallParams(TypedDict, total=False):
    """Call-time keyword arguments supported by APIClient.fetch()/stream()/stream_run().

    This is an optional typing helper only. At runtime, call-time kwargs are still
    just plain dict-style keyword arguments.
    """

    params: dict[str, Any]
    headers: dict[str, str]
    body: dict[str, Any]
    form: dict[str, Any]
    files: FilesValue
    path_params: dict[str, Any]
    canonical_parser: CanonicalParserFn
    response_format: str
    csv_delimiter: str
    encoding: str
    scrub_headers: list[str]
    scrub_query_params: list[str]
    rate_limit: RateLimitConfig | None

    max_pages: int
    max_requests: int
    time_limit: float


# endpoint / client TypedDicts
class EndpointSchema(TypedDict, total=False):
    method: str  # GET POST PUT PATCH DELETE HEAD OPTIONS
    path: str
    headers: dict[str, str]
    params: dict[str, Any]
    body: dict[str, Any]
    form: dict[str, Any]  # form-encoded body (data=)
    files: FilesValue  # multipart/file upload payload (files=)
    pagination: PaginationConfig | None
    on_response: OnResponseFn
    on_page: OnPageFn
    on_complete: OnCompleteFn
    on_page_complete: OnPageCompleteFn
    update_state: UpdateStateFn
    on_error: OnErrorFn
    on_event: OnEventFn
    on_event_kinds: None | str | set[str] | list[str] | frozenset[str]
    on_request: OnRequestFn
    response_parser: ResponseParser
    canonical_parser: CanonicalParserFn
    retry: RetryConfig | None
    rate_limit: RateLimitConfig | None
    response_format: str  # auto | json | text | xml | csv | bytes
    csv_delimiter: str  # single-character delimiter for response_format='csv'
    encoding: str  # text-layer encoding for textual responses
    scrub_headers: list[str]
    scrub_query_params: list[str]
    log_level: str  # none | error | medium | verbose
    timeout: float | tuple[float, float]
    debug: bool
    mock: MockConfig
    playback: PlaybackConfig


class SessionConfig(TypedDict, total=False):
    verify: bool | str  # True/False or path to CA bundle
    cert: str | tuple[str, str]  # client cert path or (cert, key) tuple
    proxies: dict[str, str]  # e.g. {'http': 'http://proxy:8080', 'https': '...'}
    max_redirects: int  # default 30


class ClientSchema(TypedDict, total=False):
    base_url: str  # required at runtime
    auth: AuthConfig
    retry: RetryConfig
    rate_limit: RateLimitConfig
    pagination: PaginationConfig  # client-level defaults inherited by endpoints
    endpoints: dict[str, EndpointSchema]  # required at runtime
    headers: dict[str, str]
    timeout: float | tuple[float, float]
    log_level: str
    on_error: OnErrorFn
    on_event: OnEventFn
    on_event_kinds: None | str | set[str] | list[str] | frozenset[str]
    on_request: OnRequestFn
    response_parser: ResponseParser
    canonical_parser: CanonicalParserFn
    response_format: str  # auto | json | text | xml | csv | bytes
    csv_delimiter: str  # single-character delimiter for response_format='csv'
    encoding: str  # text-layer encoding default for textual responses
    state: dict[str, Any]  # read-only config source and run-state seed
    session_config: SessionConfig  # requests.Session settings: verify, cert, proxies, max_redirects
    scrub_headers: list[str]  # extra header names to redact in logs / playback fixtures
    scrub_query_params: list[str]  # extra query param names to redact in playback fixture URLs
    metrics: bool | None | MetricsSession


# SchemaBuilder
class SchemaBuilder:
    """
    fluent builder that produces a plain dict compatible with APIClient.
    purely a developer-experience layer — the dict mindset is fully intact:
        schema = SchemaBuilder(...).bearer(...).endpoint(...).build()
        client = APIClient(schema)            # exactly the same as passing a dict

    all methods return self for chaining. build() returns the plain dict.
    """

    def __init__(self, base_url: str) -> None:
        self._schema: dict = {'base_url': base_url, 'endpoints': {}}

    # auth shortcuts
    def bearer(self, token: str | TokenCallbackFn) -> SchemaBuilder:
        if callable(token):
            self._schema['auth'] = {'type': 'bearer', 'token_callback': token}
        else:
            self._schema['auth'] = {'type': 'bearer', 'token': token}
        return self

    def basic(self, username: str, password: str) -> SchemaBuilder:
        self._schema['auth'] = {'type': 'basic', 'username': username, 'password': password}
        return self

    def oauth2(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str | None = None,
        expiry_margin: int = 60,
    ) -> SchemaBuilder:
        cfg: dict = {
            'type': 'oauth2',
            'token_url': token_url,
            'client_id': client_id,
            'client_secret': client_secret,
            'expiry_margin': expiry_margin,
        }
        if scope:
            cfg['scope'] = scope
        self._schema['auth'] = cfg
        return self

    def oauth2_password(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
        scope: str | None = None,
        expiry_margin: int = 60,
    ) -> SchemaBuilder:
        cfg: dict = {
            'type': 'oauth2_password',
            'token_url': token_url,
            'client_id': client_id,
            'client_secret': client_secret,
            'username': username,
            'password': password,
            'expiry_margin': expiry_margin,
        }
        if scope:
            cfg['scope'] = scope
        self._schema['auth'] = cfg
        return self

    def auth_callback(self, handler: AuthCallbackFn) -> SchemaBuilder:
        self._schema['auth'] = {'type': 'callback', 'handler': handler}
        return self

    # client-level config
    def session_config(
        self,
        verify: bool | str | None = None,
        cert: str | tuple[str, str] | None = None,
        proxies: dict[str, str] | None = None,
        max_redirects: int | None = None,
    ) -> SchemaBuilder:
        """
        configure the underlying requests.Session.
        verify: False to skip TLS verification, or a path to a CA bundle.
        cert: path to a client certificate, or (cert_path, key_path) tuple.
        proxies: dict mapping schemes to proxy URLs, e.g. {'https': 'http://proxy:8080'}.
        max_redirects: maximum number of redirects to follow (default 30).
        """
        cfg: dict = {}
        if verify is not None:
            cfg['verify'] = verify
        if cert is not None:
            cfg['cert'] = cert
        if proxies is not None:
            cfg['proxies'] = proxies
        if max_redirects is not None:
            cfg['max_redirects'] = max_redirects
        if cfg:
            self._schema['session_config'] = cfg
        return self

    def timeout(self, value: float | tuple[float, float]) -> SchemaBuilder:
        """
        always set a timeout — requests has no default and will hang indefinitely.
        single value: applies to both connect and read.
        tuple (connect, read): separate timeouts, e.g. (5, 60).
        """
        self._schema['timeout'] = value
        return self

    def retry(
        self,
        max_attempts: int = 3,
        backoff: str = 'exponential',
        on_codes: list[int] | None = None,
        base_delay: float | None = None,
        max_delay: float | None = None,
        jitter: bool | str = False,
        max_retry_after: float | None = None,
    ) -> SchemaBuilder:
        cfg = {
            'max_attempts': max_attempts,
            'backoff': backoff,
            'on_codes': on_codes or [429, 500, 502, 503, 504],
        }
        if base_delay is not None:
            cfg['base_delay'] = base_delay
        if max_delay is not None:
            cfg['max_delay'] = max_delay
        if jitter is not False:
            cfg['jitter'] = jitter
        if max_retry_after is not None:
            cfg['max_retry_after'] = max_retry_after
        self._schema['retry'] = cfg
        return self

    def rate_limit(self, min_delay: float = 0.0, respect_retry_after: bool = True) -> SchemaBuilder:
        self._schema['rate_limit'] = {
            'min_delay': min_delay,
            'respect_retry_after': respect_retry_after,
        }
        return self

    def headers(self, **kwargs: str) -> SchemaBuilder:
        self._schema['headers'] = kwargs
        return self

    def state(self, **kwargs: Any) -> SchemaBuilder:
        "seed read-only auth config and initial run state"
        self._schema['state'] = kwargs
        return self

    def log_level(self, level: str) -> SchemaBuilder:
        self._schema['log_level'] = level
        return self

    def scrub_headers(self, *names: str) -> SchemaBuilder:
        "additional header names to redact in logs / playback fixtures (on top of defaults)"
        self._schema['scrub_headers'] = list(names)
        return self

    def scrub_query_params(self, *names: str) -> SchemaBuilder:
        "additional query parameter names to redact in playback fixture URLs (on top of defaults)"
        self._schema['scrub_query_params'] = list(names)
        return self

    def on_event_kinds(
        self, kinds: None | str | set[str] | list[str] | frozenset[str]
    ) -> SchemaBuilder:
        "filter which event kinds reach on_event — raw value, normalized during validation"
        self._schema['on_event_kinds'] = kinds
        return self

    # endpoints
    def endpoint(
        self, name: str, *, method: str = 'GET', path: str = '', **kwargs: Any
    ) -> SchemaBuilder:
        """
        add an endpoint. all EndpointSchema keys accepted as kwargs:
            .endpoint('users', method='GET', path='/users', params={'active': True})
            .endpoint('create', method='POST', path='/users', timeout=60)
        """
        ep: dict = {'method': method}
        if path:
            ep['path'] = path
        ep.update(kwargs)
        self._schema['endpoints'][name] = ep
        return self

    def pagination(self, config: PaginationConfig) -> SchemaBuilder:
        "client-level pagination mechanics (next_request, delay, initial_params) inherited by all endpoints"
        self._schema['pagination'] = config
        return self

    # output
    def build(self) -> dict:
        """
        returns the plain schema dict. pass directly to APIClient:
            client = APIClient(builder.build())
        """
        return dict(self._schema)

    def __repr__(self) -> str:
        endpoints = list(self._schema.get('endpoints', {}).keys())
        return f'SchemaBuilder(base_url={self._schema["base_url"]!r}, endpoints={endpoints})'
