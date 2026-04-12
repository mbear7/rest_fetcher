from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from collections.abc import Iterable as ABCIterable
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

if TYPE_CHECKING:
    from .types import PaginationConfig
import logging

from .callbacks import safe_call
from .events import EventSource, now_event
from .exceptions import CallbackError, PaginationError, StateViewMutationError
from .schema import merge_dicts
from .types import PageCycleOutcome, StopSignal

logger = logging.getLogger('rest_fetcher.pagination')


class _RunStateLike(Protocol):
    """Internal run-state contract consumed by CycleRunner."""

    page_state: dict[str, Any]
    event_source: EventSource
    endpoint_name: str | None
    _last_error: Exception | None

    def expose(self, state: dict[str, Any]) -> None: ...
    def emit_event(self, event: Any) -> None: ...
    def reset_page_cycle(self) -> None: ...
    def mark_page_complete(self) -> None: ...
    def mark_wait(self, kind: str, seconds: float, *, cause: str | None = None) -> None: ...
    def build_page_cycle_outcome(
        self, *, status_code: int | None, error: Exception | None, stop_signal: StopSignal | None
    ) -> PageCycleOutcome: ...
    def page_parsed_event_data(
        self, *, status_code: int | None = None, now: float | None = None
    ) -> dict[str, Any]: ...
    def stop_event_data(self, stop: StopSignal) -> dict[str, Any]: ...
    def wait_event_data(
        self, kind: str, *, wait_ms: float, extra: dict[str, Any] | None = None
    ) -> dict[str, Any]: ...


import numbers as _numbers


def _validate_params_mode(params_mode: str) -> str:
    if params_mode not in {'merge', 'replace'}:
        raise ValueError(f"params_mode must be 'merge' or 'replace', got {params_mode!r}")
    return params_mode


def _validate_adaptive_delay(value: Any) -> float | None:
    """Validates the return value of on_page_complete.

    Returns the delay as a float, or None if no delay should be applied.
    Raises CallbackError for invalid return values.
    """
    if value is None:
        return None
    # bool is a subclass of int — reject it explicitly to prevent True -> 1.0 footgun
    if isinstance(value, bool):
        from .exceptions import CallbackError

        raise CallbackError(
            f'on_page_complete returned bool ({value!r}) — return a float in seconds, None, or 0 for no delay',
            callback_name='on_page_complete',
            cause=None,
        )
    if value == 0:
        return None
    if not isinstance(value, _numbers.Real):
        from .exceptions import CallbackError

        raise CallbackError(
            f'on_page_complete must return a float (seconds), None, or 0 — got {type(value).__name__!r}',
            callback_name='on_page_complete',
            cause=None,
        )
    delay = float(value)
    if delay < 0:
        from .exceptions import CallbackError

        raise CallbackError(
            f'on_page_complete returned negative delay ({delay}) — delay must be >= 0',
            callback_name='on_page_complete',
            cause=None,
        )
    return delay if delay > 0 else None


PathPart = str | int
PathLike = str | Iterable[PathPart] | PathPart | None
PathResolver = Callable[[Any, PathLike], Any]


def _event_source(run_state: _RunStateLike | None) -> EventSource:
    if run_state is None:
        return 'live'
    source = run_state.event_source
    if source not in ('live', 'playback'):
        raise RuntimeError(f'unexpected event_source: {source!r}')
    return source


# class FetchFn(Protocol):
#    def __call__(self, request: dict[str, Any], ctx: Any = None, *, run_state: Any = None) -> Any:
#        ...


def _copy_request_for_snapshot(request: dict[str, Any]) -> dict[str, Any]:
    """
    makes a targeted shallow copy of a request dict for use in state['_request'].

    copies the top-level dict plus params, headers, json, and data one level deep.
    this blocks the realistic mutation bugs (state['_request']['params']['page'] = 999,
    state['_request']['headers']['X-Foo'] = 'bar') without deep-copying exotic objects
    that may live in auth headers or nested structures (sessions, file handles, etc).

    what is protected:
        state['_request']['params']['x'] = y   -- blocked (params is a fresh dict)
        state['_request']['headers']['x'] = y  -- blocked (headers is a fresh dict)
        state['_request']['json']['x'] = y     -- blocked (json is a fresh dict)
        state['_request']['data']['x'] = y     -- blocked (data is a fresh dict)
        state['_request'] = new_dict           -- blocked by StateView.__setitem__

    what is not protected (not a real-world mutation pattern):
        state['_request']['headers']['x'].mutate()  -- value-object mutation; not guarded
    """
    snapshot = {**request}
    for key in ('params', 'headers', 'json', 'data'):
        val = snapshot.get(key)
        if isinstance(val, dict):
            snapshot[key] = {**val}
    return snapshot


class StateView(dict[str, Any]):
    """
    read-only snapshot of run-local page_state passed to pagination callbacks.

    raises StateViewMutationError immediately if a callback attempts to write to it directly.
    this catches the common mistake of state['key'] = value inside a callback — a write that
    looks like it should persist but is silently dropped because the snapshot is a copy.

    to persist values across pages:   return {'key': value} from update_state callback.
    to read pagination counters:       use state['_request']['params']['page'] etc. —
                                       the current request is always available in state['_request'].

    note: StateView is a thin dict subclass. isinstance(state, dict) is True.
    top-level keys are read-only. nested dicts remain plain dicts — the mutation guard does
    not recurse. state['_request'] is a defensive snapshot with one-level copies of params,
    headers, json, and data, which blocks the common request-mutation footguns
    (state['_request']['params']['page'] = 999) but is not a deep-immutable structure:
    value-object mutations (state['_request']['headers']['x'].mutate()) are not guarded.
    """

    def _raise_mutation(self, key):
        raise StateViewMutationError(key)

    def __setitem__(self, key, value):
        self._raise_mutation(key)

    def __delitem__(self, key):
        self._raise_mutation(key)

    def update(self, *args, **kwargs):
        # peek at first key lazily — do not materialise the entire input
        key = '...'
        if args:
            a = args[0]
            if hasattr(a, 'keys'):
                key = next(iter(a.keys()), '...')
            else:
                first = next(iter(a), None)
                if first is not None:
                    key = first[0] if hasattr(first, '__getitem__') else str(first)
        elif kwargs:
            key = next(iter(kwargs), '...')
        self._raise_mutation(key)

    def pop(self, key, *args):
        self._raise_mutation(key)

    def setdefault(self, key, *args):
        self._raise_mutation(key)

    def clear(self):
        self._raise_mutation('(clear)')


# built-in pagination strategies as plain functions
# each returns a pagination sub-dict ready to drop into a schema
# treat these as starting points — override any callback as needed


def _normalize_path(path: PathLike) -> tuple[PathPart, ...] | None:
    if path is None:
        return None
    if isinstance(path, bytes):
        # bytes paths aren't part of the public contract, but users will do it.
        # coerce to text so the returned parts stay within PathPart (str | int).
        path = path.decode('utf-8', errors='replace')
    if isinstance(path, str):
        return tuple(part for part in path.split('.') if part)

    def _coerce_part(part: Any) -> PathPart:
        if isinstance(part, bytes):
            return part.decode('utf-8', errors='replace')
        if isinstance(part, (str, int)):
            return part
        # keep this function total: if a user feeds in weird stuff,
        # treat it as a string key.
        return str(part)

    if isinstance(path, (tuple, list)):
        return tuple(_coerce_part(part) for part in path)
    # PathLike accepts any iterable of PathPart (not just tuple/list).
    # Avoid treating a scalar PathPart as an iterable.
    if isinstance(path, ABCIterable) and not isinstance(path, (str, bytes)):
        return tuple(_coerce_part(part) for part in path)
    if isinstance(path, int):
        return (path,)
    return (str(path),)


def _resolve_path(resp: Any, path: PathLike, default: Any = None) -> Any:
    parts = _normalize_path(path)
    if parts is None:
        return default
    value = resp
    for part in parts:
        if isinstance(value, dict):
            value = value.get(part)
        elif isinstance(value, list):
            try:
                idx = int(part)
            except (TypeError, ValueError):
                return default
            if idx < 0 or idx >= len(value):
                return default
            value = value[idx]
        else:
            return default
        if value is None:
            return default
    return value


def _safe_int(value, fallback, context=''):
    """
    converts value to int, returning fallback on failure.
    logs a warning for non-numeric values. callers decide whether to
    stop pagination or fall back to heuristics.

    examples:
        _safe_int('629', 0)          -> 629
        _safe_int('unknown', 0)      -> 0  (+ warning)
        _safe_int('25 items', 0)     -> 0  (+ warning)
        _safe_int(None, 0)           -> 0
    """
    if value is None:
        return fallback
    try:
        return int(value)
    except (ValueError, TypeError):
        logger.debug(
            'pagination: could not parse %r as int%s', value, f' ({context})' if context else ''
        )
        return fallback


def _safe_items(value, data_key=''):
    """
    ensures the items value is a list.
    returns [] with a warning if value is None, a dict, or any non-list type.
    protects len() and iteration from crashing on unexpected response shapes.
    """
    if isinstance(value, list):
        return value
    if value is None:
        logger.debug('pagination: data_key %r resolved to None — treating as empty page', data_key)
    else:
        logger.debug(
            'pagination: data_key %r resolved to %s, expected list — treating as empty page',
            data_key,
            type(value).__name__,
        )
    return []


def offset_pagination(
    offset_param: str = 'offset',
    limit_param: str = 'limit',
    limit: int = 100,
    data_path: PathLike = 'items',
    total_path: PathLike = None,
    path_resolver: PathResolver | None = None,
    params_mode: str = 'merge',
) -> PaginationConfig:
    params_mode = _validate_params_mode(params_mode)

    def next_request(parsed_body: Any, state: Any) -> dict[str, Any] | None:
        resolver = path_resolver or _resolve_path
        items = _safe_items(resolver(parsed_body, data_path), data_path)
        if not items:
            return None
        req_params = (state.get('_request') or {}).get('params') or {}
        current_offset = _safe_int(
            req_params.get(offset_param)
            if req_params.get(offset_param) is not None
            else state.get('offset', 0),
            0,
            'offset',
        )
        effective_limit = _safe_int(
            req_params.get(limit_param)
            if req_params.get(limit_param) is not None
            else state.get(limit_param, limit),
            limit,
            limit_param,
        )
        next_offset = current_offset + len(items)

        if total_path:
            raw_total = resolver(parsed_body, total_path)
            total = _safe_int(raw_total, None, total_path)
            if total is None:
                logger.warning(
                    'offset_pagination: invalid total at %r: %r; stopping', total_path, raw_total
                )
                return None
            if next_offset >= total:
                return None
        else:
            if len(items) < effective_limit:
                return None
        result: dict[str, Any] = {
            'params': {offset_param: next_offset, limit_param: effective_limit}
        }
        if params_mode != 'merge':
            result['params_mode'] = params_mode
        return result

    def on_response(resp: Any, state: Any) -> list[Any]:
        resolver = path_resolver or _resolve_path
        return _safe_items(resolver(resp, data_path), data_path)

    return cast(
        'PaginationConfig',
        {
            '_rf_pagination_helper': True,
            'next_request': next_request,
            'on_response': on_response,
            'initial_params': {offset_param: 0, limit_param: limit},
        },
    )


def cursor_pagination(
    cursor_param: str | None = None,
    next_cursor_path: PathLike = None,
    data_path: PathLike = 'items',
    path_resolver: PathResolver | None = None,
    params_mode: str = 'merge',
) -> PaginationConfig:
    params_mode = _validate_params_mode(params_mode)
    if cursor_param is None:
        raise TypeError('cursor_pagination() missing required argument: cursor_param')
    if next_cursor_path is None:
        raise TypeError('cursor_pagination() missing required argument: next_cursor_path')

    def next_request(parsed_body: Any, state: Any) -> dict[str, Any] | None:
        resolver = path_resolver or _resolve_path
        cursor = resolver(parsed_body, next_cursor_path)
        if not cursor:
            return None
        result: dict[str, Any] = {'params': {cursor_param: cursor}}
        if params_mode != 'merge':
            result['params_mode'] = params_mode
        return result

    def on_response(resp: Any, state: Any) -> list[Any]:
        resolver = path_resolver or _resolve_path
        return _safe_items(resolver(resp, data_path), data_path)

    return cast(
        'PaginationConfig',
        {
            '_rf_pagination_helper': True,
            'next_request': next_request,
            'on_response': on_response,
        },
    )


def page_number_pagination(
    page_param: str = 'page',
    page_size_param: str = 'page_size',
    page_size: int = 100,
    data_path: PathLike = 'items',
    total_pages_path: PathLike = None,
    path_resolver: PathResolver | None = None,
    params_mode: str = 'merge',
) -> PaginationConfig:
    params_mode = _validate_params_mode(params_mode)

    def next_request(parsed_body: Any, state: Any) -> dict[str, Any] | None:
        resolver = path_resolver or _resolve_path
        items = _safe_items(resolver(parsed_body, data_path), data_path)
        if not items:
            return None
        req_params = (state.get('_request') or {}).get('params') or {}
        current_page = _safe_int(
            req_params.get(page_param)
            if req_params.get(page_param) is not None
            else state.get('page', 1),
            1,
            'page',
        )
        effective_page_size = _safe_int(
            req_params.get(page_size_param)
            if req_params.get(page_size_param) is not None
            else state.get(page_size_param, page_size),
            page_size,
            page_size_param,
        )
        next_page = current_page + 1

        if total_pages_path:
            raw_total = resolver(parsed_body, total_pages_path)
            total_pages = _safe_int(raw_total, None, total_pages_path)
            if total_pages is None:
                logger.warning(
                    'page_number_pagination: invalid total_pages at %r: %r; stopping',
                    total_pages_path,
                    raw_total,
                )
                return None
            if current_page >= total_pages:
                return None
        else:
            if len(items) < effective_page_size:
                return None
        result: dict[str, Any] = {
            'params': {page_param: next_page, page_size_param: effective_page_size}
        }
        if params_mode != 'merge':
            result['params_mode'] = params_mode
        return result

    def on_response(resp: Any, state: Any) -> list[Any]:
        resolver = path_resolver or _resolve_path
        return _safe_items(resolver(resp, data_path), data_path)

    return cast(
        'PaginationConfig',
        {
            '_rf_pagination_helper': True,
            'next_request': next_request,
            'on_response': on_response,
            'initial_params': {page_param: 1, page_size_param: page_size},
        },
    )


def link_header_pagination(data_path: PathLike = 'items') -> PaginationConfig:
    """
    pagination via RFC 5988 Link header.
    looks for rel="next" in the Link header and uses that url directly.
    header lookup is case-insensitive.

    schema usage:
        'pagination': link_header_pagination(data_path='results')

    covers apis like github, which return:
        Link: <https://api.github.com/repos?page=2>; rel="next"
    """

    def next_request(_parsed_body: Any, state: Any) -> dict[str, Any] | None:
        headers = state.get('_response_headers', {})
        link_header = ''
        for key, value in headers.items():
            if isinstance(key, str) and key.casefold() == 'link':
                link_header = value
                break
        next_url = _parse_link_next(link_header)
        if not next_url:
            return None
        # returning full url override — FetchJob will use it as-is
        return {'url': next_url}

    def on_response(resp: Any, state: Any) -> Any:
        if isinstance(resp, dict):
            resolved = _resolve_path(resp, data_path)
            return _safe_items(resolved, data_path) if resolved is not None else resp
        return resp

    return cast(
        'PaginationConfig',
        {
            '_rf_pagination_helper': True,
            'next_request': next_request,
            'on_response': on_response,
        },
    )


def url_header_pagination(header_name: str, data_path: PathLike = 'items') -> PaginationConfig:
    """
    pagination via a named response header whose value is the next URL.
    header lookup is case-insensitive. if the header is absent or empty,
    pagination stops normally. the header value is used as-is as the next URL.
    """

    def next_request(_parsed_body: Any, state: Any) -> dict[str, Any] | None:
        headers = state.get('_response_headers', {})
        next_url = ''
        for key, value in headers.items():
            if isinstance(key, str) and key.casefold() == header_name.casefold():
                next_url = value
                break
        if not next_url:
            return None
        return {'url': next_url}

    def on_response(resp: Any, state: Any) -> Any:
        if isinstance(resp, dict):
            resolved = _resolve_path(resp, data_path)
            return _safe_items(resolved, data_path) if resolved is not None else resp
        return resp

    return cast(
        'PaginationConfig',
        {
            '_rf_pagination_helper': True,
            'next_request': next_request,
            'on_response': on_response,
        },
    )


def _parse_link_next(link_header: str) -> str | None:
    """
    parses RFC 5988 Link header, returns the url for rel="next" or None.

    handles:
      - multiple link parts:   <url1>; rel="prev", <url2>; rel="next"
      - rel appears anywhere:  <url>; type="text/html"; rel="next"
      - space-separated rels:  <url>; rel="next prev"
      - multiple rel= params:  <url>; rel="next"; rel="prefetch"
      - case variations:       REL="Next", rel=next (no quotes)

    limitation: splits on commas to separate link parts, so URLs containing
    literal commas in query strings would be misparsed. this is vanishingly rare
    in practice and a full RFC 7230 parser is out of scope.
    """
    if not link_header:
        return None
    for part in link_header.split(','):
        segments = [s.strip() for s in part.split(';')]
        if len(segments) < 2:
            continue
        url_part = segments[0]
        # collect all rel= segments (there may be more than one; order is arbitrary)
        rel_values: set[str] = set()
        for seg in segments[1:]:
            seg_lower = seg.lower()
            if not seg_lower.lstrip().startswith('rel='):
                continue
            # strip: rel= , rel=" , REL= etc. then split on whitespace for "next prev" form
            value = seg.split('=', 1)[1].strip()
            value = value.strip('"').strip("'")
            rel_values.update(v.lower() for v in value.split())
        if 'next' in rel_values:
            url = url_part.strip()
            url = url[1:-1].strip() if url.startswith('<') and url.endswith('>') else url
            return url
    return None


class CycleRunner:
    """
    orchestrates the pagination loop for a single FetchJob.
    constructed from the resolved pagination config dict.

    the loop contract:
        - on_response(page_payload, state) -> page_data            called every page
        - next_request(parsed_body, state) -> dict|None            None means stop
        - update_state(page_payload, state) -> dict                optional, mutates state
        - on_page(page_data, state)                       optional side effect per page
        - on_complete(...) is mode-specific:
            fetch()  -> on_complete(results, state) -> final result
            stream() -> on_complete(summary, state)       return value ignored

    state is a plain dict carried across all pages.
    _response_headers is injected into state automatically so pagination
    callbacks can access headers without needing the raw http response.
    """

    def __init__(self, pagination_config):
        self._next_request = None
        self._on_response = None
        self._update_state = None
        self._on_page = None
        self._on_complete = None
        self._on_page_complete = None
        self._delay = 0.0
        self._initial_params = {}

        if pagination_config is None:
            return

        cfg = pagination_config

        self._next_request = cfg.get('next_request')
        self._on_response = cfg.get('on_response')
        self._update_state = cfg.get('update_state')
        self._on_page = cfg.get('on_page')
        self._on_complete = cfg.get('on_complete')
        self._on_page_complete = cfg.get('on_page_complete')
        self._delay = cfg.get('delay', 0.0)
        self._initial_params = cfg.get('initial_params', {})

        if self._next_request is None:
            raise PaginationError('pagination is enabled but next_request callback is not defined')

    @property
    def initial_params(self) -> dict[str, Any]:
        "extra params to inject into the first request when using built-in strategies"
        return self._initial_params

    def _record_stop(self, page_state: dict[str, Any], stop: StopSignal) -> None:
        page_state['_stop_signal'] = stop
        stop_view: dict[str, Any] = {'kind': stop.kind}
        if stop.limit is not None:
            stop_view['limit'] = stop.limit
        if stop.observed is not None:
            stop_view['observed'] = stop.observed
        page_state['stop'] = stop_view

    def run(
        self,
        # fetch_fn stays broadly typed because the concrete callable varies across live http,
        # playback, and tests, but all implementations follow the documented request/ctx/run_state contract.
        fetch_fn: Callable[..., Any],
        initial_request: dict[str, Any],
        ctx: Any = None,
        *,
        run_state: _RunStateLike | None = None,
        initial_state: dict[str, Any] | None = None,
        mode: Literal['fetch', 'stream'] = 'fetch',
    ) -> Any:
        """
        runs the full pagination loop.
        fetch_fn(request_kwargs, ctx, *, run_state=None) -> _RequestOutcome
        ctx is always passed; fetch_fn implementations that don't use it should accept ctx=None.
        run_state is forwarded unchanged for per-run execution state (mock sequencing, playback capture, etc).

        ctx: optional OperationContext for safety caps:
            max_pages     — checked before fetching page N+1 (after page N completes)
                            so a cleanly completed page still counts as observed work
            max_requests  — checked inside RetryHandler before each http attempt
            time_limit    — checked before each page fetch and inside RetryHandler

        fetch mode:
            - without on_complete: yields page_data per page to the collector
            - with on_complete: collects all pages and yields the transformed final result once

        stream mode:
            - always yields page_data per page
            - stream completion callbacks are handled by the outer fetch job so
              the same terminal StreamSummary can feed stream_run() and metrics
              session recording without duplicate summary construction

        state scope:
            page_state is the single run-local state dict for this run(). It is seeded once
            from initial_state, then initial_params override that seed, then runner metadata
            such as _request is injected. Callbacks receive StateView(page_state).
        """
        page_state = dict(initial_state or {})
        request = dict(initial_request)
        if self._initial_params:
            page_state.update(self._initial_params)
        page_state.update(request.get('params', {}))
        if run_state is not None:
            run_state.page_state = page_state
            run_state.expose(page_state)
        all_pages: list[Any] | None = [] if (mode == 'fetch' and self._on_complete) else None
        page_num = 0
        page_state['_completed_pages'] = 0

        while True:
            page_num += 1
            logger.debug('fetching page %d', page_num)

            if ctx is not None:
                ctx.check_deadline()
                stop = ctx.check_max_pages()
                if stop is not None:
                    self._record_stop(page_state, stop)
                    if run_state is not None:
                        run_state.emit_event(
                            now_event(
                                kind='stopped',
                                source=_event_source(run_state),
                                endpoint=run_state.endpoint_name,
                                url=request.get('url'),
                                data=run_state.stop_event_data(stop),
                            )
                        )
                    logger.debug(
                        'max_pages reached: %d / %d — stopping cleanly', stop.observed, stop.limit
                    )
                    break

            if run_state is not None:
                run_state.reset_page_cycle()

            fetch_result = fetch_fn(request, ctx, run_state=run_state)
            if isinstance(fetch_result, StopSignal):
                self._record_stop(page_state, fetch_result)
                if run_state is not None:
                    run_state.emit_event(
                        now_event(
                            kind='stopped',
                            source=_event_source(run_state),
                            endpoint=run_state.endpoint_name,
                            url=request.get('url'),
                            data=run_state.stop_event_data(fetch_result),
                        )
                    )
                logger.debug(
                    'max_requests reached: %d / %d — stopping cleanly',
                    fetch_result.observed,
                    fetch_result.limit,
                )
                # fire on_page_complete for error_stop outcomes so hook can observe them
                # (no sleep is performed since stop is already decided; return is still validated)
                if (
                    self._on_page_complete is not None
                    and run_state is not None
                    and fetch_result.kind == 'error_stop'
                ):
                    _last_err = run_state._last_error
                    _err_status = getattr(_last_err, 'status_code', None)
                    _err_outcome = run_state.build_page_cycle_outcome(
                        status_code=_err_status,
                        error=_last_err,
                        stop_signal=fetch_result,
                    )
                    _raw = self._on_page_complete(_err_outcome, StateView(page_state))
                    _validate_adaptive_delay(_raw)  # enforce contract; sleep is skipped
                break

            page_payload = fetch_result.parsed
            parsed_body = getattr(fetch_result, 'parsed_body', page_payload)
            headers = fetch_result.headers
            post_auth_request = fetch_result.request_kwargs
            page_state['_response_headers'] = headers

            if run_state is not None:
                run_state.emit_event(
                    now_event(
                        kind='page_parsed',
                        source=_event_source(run_state),
                        endpoint=run_state.endpoint_name,
                        url=post_auth_request.get('url'),
                        data=run_state.page_parsed_event_data(
                            status_code=headers.get('_status_code')
                        ),
                    )
                )

            # expose a defensive snapshot of the effective request to callbacks.
            # this preserves useful request context in state['_request'] without letting
            # callbacks mutate the live request that produced this page.
            request_snapshot = _copy_request_for_snapshot(post_auth_request)
            page_state['_request'] = request_snapshot
            if run_state is not None:
                run_state.expose(page_state)
            state = StateView(page_state)

            if self._on_response:
                try:
                    page_data = safe_call(
                        self._on_response, page_payload, state, name='on_response'
                    )
                except CallbackError as exc:
                    if run_state is not None:
                        run_state.emit_event(
                            now_event(
                                kind='callback_error',
                                source=_event_source(run_state),
                                endpoint=run_state.endpoint_name,
                                url=post_auth_request.get('url'),
                                data={
                                    'callback': 'on_response',
                                    'exception_type': type(exc).__name__,
                                    'exception_msg': str(exc),
                                },
                            )
                        )
                    raise
            else:
                page_data = page_payload

            try:
                safe_call(self._on_page, page_data, state, name='on_page')
            except CallbackError as exc:
                if run_state is not None:
                    run_state.emit_event(
                        now_event(
                            kind='callback_error',
                            source=_event_source(run_state),
                            endpoint=run_state.endpoint_name,
                            url=post_auth_request.get('url'),
                            data={
                                'callback': 'on_page',
                                'exception_type': type(exc).__name__,
                                'exception_msg': str(exc),
                            },
                        )
                    )
                raise

            if all_pages is not None:
                all_pages.append(page_data)
            else:
                yield page_data

            if self._update_state:
                try:
                    updated = safe_call(
                        self._update_state, page_payload, state, name='update_state'
                    )
                except CallbackError as exc:
                    if run_state is not None:
                        run_state.emit_event(
                            now_event(
                                kind='callback_error',
                                source=_event_source(run_state),
                                endpoint=run_state.endpoint_name,
                                url=post_auth_request.get('url'),
                                data={
                                    'callback': 'update_state',
                                    'exception_type': type(exc).__name__,
                                    'exception_msg': str(exc),
                                },
                            )
                        )
                    raise
                # update_state is the one callback allowed to persist values across pages.
                # it returns a dict to merge into the live run-local page_state.
                if isinstance(updated, dict) and updated:
                    page_state.update(updated)
                    if run_state is not None:
                        run_state.expose(page_state)
                    state = StateView(page_state)

            # mark the page complete only after on_response/on_page/update_state/next_request
            # have all run successfully for this page.
            if ctx is not None:
                ctx.record_page()
                page_state['_completed_pages'] = ctx.page_count
            else:
                page_state['_completed_pages'] = page_state.get('_completed_pages', 0) + 1
            if run_state is not None:
                run_state.mark_page_complete()
                run_state.expose(page_state)

            # on_page_complete hook — fires after full page cycle completes,
            # before next_request is called. not fired on on_error->'raise'.
            # Returns an adaptive delay (seconds) to apply if pagination continues.
            _adaptive_delay_s = None
            if self._on_page_complete is not None and run_state is not None:
                _cycle_stop = (
                    page_state.get('_stop_signal')
                    if isinstance(page_state.get('_stop_signal'), StopSignal)
                    else None
                )
                _last_err = run_state._last_error
                _outcome = run_state.build_page_cycle_outcome(
                    status_code=headers.get('_status_code') if headers else None,
                    error=_last_err,
                    stop_signal=_cycle_stop,
                )
                _raw_delay = self._on_page_complete(_outcome, StateView(page_state))
                _adaptive_delay_s = _validate_adaptive_delay(_raw_delay)
                # if stop already decided, do not sleep even if hook returned a value
                if _cycle_stop is not None:
                    _adaptive_delay_s = None

            try:
                overrides = safe_call(self._next_request, parsed_body, state, name='next_request')
            except CallbackError as exc:
                if run_state is not None:
                    run_state.emit_event(
                        now_event(
                            kind='callback_error',
                            source=_event_source(run_state),
                            endpoint=run_state.endpoint_name,
                            url=request.get('url'),
                            data={
                                'callback': 'next_request',
                                'exception_type': type(exc).__name__,
                                'exception_msg': str(exc),
                            },
                        )
                    )
                raise

            if overrides is None:
                stop = StopSignal(kind='next_request_none')
                self._record_stop(page_state, stop)
                if run_state is not None:
                    run_state.emit_event(
                        now_event(
                            kind='stopped',
                            source=_event_source(run_state),
                            endpoint=run_state.endpoint_name,
                            url=request.get('url'),
                            data=run_state.stop_event_data(stop),
                        )
                    )
                logger.debug(
                    'next_request returned None — pagination complete after %d pages', page_num
                )
                break

            # params_mode='replace' means replace the params mapping entirely instead of
            # merge_dicts() recursively merging it with the previous request params.
            params_mode = overrides.pop('params_mode', 'merge')
            if params_mode not in {'merge', 'replace'}:
                raise CallbackError(
                    f"next_request returned invalid params_mode {params_mode!r} — expected 'merge' or 'replace'",
                    callback_name='next_request',
                    cause=None,
                )
            if params_mode == 'replace':
                overrides.setdefault('params', {})
                request = merge_dicts(request, overrides)
                request['params'] = dict(overrides['params'] or {})
            else:
                request = merge_dicts(request, overrides)

            # static delay and adaptive delay are additive. both only apply when
            # pagination continues (next_request returned non-None above). the
            # implementation currently sleeps in static-then-adaptive order, but
            # callers should treat only the total inter-page delay as contractual.
            if self._delay > 0:
                logger.debug('pagination delay: sleeping %.2fs', self._delay)
                time.sleep(self._delay)
                if run_state is not None:
                    run_state.mark_wait('static', self._delay)
                if ctx is not None:
                    # re-check the deadline after sleep so long pagination delays still obey
                    # the time_limit even when the request itself would otherwise be fast.
                    ctx.check_deadline()

            if _adaptive_delay_s is not None and _adaptive_delay_s > 0:
                logger.debug('adaptive delay: sleeping %.3fs', _adaptive_delay_s)
                time.sleep(_adaptive_delay_s)
                if run_state is not None:
                    run_state.mark_wait('adaptive', _adaptive_delay_s)
                    run_state.emit_event(
                        now_event(
                            kind='adaptive_wait_end',
                            source=_event_source(run_state),
                            endpoint=run_state.endpoint_name,
                            url=post_auth_request.get('url'),
                            data=run_state.wait_event_data(
                                'adaptive', wait_ms=_adaptive_delay_s * 1000.0
                            ),
                        )
                    )
                if ctx is not None:
                    ctx.check_deadline()

        final_page_state = {
            k: v
            for k, v in page_state.items()
            if k not in {'_completed_pages', '_request', '_response_headers', '_stop_signal'}
        }
        final_state = StateView(final_page_state)

        if mode == 'fetch' and all_pages is not None:
            try:
                result = safe_call(self._on_complete, all_pages, final_state, name='on_complete')
            except CallbackError as exc:
                if run_state is not None:
                    run_state.emit_event(
                        now_event(
                            kind='callback_error',
                            source=_event_source(run_state),
                            endpoint=run_state.endpoint_name,
                            url=request.get('url'),
                            data={
                                'callback': 'on_complete',
                                'exception_type': type(exc).__name__,
                                'exception_msg': str(exc),
                            },
                        )
                    )
                raise
            yield (result if result is not None else all_pages)
            return

        # stream-mode on_complete is handled by _FetchJob so the same terminal
        # StreamSummary object can be shared with metrics-session recording and
        # stream_run() summary capture. runner.run() leaves stream completion
        # callback invocation to that outer layer.


def build_cycle_runner(pagination_config: dict[str, Any] | None) -> CycleRunner:
    """Build a CycleRunner from a pagination config dict."""
    return CycleRunner(pagination_config)
