from __future__ import annotations

import logging
import numbers as _numbers
import time
from collections.abc import Callable
from typing import Any, Literal, Protocol

from .callbacks import safe_call
from .events import EventSource, now_event
from .exceptions import CallbackError, PaginationError, StateViewMutationError
from .schema import merge_dicts
from .strategies import (
    PathLike,
    PathPart,
    PathResolver,
    _normalize_path,  # noqa: F401 — re-exported; tests and helpers import from here
    _parse_link_next,  # noqa: F401
    _resolve_path,  # noqa: F401
    _safe_int,  # noqa: F401
    _safe_items,  # noqa: F401
    cursor_pagination,
    link_header_pagination,
    offset_pagination,
    page_number_pagination,
    url_header_pagination,
)
from .types import PageCycleOutcome, StopSignal

# Transitional compatibility re-exports — prefer rest_fetcher.strategies for new code.
# CycleRunner and build_cycle_runner are aliases kept for backward compatibility;
# the canonical names are PaginationRunner and build_pagination_runner.
__all__ = [
    'PaginationRunner',
    'StateView',
    'build_pagination_runner',
    # compatibility aliases — not removed yet
    'CycleRunner',
    'build_cycle_runner',
    # strategies
    'cursor_pagination',
    'link_header_pagination',
    'offset_pagination',
    'page_number_pagination',
    'url_header_pagination',
    # path type utilities
    'PathLike',
    'PathPart',
    'PathResolver',
]

logger = logging.getLogger('rest_fetcher.pagination')


class _RunStateLike(Protocol):
    """Internal run-state contract consumed by PaginationRunner."""

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


def _validate_adaptive_delay(value: Any) -> float | None:
    """Validates the return value of on_page_complete.

    Returns the delay as a float, or None if no delay should be applied.
    Raises CallbackError for invalid return values.
    """
    if value is None:
        return None
    # bool is a subclass of int — reject it explicitly to prevent True -> 1.0 footgun
    if isinstance(value, bool):
        raise CallbackError(
            f'on_page_complete returned bool ({value!r}) — return a float in seconds, None, or 0 for no delay',
            callback_name='on_page_complete',
            cause=None,
        )
    if value == 0:
        return None
    if not isinstance(value, _numbers.Real):
        raise CallbackError(
            f'on_page_complete must return a float (seconds), None, or 0 — got {type(value).__name__!r}',
            callback_name='on_page_complete',
            cause=None,
        )
    delay = float(value)
    if delay < 0:
        raise CallbackError(
            f'on_page_complete returned negative delay ({delay}) — delay must be >= 0',
            callback_name='on_page_complete',
            cause=None,
        )
    return delay if delay > 0 else None


def _event_source(run_state: _RunStateLike | None) -> EventSource:
    if run_state is None:
        return 'live'
    source = run_state.event_source
    if source not in ('live', 'playback'):
        raise RuntimeError(f'unexpected event_source: {source!r}')
    return source


def _cb_call(
    fn: Any,
    *args: Any,
    name: str,
    run_state: _RunStateLike | None,
    url: str | None,
) -> Any:
    """Call a pagination callback via safe_call, emitting a callback_error event on failure."""
    try:
        return safe_call(fn, *args, name=name)
    except CallbackError as exc:
        if run_state is not None:
            run_state.emit_event(
                now_event(
                    kind='callback_error',
                    source=_event_source(run_state),
                    endpoint=run_state.endpoint_name,
                    url=url,
                    data={
                        'callback': name,
                        'exception_type': type(exc).__name__,
                        'exception_msg': str(exc),
                    },
                )
            )
        raise


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


class PaginationRunner:
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

            _post_url = post_auth_request.get('url')
            if self._on_response:
                page_data = _cb_call(
                    self._on_response,
                    page_payload,
                    state,
                    name='on_response',
                    run_state=run_state,
                    url=_post_url,
                )
            else:
                page_data = page_payload

            _cb_call(
                self._on_page,
                page_data,
                state,
                name='on_page',
                run_state=run_state,
                url=_post_url,
            )

            if all_pages is not None:
                all_pages.append(page_data)
            else:
                yield page_data

            if self._update_state:
                updated = _cb_call(
                    self._update_state,
                    page_payload,
                    state,
                    name='update_state',
                    run_state=run_state,
                    url=_post_url,
                )
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

            overrides = _cb_call(
                self._next_request,
                parsed_body,
                state,
                name='next_request',
                run_state=run_state,
                url=request.get('url'),
            )

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
            result = _cb_call(
                self._on_complete,
                all_pages,
                final_state,
                name='on_complete',
                run_state=run_state,
                url=request.get('url'),
            )
            yield (result if result is not None else all_pages)
            return

        # stream-mode on_complete is handled by _FetchJob so the same terminal
        # StreamSummary object can be shared with metrics-session recording and
        # stream_run() summary capture. runner.run() leaves stream completion
        # callback invocation to that outer layer.


def build_pagination_runner(pagination_config: dict[str, Any] | None) -> PaginationRunner:
    """Build a PaginationRunner from a pagination config dict."""
    return PaginationRunner(pagination_config)


# Compatibility aliases — kept for backward compatibility; prefer the canonical names above.
CycleRunner = PaginationRunner
build_cycle_runner = build_pagination_runner
