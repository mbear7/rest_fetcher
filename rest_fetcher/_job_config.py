from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .events import PaginationEvent
from .pagination import PaginationRunner, build_pagination_runner
from .playback import PlaybackHandler, build_playback_handler
from .types import CanonicalParserFn, ResponseParser

_LOG_LEVELS = {
    'none': logging.CRITICAL + 1,
    'error': logging.ERROR,
    'medium': logging.INFO,
    'verbose': logging.DEBUG,
}

_MISSING = object()

_TOKEN_BUCKET_KEYS = {'strategy', 'requests_per_second', 'burst', 'on_limit', 'clock', 'sleep'}

TimeoutValue = float | tuple[float, float]


def _has_token_bucket_config(cfg: dict[str, Any] | None) -> bool:
    return isinstance(cfg, dict) and any(k in cfg for k in _TOKEN_BUCKET_KEYS)


def _resolve(key: str, *dicts: dict[str, Any], default: Any = None) -> Any:
    """resolve a config value by priority, skipping None.

    priority order is left-to-right; the first non-None value wins.
    """
    for d in dicts:
        v = d.get(key)
        if v is not None:
            return v
    return default


def _resolve_explicit(key: str, *dicts: dict[str, Any], default: Any = None) -> Any:
    """resolve a config value by priority, preserving explicit None.

    priority order is left-to-right; the first dict that defines the key wins,
    even when the value is None. This is used for settings where `None` is an
    intentional override rather than "keep searching".
    """
    for d in dicts:
        if key in d:
            return d[key]
    return default


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
    runner: PaginationRunner
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


def _resolve_job_config(
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
    retry_config = cfg.get('retry', _MISSING)

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
    _lifecycle_hooks = {hk: cfg[hk] if hk in cfg else _helper_hooks.get(hk) for hk in _hook_names}

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
    runner = build_pagination_runner(runner_cfg)
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
