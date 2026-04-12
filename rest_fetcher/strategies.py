from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from collections.abc import Iterable as ABCIterable
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from .types import PaginationConfig

logger = logging.getLogger('rest_fetcher.pagination')

# ---------------------------------------------------------------------------
# Public path-traversal types re-exported for callers that write custom
# pagination helpers using the same path resolver conventions.
# ---------------------------------------------------------------------------

PathPart = str | int
PathLike = str | Iterable[PathPart] | PathPart | None
PathResolver = Callable[[Any, PathLike], Any]


# ---------------------------------------------------------------------------
# Internal helpers shared across strategy implementations
# ---------------------------------------------------------------------------


def _validate_params_mode(params_mode: str) -> str:
    if params_mode not in {'merge', 'replace'}:
        raise ValueError(f"params_mode must be 'merge' or 'replace', got {params_mode!r}")
    return params_mode


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


# ---------------------------------------------------------------------------
# Built-in pagination strategies
# each returns a pagination sub-dict ready to drop into a schema.
# treat these as starting points — override any callback as needed.
# ---------------------------------------------------------------------------


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
