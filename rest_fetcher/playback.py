from __future__ import annotations

import base64
import json
import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit

from requests.structures import CaseInsensitiveDict

from .exceptions import FixtureFormatError, PlaybackError
from .parsing import VALID_RESPONSE_FORMATS

__all__ = [
    'PlaybackError',
    'FixtureFormatError',
    'RecordedResponse',
    'PlaybackHandler',
    'build_playback_handler',
    'deserialize_playback_response',
    '_scrub',
    '_scrub_recorded_url',
    '_scrub_playback_envelope',
]

logger = logging.getLogger('rest_fetcher.playback')

# ---------------------------------------------------------------------------
# Header and URL scrubbing — shared between playback recording and request logging
# ---------------------------------------------------------------------------

# default set of header names (lowercase) whose values are always redacted
_SCRUB_HEADERS_DEFAULT = {
    'authorization',
    'x-api-key',
    'x-api-secret',
    'x-auth-token',
    'api-key',
    'proxy-authorization',
    'cookie',
    'set-cookie',
}

# substring patterns: any header key containing one of these (case-insensitive) is scrubbed
_SCRUB_PATTERNS = ('token', 'secret', 'password', 'key', 'auth')

_SCRUB_QUERY_PARAMS_DEFAULT = {
    'access_token',
    'refresh_token',
    'api_key',
    'client_secret',
    'token',
    'sig',
    'signature',
}
_SCRUB_QUERY_PATTERNS = ('token', 'secret', 'key', 'sig', 'auth')


def _scrub(headers: dict[str, Any], extra_scrub: list[str] | None = None) -> dict[str, Any]:
    """
    redacts sensitive header values before logging or recording.
    exact-match: _SCRUB_HEADERS_DEFAULT + schema scrub_headers list.
    pattern-match: any header key containing 'token', 'secret', 'password', 'key', 'auth'.
    """
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
    """
    redact sensitive query parameter values in recorded playback URLs.
    exact-match: built-in query defaults + schema scrub_query_params list.
    pattern-match: any query key containing one of _SCRUB_QUERY_PATTERNS.
    """
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


_TEXT_FORMATS = {'json', 'xml', 'csv', 'text'}
_XML_DECL_RE = re.compile(r"^\s*<\?xml[^>]*encoding\s*=\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)


def _validate_xml_body(body: str) -> None:
    match = _XML_DECL_RE.search(body)
    if match and match.group(1).lower() != 'utf-8':
        raise FixtureFormatError(
            'xml textual fixture must declare utf-8 or omit the xml encoding declaration'
        )


@dataclass
class RecordedResponse:
    status_code: int
    headers: Mapping[str, Any]
    format: str
    url: str = ''
    body: str | None = None
    content_b64: str | None = None
    encoding: str | None = None

    @property
    def ok(self):
        return self.status_code < 400

    @property
    def text(self):
        if self.body is not None:
            return self.body
        content = self.content
        chosen = self.encoding or 'utf-8'
        try:
            return bytes(content).decode(chosen)
        except (LookupError, UnicodeDecodeError):
            return bytes(content).decode('utf-8', errors='replace')

    @property
    def content(self):
        if self.content_b64 is not None:
            try:
                return base64.b64decode(self.content_b64.encode('ascii'), validate=True)
            except Exception as e:
                raise FixtureFormatError(f'invalid content_b64 payload: {e}') from e

        body = self.body or ''
        if self.format == 'json':
            return body.encode('utf-8')
        if self.format == 'xml':
            return body.encode('utf-8')
        if self.format in ('csv', 'text'):
            encoding = self.encoding or 'utf-8'
            try:
                return body.encode(encoding)
            except LookupError as e:
                raise FixtureFormatError(
                    f'invalid encoding {encoding!r} for textual fixture'
                ) from e
        raise FixtureFormatError(f'cannot reconstruct content for fixture format {self.format!r}')

    def json(self):
        return json.loads(self.content)


def deserialize_playback_response(envelope: dict[str, Any]) -> RecordedResponse:
    if not isinstance(envelope, dict):
        raise FixtureFormatError(
            f'invalid playback envelope: expected dict, got {type(envelope).__name__}'
        )
    if envelope.get('kind') != 'raw_response':
        raise FixtureFormatError('invalid raw playback envelope: expected kind="raw_response"')

    fmt = envelope.get('format', 'auto')
    if fmt not in VALID_RESPONSE_FORMATS - {'auto'}:
        raise FixtureFormatError(f'invalid raw playback envelope: unsupported format {fmt!r}')

    headers = envelope.get('headers') or {}
    if not isinstance(headers, dict):
        raise FixtureFormatError('invalid raw playback envelope: headers must be a dict')
    headers = CaseInsensitiveDict(headers)

    status_code = int(envelope.get('status_code', 200))
    url = envelope.get('url') or ''
    encoding = envelope.get('encoding')
    if encoding is not None and not isinstance(encoding, str):
        raise FixtureFormatError(
            'invalid raw playback envelope: encoding must be a string if provided'
        )

    has_body = 'body' in envelope
    has_b64 = 'content_b64' in envelope

    if has_body and has_b64:
        raise FixtureFormatError(
            'invalid raw playback envelope: body and content_b64 are mutually exclusive'
        )
    if not has_body and not has_b64:
        raise FixtureFormatError(
            'invalid raw playback envelope: expected one of body or content_b64'
        )

    if fmt == 'bytes':
        if has_body:
            raise FixtureFormatError(
                'invalid raw playback envelope: bytes fixtures must use content_b64, not body'
            )
        payload = envelope.get('content_b64')
        if not isinstance(payload, str):
            if isinstance(payload, list):
                raise FixtureFormatError('chunked content_b64 (list form) is not supported')
            raise FixtureFormatError('invalid raw playback envelope: content_b64 must be a string')
        return RecordedResponse(
            status_code=status_code,
            headers=headers,
            format=fmt,
            url=url,
            content_b64=payload,
            encoding=encoding,
        )

    if has_body:
        body = envelope.get('body')
        if body is None:
            body = ''
        if not isinstance(body, str):
            raise FixtureFormatError('invalid raw playback envelope: body must be a string')
        if fmt == 'xml':
            _validate_xml_body(body)
        return RecordedResponse(
            status_code=status_code,
            headers=headers,
            format=fmt,
            url=url,
            body=body,
            encoding=encoding,
        )

    payload = envelope.get('content_b64')
    if not isinstance(payload, str):
        if isinstance(payload, list):
            raise FixtureFormatError('chunked content_b64 (list form) is not supported')
        raise FixtureFormatError('invalid raw playback envelope: content_b64 must be a string')
    return RecordedResponse(
        status_code=status_code,
        headers=headers,
        format=fmt,
        url=url,
        content_b64=payload,
        encoding=encoding,
    )


def _load_pages(path: str) -> list[Any]:
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    return data if isinstance(data, list) else [data]


def _save_pages(path: str, pages: list[Any]) -> None:
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(pages, f, ensure_ascii=False, indent=2)


def _resolve_config(playback: dict[str, Any]) -> tuple[str, str]:
    mode = playback.get('mode', 'auto')
    if mode not in ('auto', 'save', 'load', 'none'):
        raise PlaybackError(f'playback mode must be auto|save|load|none, got {mode!r}')
    if mode == 'none':
        return '', 'none'
    path = playback.get('path')
    if not path:
        raise PlaybackError('playback config requires a "path" key unless mode="none"')
    return path, mode


class PlaybackHandler:
    def __init__(self, playback_config: dict[str, Any]) -> None:
        self._path, self._mode = _resolve_config(playback_config)

    @property
    def should_load(self):
        if self._mode == 'none':
            return False
        if self._mode == 'load':
            return True
        if self._mode == 'auto':
            return os.path.exists(self._path)
        return False

    @property
    def should_save(self):
        if self._mode == 'none':
            return False
        return not self.should_load

    def load(self) -> list[Any]:
        if not os.path.exists(self._path):
            raise PlaybackError(
                f'playback file not found: {self._path!r} '
                f'(mode={self._mode!r} — run once in save or auto mode to record it)'
            )
        pages = _load_pages(self._path)
        logger.info('playback: loaded %d page(s) from %s', len(pages), self._path)
        return pages

    def save(self, pages: list[Any]) -> None:
        serialised = pages if isinstance(pages, list) else list(pages)
        _save_pages(self._path, serialised)
        logger.info('playback: saved %d page(s) to %s', len(serialised), self._path)


def build_playback_handler(playback_config: dict[str, Any] | None) -> PlaybackHandler | None:
    if playback_config is None:
        return None
    handler = PlaybackHandler(playback_config)
    if handler._mode == 'none':
        return None
    return handler
