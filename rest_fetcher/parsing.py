from __future__ import annotations

import base64
import csv
import io
import json
import re
from typing import Any
from xml.etree import ElementTree as ET

from .exceptions import FixtureFormatError, PlaybackError, ResponseError

VALID_RESPONSE_FORMATS = {'auto', 'json', 'text', 'xml', 'csv', 'bytes'}


def _has_content_bytes(response: Any) -> bool:
    # Parsing should prefer raw bytes from .content whenever available.
    # .text is a convenience/debug surface and should not be part of the
    # normal parse path.
    content = getattr(response, 'content', None)
    return isinstance(content, (bytes, bytearray)) and len(content) > 0


def _response_text_fallback(response: Any) -> str:
    # Last-resort fallback for stubs or unusual response objects.
    # Keep this small and boring: the normal parse path should use .content.
    text = getattr(response, 'text', None)
    return '' if text is None else str(text)


def _response_bytes_for_recording(response: Any, fallback_encoding: str = 'utf-8') -> bytes:
    if _has_content_bytes(response):
        return bytes(response.content)
    return _response_text_fallback(response).encode(fallback_encoding)


_XML_DECL_RE = re.compile(
    r"^\s*<\?xml[^>]*encoding\s*=\s*(['\"])([^'\"]+)\1[^>]*\?>", re.IGNORECASE
)


def detect_response_format(response: Any, configured_format: str = 'auto') -> str:
    if configured_format != 'auto':
        return configured_format
    content_type = (response.headers.get('Content-Type') or '').lower()
    if 'json' in content_type:
        return 'json'
    if 'xml' in content_type:
        return 'xml'
    if 'csv' in content_type:
        return 'csv'
    if content_type.startswith('text/'):
        return 'text'
    return 'bytes'


def decode_response_text(response: Any, encoding: str) -> str:
    return bytes(response.content).decode(encoding)


def parse_json_response(response: Any) -> Any:
    try:
        if _has_content_bytes(response):
            return json.loads(bytes(response.content).decode('utf-8'))
        return json.loads(_response_text_fallback(response))
    except Exception as e:
        raise ResponseError(
            f'invalid JSON response: {e}', raw=getattr(response, 'text', None)
        ) from e


def parse_text_response(response: Any, encoding: str = 'utf-8') -> str:
    try:
        if _has_content_bytes(response):
            return decode_response_text(response, encoding)
        return _response_text_fallback(response)
    except Exception as e:
        raise ResponseError(
            f'invalid text response: {e}', raw=getattr(response, 'text', None)
        ) from e


def parse_xml_response(response: Any) -> ET.Element:
    content = (
        bytes(response.content)
        if _has_content_bytes(response)
        else _response_text_fallback(response).encode('utf-8')
    )
    try:
        return ET.fromstring(content)
    except ET.ParseError as e:
        raise ResponseError(
            f'invalid XML response: {e}', raw=getattr(response, 'text', None)
        ) from e


def parse_csv_text(text: str, delimiter: str = ';') -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(text), delimiter=delimiter))


def parse_csv_response(
    response: Any, delimiter: str = ';', encoding: str = 'utf-8'
) -> list[dict[str, str]]:
    try:
        text = (
            decode_response_text(response, encoding)
            if _has_content_bytes(response)
            else _response_text_fallback(response)
        )
    except Exception as e:
        raise ResponseError(
            f'invalid CSV response: {e}', raw=getattr(response, 'text', None)
        ) from e
    return parse_csv_text(text, delimiter=delimiter)


def default_parse_response(
    response: Any, response_format: str = 'auto', csv_delimiter: str = ';', encoding: str = 'utf-8'
) -> Any:
    content = getattr(response, 'content', None)
    if isinstance(content, (bytes, bytearray)):
        if len(content) == 0:
            return None
    else:
        text = getattr(response, 'text', None)
        if text is None or not str(text).strip():
            return None

    if response_format == 'auto' and not ((response.headers.get('Content-Type') or '').strip()):
        try:
            return parse_json_response(response)
        except ResponseError:
            return parse_text_response(response, encoding=encoding)

    effective = detect_response_format(response, response_format)
    if effective == 'json':
        return parse_json_response(response)
    if effective == 'text':
        return parse_text_response(response, encoding=encoding)
    if effective == 'xml':
        return parse_xml_response(response)
    if effective == 'csv':
        return parse_csv_response(response, delimiter=csv_delimiter, encoding=encoding)
    if effective == 'bytes':
        return response.content
    raise ResponseError(f'unsupported response format: {effective!r}')


def _normalize_xml_text(text: str) -> str:
    if _XML_DECL_RE.search(text):
        text = _XML_DECL_RE.sub("<?xml version='1.0' encoding='utf-8'?>", text, count=1)
    return text


def _decode_for_recording(response: Any, response_format: str, encoding: str | None) -> str:
    content = _response_bytes_for_recording(response)
    if response_format == 'json':
        return content.decode('utf-8')
    if response_format == 'xml':
        chosen = encoding or 'utf-8'
        text = content.decode(chosen)
        return _normalize_xml_text(text)
    if response_format in ('csv', 'text'):
        chosen = encoding or 'utf-8'
        return content.decode(chosen)
    raise PlaybackError(f'unsupported textual playback format for recording: {response_format!r}')


def serialize_response_for_playback(
    response: Any,
    response_format: str = 'auto',
    request_kwargs: dict[str, Any] | None = None,
    encoding: str = 'utf-8',
    record_as_bytes: bool = False,
) -> dict[str, Any]:
    effective = detect_response_format(response, response_format)
    request_kwargs = request_kwargs or {}
    env = {
        'kind': 'raw_response',
        'format': effective,
        'status_code': int(getattr(response, 'status_code', 200) or 200),
        'headers': dict(getattr(response, 'headers', {}) or {}),
        'request_headers': dict(request_kwargs.get('headers', {}) or {}),
        'url': getattr(response, 'url', '') or request_kwargs.get('url') or '',
    }
    if effective in ('bytes',) or record_as_bytes:
        env['content_b64'] = base64.b64encode(
            _response_bytes_for_recording(response, fallback_encoding=encoding or 'utf-8')
        ).decode('ascii')
        if effective in ('csv', 'text', 'xml') and encoding:
            env['encoding'] = encoding
        return env
    try:
        body = _decode_for_recording(response, effective, encoding)
    except UnicodeDecodeError as e:
        raise FixtureFormatError(
            f'failed to decode {effective} payload for textual fixture storage: {e}'
        ) from e
    except LookupError as e:
        raise FixtureFormatError(
            f'invalid encoding {encoding!r} for textual fixture storage'
        ) from e
    env['body'] = body
    if effective in ('csv', 'text') and encoding:
        env['encoding'] = encoding
    return env
