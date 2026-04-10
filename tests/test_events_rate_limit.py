# Domain: runtime telemetry and proactive rate limiting.
# Place new tests here by primary behavioral contract when the assertion is
# on_event/lifecycle emission, playback event source semantics, token-bucket
# behavior, burst/refill semantics, RateLimitExceeded, or rate-limit wait/raise
# behavior. Prefer this existing domain file for that work. If a test spans
# multiple areas, place it where the main assertion belongs.

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from typing import Any

import pytest
import requests

from rest_fetcher import APIClient, PaginationEvent, PlaybackError, RateLimitExceeded, SchemaError
from rest_fetcher.pagination import cursor_pagination


def _json_response(body=None, status=200, headers=None):
    import json as _json

    resp = MagicMock()
    resp.status_code = status
    resp.headers = headers or {'Content-Type': 'application/json'}
    resp.content = _json.dumps(body or {}).encode('utf-8')
    resp.text = resp.content.decode('utf-8')
    resp.json.return_value = body or {}
    resp.url = 'https://api.example.com/any'
    return resp


FIXTURES = Path(__file__).resolve().parent.parent / 'examples' / 'fixtures'


class TestOnEventPlumbing(unittest.TestCase):
    def playback_config(self, fixture_name: str) -> dict[str, str]:
        return {'path': str(FIXTURES / fixture_name), 'mode': 'load'}

    def make_client(
        self, endpoint_name: str, endpoint_config: dict[str, Any], *, on_event=None
    ) -> APIClient:
        schema: dict[str, Any] = {
            'base_url': 'https://api.example.com/v1',
            'endpoints': {endpoint_name: endpoint_config},
        }
        if on_event is not None:
            schema['on_event'] = on_event
        return APIClient(schema)

    def test_on_event_exceptions_are_swallowed(self):
        def boom(_event: PaginationEvent) -> None:
            raise RuntimeError('nope')

        client = self.make_client(
            'list_events',
            {
                'method': 'GET',
                'path': '/events',
                'playback': self.playback_config('example_02_list_events.json'),
                'pagination': cursor_pagination(
                    cursor_param='cursor', next_cursor_path='meta.next_cursor', data_path='items'
                ),
            },
            on_event=boom,
        )
        pages = client.fetch('list_events')
        self.assertEqual(len(pages), 2)

    def test_on_event_source_is_playback(self):
        events: list[PaginationEvent] = []

        def capture(event: PaginationEvent) -> None:
            events.append(event)

        client = self.make_client(
            'list_events',
            {
                'method': 'GET',
                'path': '/events',
                'playback': self.playback_config('example_02_list_events.json'),
                'pagination': cursor_pagination(
                    cursor_param='cursor', next_cursor_path='meta.next_cursor', data_path='items'
                ),
            },
            on_event=capture,
        )
        client.fetch('list_events')
        self.assertTrue(events, 'expected at least one event')
        self.assertTrue(all(e.source == 'playback' for e in events))
        self.assertTrue(any(e.kind == 'request_start' for e in events))
        self.assertTrue(any(e.kind == 'request_end' for e in events))

    def test_request_end_event_includes_progress_fields(self) -> None:
        events: list[PaginationEvent] = []

        def capture(event: PaginationEvent) -> None:
            events.append(event)

        client = self.make_client(
            'list_events',
            {
                'method': 'GET',
                'path': '/events',
                'playback': self.playback_config('example_02_list_events.json'),
                'pagination': cursor_pagination(
                    cursor_param='cursor', next_cursor_path='meta.next_cursor', data_path='items'
                ),
            },
            on_event=capture,
        )
        client.fetch('list_events')
        request_end = [e for e in events if e.kind == 'request_end']
        self.assertTrue(request_end)
        self.assertTrue(all('requests_so_far' in (e.data or {}) for e in request_end))
        self.assertTrue(all('elapsed_seconds_so_far' in (e.data or {}) for e in request_end))


import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

from rest_fetcher.client import APIClient


def _write_json_playback_page(path: Path, body_text: str) -> None:
    """Write a single-envelope playback fixture."""
    envelope = {
        'kind': 'raw_response',
        'status_code': 200,
        'headers': {'Content-Type': 'application/json'},
        'format': 'json',
        'body': body_text,
        'url': 'https://example.invalid/t',
    }
    path.write_text(json.dumps([envelope], ensure_ascii=False, indent=2), encoding='utf-8')


class TestPlaybackEventSourceSemantics(unittest.TestCase):
    """Playback event source and event-kind emission tests.

    Formerly bare pytest functions using tmp_path; converted to TestCase
    so they run under both pytest and unittest discover.
    """

    def setUp(self) -> None:
        self._td = tempfile.mkdtemp()
        self.tmp_path = Path(self._td)

    def tearDown(self) -> None:
        shutil.rmtree(self._td, ignore_errors=True)

    def test_playback_request_events_have_playback_source(self) -> None:
        events: list[Any] = []

        def on_event(ev: Any) -> None:
            events.append(ev)

        fixture = self.tmp_path / 'fixture.json'
        _write_json_playback_page(fixture, '{"ok": true}')

        client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'on_event': on_event,
                'endpoints': {
                    't': {
                        'method': 'GET',
                        'path': '/t',
                        'response_format': 'json',
                        'playback': {'mode': 'load', 'path': str(fixture)},
                    }
                },
            }
        )
        client.fetch('t')

        kinds = [e.kind for e in events]
        self.assertIn('request_start', kinds)
        self.assertIn('request_end', kinds)
        self.assertTrue(all(e.source == 'playback' for e in events))

    def test_page_parsed_and_stopped_events_emitted(self) -> None:
        events: list[Any] = []

        def on_event(ev: Any) -> None:
            events.append(ev)

        fixture = self.tmp_path / 'fixture.json'
        _write_json_playback_page(fixture, '{"items": [1, 2]}')

        client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'on_event': on_event,
                'endpoints': {
                    't': {
                        'method': 'GET',
                        'path': '/t',
                        'response_format': 'json',
                        'pagination': {
                            'next_request': lambda parsed_body, state: None,
                        },
                        'playback': {'mode': 'load', 'path': str(fixture)},
                    }
                },
            }
        )
        client.fetch('t')
        self.assertTrue(any(e.kind == 'page_parsed' for e in events))
        self.assertTrue(any(e.kind == 'stopped' for e in events))

    def test_page_parsed_and_stopped_events_include_observability_fields(self) -> None:
        events: list[Any] = []

        def on_event(ev: Any) -> None:
            events.append(ev)

        fixture = self.tmp_path / 'fixture.json'
        _write_json_playback_page(fixture, '{"items": [1, 2]}')

        client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'on_event': on_event,
                'endpoints': {
                    't': {
                        'method': 'GET',
                        'path': '/t',
                        'response_format': 'json',
                        'pagination': {
                            'next_request': lambda parsed_body, state: None,
                        },
                        'playback': {'mode': 'load', 'path': str(fixture)},
                    }
                },
            }
        )
        client.fetch('t')
        page_ev = next(e for e in events if e.kind == 'page_parsed')
        stop_ev = next(e for e in events if e.kind == 'stopped')
        self.assertIn('pages_so_far', page_ev.data)
        self.assertIn('requests_so_far', page_ev.data)
        self.assertEqual(page_ev.data['pages_so_far'], 1)
        self.assertEqual(page_ev.data['requests_so_far'], 1)
        self.assertIn('stop', stop_ev.data)
        self.assertIn('pages_so_far', stop_ev.data)
        self.assertIn('requests_so_far', stop_ev.data)
        self.assertIn('elapsed_seconds_so_far', stop_ev.data)
        self.assertEqual(stop_ev.data['stop']['kind'], 'next_request_none')


import json
import unittest
from typing import Any

from rest_fetcher.client import APIClient
from rest_fetcher.exceptions import CallbackError


def _write_json_playback_sequence(path, statuses_and_bodies):
    envelopes = []
    for status, body in statuses_and_bodies:
        envelopes.append(
            {
                'kind': 'raw_response',
                'status_code': status,
                'headers': {'Content-Type': 'application/json'},
                'format': 'json',
                'body': body,
                'url': 'https://example.invalid/t',
            }
        )
    path.write_text(json.dumps(envelopes, ensure_ascii=False, indent=2), encoding='utf-8')


class TestEventErrorAndRetryReporting(unittest.TestCase):
    def test_callback_error_event_emitted(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        events: list[Any] = []

        def on_event(ev):
            events.append(ev)

        with TemporaryDirectory() as td:
            fixture = Path(td) / 'fixture.json'
            _write_json_playback_sequence(fixture, [(200, '{"items": [1]}')])

            def boom(page_payload, state):
                raise RuntimeError('nope')

            client = APIClient(
                {
                    'base_url': 'https://example.invalid',
                    'on_event': on_event,
                    'endpoints': {
                        't': {
                            'method': 'GET',
                            'path': '/t',
                            'response_format': 'json',
                            'pagination': {
                                'next_request': lambda parsed_body, state: None,
                            },
                            'update_state': boom,
                            'playback': {'mode': 'load', 'path': str(fixture)},
                        }
                    },
                }
            )

            with self.assertRaises(CallbackError):
                client.fetch('t')

            self.assertTrue(any(e.kind == 'callback_error' for e in events))

    def test_retry_event_emitted_in_playback(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        events: list[Any] = []

        def on_event(ev):
            events.append(ev)

        with TemporaryDirectory() as td:
            fixture = Path(td) / 'fixture.json'
            _write_json_playback_sequence(fixture, [(429, '{"err": 1}'), (200, '{"items": [1]}')])

            client = APIClient(
                {
                    'base_url': 'https://example.invalid',
                    'on_event': on_event,
                    'endpoints': {
                        't': {
                            'method': 'GET',
                            'path': '/t',
                            'response_format': 'json',
                            'pagination': {'next_request': lambda parsed_body, state: None},
                            # avoid real sleep in retry layer
                            'retry': {'base_delay': 0.001, 'max_delay': 0.001},
                            'playback': {'mode': 'load', 'path': str(fixture)},
                        }
                    },
                }
            )

            client.fetch('t')
            self.assertTrue(any(e.kind == 'retry' and e.source == 'playback' for e in events))

    def test_error_event_emitted_on_terminal_failure(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        events: list[Any] = []

        def on_event(ev):
            events.append(ev)

        with TemporaryDirectory() as td:
            fixture = Path(td) / 'fixture.json'
            fixture.write_text('[]', encoding='utf-8')

            client = APIClient(
                {
                    'base_url': 'https://example.invalid',
                    'on_event': on_event,
                    'endpoints': {
                        't': {
                            'method': 'GET',
                            'path': '/t',
                            'response_format': 'json',
                            'pagination': {'next_request': lambda parsed_body, state: None},
                            'playback': {'mode': 'load', 'path': str(fixture)},
                        }
                    },
                }
            )

            with self.assertRaises(PlaybackError):
                client.fetch('t')

            self.assertTrue(any(e.kind == 'error' and e.source == 'playback' for e in events))


import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from rest_fetcher.client import APIClient


def _write_json_playback_page_for_rate_limit(path: Path, body_text: str = '{"ok": true}') -> None:
    envelope = {
        'kind': 'raw_response',
        'status_code': 200,
        'headers': {'Content-Type': 'application/json'},
        'format': 'json',
        'body': body_text,
        'url': 'https://example.invalid/t',
    }
    path.write_text(json.dumps([envelope], ensure_ascii=False, indent=2), encoding='utf-8')


class TestRateLimitValidationAndPlayback(unittest.TestCase):
    def test_rate_limit_validation_rejects_non_positive_rps(self):
        with TemporaryDirectory() as td:
            fixture = Path(td) / 'fixture.json'
            _write_json_playback_page_for_rate_limit(fixture)

            with self.assertRaises(SchemaError):
                APIClient(
                    {
                        'base_url': 'https://example.invalid',
                        'rate_limit': {
                            'strategy': 'token_bucket',
                            'requests_per_second': 0,
                            'burst': 1,
                            'on_limit': 'wait',
                        },
                        'endpoints': {
                            't': {
                                'method': 'GET',
                                'path': '/t',
                                'response_format': 'json',
                                'playback': {'mode': 'load', 'path': str(fixture)},
                            }
                        },
                    }
                )

    def test_rate_limit_validation_rejects_non_positive_burst(self):
        with TemporaryDirectory() as td:
            fixture = Path(td) / 'fixture.json'
            _write_json_playback_page_for_rate_limit(fixture)

            with self.assertRaises(SchemaError):
                APIClient(
                    {
                        'base_url': 'https://example.invalid',
                        'rate_limit': {
                            'strategy': 'token_bucket',
                            'requests_per_second': 1.0,
                            'burst': 0,
                            'on_limit': 'wait',
                        },
                        'endpoints': {
                            't': {
                                'method': 'GET',
                                'path': '/t',
                                'response_format': 'json',
                                'playback': {'mode': 'load', 'path': str(fixture)},
                            }
                        },
                    }
                )

    def test_rate_limit_bypassed_in_playback(self):
        events = []

        def on_event(ev):
            events.append(ev)

        with TemporaryDirectory() as td:
            fixture = Path(td) / 'fixture.json'
            _write_json_playback_page_for_rate_limit(fixture)

            client = APIClient(
                {
                    'base_url': 'https://example.invalid',
                    'on_event': on_event,
                    'rate_limit': {
                        'strategy': 'token_bucket',
                        'requests_per_second': 0.0001,
                        'burst': 1,
                        'on_limit': 'raise',
                    },
                    'endpoints': {
                        't': {
                            'method': 'GET',
                            'path': '/t',
                            'response_format': 'json',
                            'playback': {'mode': 'load', 'path': str(fixture)},
                        }
                    },
                }
            )

            client.fetch('t')
            self.assertFalse(any(getattr(e, 'kind', '').startswith('rate_limit_') for e in events))


import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from rest_fetcher.client import APIClient
from rest_fetcher.exceptions import RateLimitError, RateLimitExceeded


class TestRateLimitEventReporting(unittest.TestCase):
    def test_rate_limit_wait_events_emitted_without_real_sleep(self):
        events: list[Any] = []
        now = {'t': 0.0}

        def clock():
            return now['t']

        def sleep(seconds: float):
            now['t'] += seconds

        def on_event(ev):
            events.append(ev)

        client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'on_event': on_event,
                'rate_limit': {
                    'strategy': 'token_bucket',
                    'requests_per_second': 1.0,
                    'burst': 1,
                    'on_limit': 'wait',
                    'clock': clock,
                    'sleep': sleep,
                },
                'endpoints': {
                    't': {
                        'method': 'GET',
                        'path': '/t',
                        'response_format': 'json',
                        'mock': [
                            {'status_code': 200, 'body': {'items': [1]}},
                            {'status_code': 200, 'body': {'items': [2]}},
                        ],
                        'pagination': {
                            'next_request': lambda parsed_body, state: None
                            if state.get('n', 0) >= 2
                            else {'url': 'https://example.invalid/t?page=2'},
                        },
                        'update_state': lambda page_payload, state: {'n': state.get('n', 0) + 1},
                    }
                },
            }
        )

        client.fetch('t')
        kinds = [e.kind for e in events if e.source == 'live']
        self.assertIn('rate_limit_wait_start', kinds)
        self.assertIn('rate_limit_wait_end', kinds)
        self.assertGreater(now['t'], 0.0)

    def test_rate_limit_wait_end_includes_wait_type_and_cumulative_fields(self):
        events: list[Any] = []
        now = {'t': 0.0}

        def clock():
            return now['t']

        def sleep(seconds: float):
            now['t'] += seconds

        def on_event(ev):
            events.append(ev)

        client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'on_event': on_event,
                'rate_limit': {
                    'strategy': 'token_bucket',
                    'requests_per_second': 1.0,
                    'burst': 1,
                    'on_limit': 'wait',
                    'clock': clock,
                    'sleep': sleep,
                },
                'endpoints': {
                    't': {
                        'method': 'GET',
                        'path': '/t',
                        'response_format': 'json',
                        'mock': [
                            {'status_code': 200, 'body': {'items': [1]}},
                            {'status_code': 200, 'body': {'items': [2]}},
                        ],
                        'pagination': {
                            'next_request': lambda parsed_body, state: None
                            if state.get('n', 0) >= 2
                            else {'url': 'https://example.invalid/t?page=2'},
                        },
                        'update_state': lambda page_payload, state: {'n': state.get('n', 0) + 1},
                    }
                },
            }
        )
        client.fetch('t')
        wait_ev = next(e for e in events if e.kind == 'rate_limit_wait_end')
        self.assertEqual(wait_ev.data['wait_type'], 'proactive')
        self.assertIn('proactive_waits_so_far', wait_ev.data)
        self.assertIn('proactive_wait_seconds_so_far', wait_ev.data)

    def test_retry_wait_end_event_includes_reactive_wait_fields(self):
        events: list[Any] = []
        resp1 = _json_response({'err': True}, status=429, headers={'Retry-After': '1'})
        resp1.ok = False
        resp2 = _json_response({'ok': True}, status=200)
        with (
            patch('requests.Session.request', side_effect=[resp1, resp2]),
            patch('time.sleep', return_value=None),
        ):
            client = APIClient(
                {
                    'base_url': 'https://example.invalid',
                    'on_event': lambda ev: events.append(ev),
                    'retry': {
                        'max_attempts': 2,
                        'backoff': 'linear',
                        'base_delay': 0.001,
                        'max_delay': 0.001,
                        'on_codes': [429],
                        'max_retry_after': 10.0,
                    },
                    'rate_limit': {'respect_retry_after': True},
                    'endpoints': {'t': {'method': 'GET', 'path': '/t', 'response_format': 'json'}},
                }
            )
            result = client.fetch('t')
        self.assertEqual(result, {'ok': True})
        reactive_wait = next(
            e
            for e in events
            if e.kind == 'rate_limit_wait_end' and e.data.get('wait_type') == 'reactive'
        )
        self.assertIn('reactive_waits_so_far', reactive_wait.data)
        self.assertIn('reactive_wait_seconds_so_far', reactive_wait.data)

    def test_rate_limit_exceeded_emits_event_and_raises(self):
        events: list[Any] = []
        now = {'t': 0.0}

        def clock():
            return now['t']

        def sleep(seconds: float):
            now['t'] += seconds

        def on_event(ev):
            events.append(ev)

        client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'on_event': on_event,
                'rate_limit': {
                    'strategy': 'token_bucket',
                    'requests_per_second': 0.0001,
                    'burst': 1,
                    'on_limit': 'raise',
                    'clock': clock,
                    'sleep': sleep,
                },
                'endpoints': {
                    't': {
                        'method': 'GET',
                        'path': '/t',
                        'response_format': 'json',
                        'mock': [
                            {'status_code': 200, 'body': {'items': [1]}},
                            {'status_code': 200, 'body': {'items': [2]}},
                        ],
                        'pagination': {
                            'next_request': lambda parsed_body, state: None
                            if state.get('n', 0) >= 2
                            else {'url': 'https://example.invalid/t?page=2'},
                        },
                        'update_state': lambda page_payload, state: {'n': state.get('n', 0) + 1},
                    }
                },
            }
        )

        with self.assertRaises(RateLimitExceeded):
            client.fetch('t')

        self.assertTrue(any(e.kind == 'rate_limit_exceeded' and e.source == 'live' for e in events))

    def test_non_rate_limit_request_error_still_routes_through_on_error(self):
        called = {'n': 0}

        def on_error(exc, state):
            called['n'] += 1
            return 'skip'

        with TemporaryDirectory() as td:
            fixture = Path(td) / 'fixture.json'
            envelope = {
                'kind': 'raw_response',
                'status_code': 404,
                'headers': {'Content-Type': 'application/json'},
                'format': 'json',
                'body': '{"err": true}',
                'url': 'https://example.invalid/t',
            }
            fixture.write_text(
                json.dumps([envelope], ensure_ascii=False, indent=2), encoding='utf-8'
            )

            client = APIClient(
                {
                    'base_url': 'https://example.invalid',
                    'endpoints': {
                        't': {
                            'method': 'GET',
                            'path': '/t',
                            'response_format': 'json',
                            'pagination': {'next_request': lambda parsed_body, state: None},
                            'retry': {'max_attempts': 1},
                            'on_error': on_error,
                            'playback': {'mode': 'load', 'path': str(fixture)},
                        }
                    },
                }
            )

            client.fetch('t')
            self.assertEqual(called['n'], 1)


class TestRateLimitOverrideSemantics(unittest.TestCase):
    def test_endpoint_rate_limit_replaces_client_retry_after_policy(self):
        sleeps: list[float] = []
        responses = [
            MagicMock(
                status_code=429,
                ok=False,
                headers={'Retry-After': '5'},
                url='https://example.invalid/t',
            ),
            MagicMock(status_code=200, ok=True, headers={}, url='https://example.invalid/t'),
        ]
        responses[0].json.return_value = {'err': True}
        responses[0].text = '{"err": true}'
        responses[1].json.return_value = {'ok': True}
        responses[1].text = '{"ok": true}'

        client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'retry': {
                    'max_attempts': 2,
                    'backoff': 'linear',
                    'base_delay': 7.0,
                    'max_delay': 7.0,
                    'max_retry_after': 30.0,
                },
                'rate_limit': {'respect_retry_after': True},
                'endpoints': {
                    't': {
                        'method': 'GET',
                        'path': '/t',
                        'response_format': 'json',
                        'rate_limit': {'respect_retry_after': False},
                    }
                },
            }
        )

        with (
            patch('requests.Session.request', side_effect=responses),
            patch('time.sleep', side_effect=lambda s: sleeps.append(s)),
        ):
            result = client.fetch('t')

        self.assertEqual(result, {'ok': True})
        self.assertEqual(sleeps, [7.0])

    def test_endpoint_rate_limit_none_opts_out_of_inherited_retry_after_policy(self):
        sleeps: list[float] = []
        responses = [
            MagicMock(
                status_code=429,
                ok=False,
                headers={'Retry-After': '5'},
                url='https://example.invalid/t',
            ),
            MagicMock(status_code=200, ok=True, headers={}, url='https://example.invalid/t'),
        ]
        responses[0].json.return_value = {'err': True}
        responses[0].text = '{"err": true}'
        responses[1].json.return_value = {'ok': True}
        responses[1].text = '{"ok": true}'

        client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'retry': {
                    'max_attempts': 2,
                    'backoff': 'linear',
                    'base_delay': 7.0,
                    'max_delay': 7.0,
                    'max_retry_after': 30.0,
                },
                'rate_limit': {'respect_retry_after': True},
                'endpoints': {
                    't': {
                        'method': 'GET',
                        'path': '/t',
                        'response_format': 'json',
                        'rate_limit': None,
                    }
                },
            }
        )

        with (
            patch('requests.Session.request', side_effect=responses),
            patch('time.sleep', side_effect=lambda s: sleeps.append(s)),
        ):
            result = client.fetch('t')

        self.assertEqual(result, {'ok': True})
        self.assertEqual(sleeps, [7.0])

    def test_endpoint_rate_limit_replacement_controls_max_retry_after(self):
        response = MagicMock(
            status_code=429, ok=False, headers={'Retry-After': '5'}, url='https://example.invalid/t'
        )
        response.json.return_value = {'err': True}
        response.text = '{"err": true}'

        client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'retry': {
                    'max_attempts': 2,
                    'backoff': 'linear',
                    'base_delay': 0.1,
                    'max_delay': 0.1,
                    'max_retry_after': 30.0,
                },
                'rate_limit': {'respect_retry_after': True},
                'endpoints': {
                    't': {
                        'method': 'GET',
                        'path': '/t',
                        'response_format': 'json',
                        'rate_limit': {'respect_retry_after': True},
                        'retry': {'max_retry_after': 1.0},
                    }
                },
            }
        )

        with patch('requests.Session.request', return_value=response), patch('time.sleep'):
            with self.assertRaises(RateLimitError):
                client.fetch('t')


def test_client_level_token_bucket_is_shared_across_jobs():
    class FakeClock:
        def __init__(self):
            self.now = 0.0

        def __call__(self):
            return self.now

    clock = FakeClock()
    schema = {
        'base_url': 'https://api.example.com',
        'rate_limit': {
            'strategy': 'token_bucket',
            'requests_per_second': 1.0,
            'burst': 1,
            'on_limit': 'raise',
            'clock': clock,
            'sleep': lambda _s: None,
        },
        'endpoints': {
            'users': {'method': 'GET', 'path': '/users'},
            'orders': {'method': 'GET', 'path': '/orders'},
        },
    }
    client = APIClient(schema)
    with patch.object(requests.Session, 'request', return_value=_json_response({'ok': True})):
        assert client.fetch('users') == {'ok': True}
        with pytest.raises(RateLimitExceeded):
            client.fetch('orders')


def test_call_level_token_bucket_override_is_not_shared_across_jobs():
    class FakeClock:
        def __init__(self):
            self.now = 0.0

        def __call__(self):
            return self.now

    clock = FakeClock()
    schema = {
        'base_url': 'https://api.example.com',
        'endpoints': {
            'users': {'method': 'GET', 'path': '/users'},
            'orders': {'method': 'GET', 'path': '/orders'},
        },
    }
    client = APIClient(schema)
    override = {
        'strategy': 'token_bucket',
        'requests_per_second': 1.0,
        'burst': 1,
        'on_limit': 'raise',
        'clock': clock,
        'sleep': lambda _s: None,
    }
    with patch.object(requests.Session, 'request', return_value=_json_response({'ok': True})):
        assert client.fetch('users', rate_limit=override) == {'ok': True}
        assert client.fetch('orders', rate_limit=override) == {'ok': True}


class TestRetryEventPayloads(unittest.TestCase):
    def test_status_code_retry_reports_planned_ms_from_retry_after(self):
        events: list[PaginationEvent] = []

        def on_event(ev):
            events.append(ev)

        responses = [
            _json_response({'err': True}, status=429, headers={'Retry-After': '5'}),
            _json_response({'ok': True}, status=200),
        ]
        client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'on_event': on_event,
                'retry': {
                    'max_attempts': 2,
                    'backoff': 'linear',
                    'base_delay': 1.0,
                    'max_delay': 1.0,
                },
                'rate_limit': {'respect_retry_after': True},
                'endpoints': {'t': {'method': 'GET', 'path': '/t', 'response_format': 'json'}},
            }
        )
        with patch('requests.Session.request', side_effect=responses), patch('time.sleep'):
            self.assertEqual(client.fetch('t'), {'ok': True})
        retry_events = [e for e in events if e.kind == 'retry']
        self.assertEqual(len(retry_events), 1)
        self.assertEqual(retry_events[0].data['planned_ms'], 5000.0)

    def test_retry_event_planned_ms_is_consistent_across_status_and_network_paths(self):
        status_events: list[PaginationEvent] = []
        network_events: list[PaginationEvent] = []

        status_client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'on_event': status_events.append,
                'retry': {
                    'max_attempts': 2,
                    'backoff': 'linear',
                    'base_delay': 2.0,
                    'max_delay': 2.0,
                },
                'rate_limit': {'respect_retry_after': False},
                'endpoints': {'t': {'method': 'GET', 'path': '/t', 'response_format': 'json'}},
            }
        )
        network_client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'on_event': network_events.append,
                'retry': {
                    'max_attempts': 2,
                    'backoff': 'linear',
                    'base_delay': 2.0,
                    'max_delay': 2.0,
                },
                'endpoints': {'t': {'method': 'GET', 'path': '/t', 'response_format': 'json'}},
            }
        )

        with (
            patch(
                'requests.Session.request',
                side_effect=[
                    _json_response({'err': True}, status=500),
                    _json_response({'ok': True}, status=200),
                ],
            ),
            patch('time.sleep'),
        ):
            self.assertEqual(status_client.fetch('t'), {'ok': True})
        with (
            patch(
                'requests.Session.request',
                side_effect=[
                    requests.ConnectionError('down'),
                    _json_response({'ok': True}, status=200),
                ],
            ),
            patch('time.sleep'),
        ):
            self.assertEqual(network_client.fetch('t'), {'ok': True})

        status_retry = [e for e in status_events if e.kind == 'retry'][0]
        network_retry = [e for e in network_events if e.kind == 'retry'][0]
        self.assertEqual(status_retry.data['planned_ms'], 2000.0)
        self.assertEqual(network_retry.data['planned_ms'], 2000.0)


class TestG3WaitObservability(unittest.TestCase):
    def test_reactive_wait_end_includes_planned_ms_and_cause(self):
        events = []
        resp1 = _json_response({'err': True}, status=429, headers={'Retry-After': '1'})
        resp1.ok = False
        resp2 = _json_response({'ok': True}, status=200)
        with (
            patch('requests.Session.request', side_effect=[resp1, resp2]),
            patch('time.sleep', return_value=None),
        ):
            client = APIClient(
                {
                    'base_url': 'https://example.invalid',
                    'on_event': lambda ev: events.append(ev),
                    'retry': {
                        'max_attempts': 2,
                        'backoff': 'linear',
                        'base_delay': 0.001,
                        'max_delay': 0.001,
                        'on_codes': [429],
                    },
                    'rate_limit': {'respect_retry_after': True},
                    'endpoints': {'t': {'method': 'GET', 'path': '/t', 'response_format': 'json'}},
                }
            )
            client.fetch('t')
        retry_event = next(e for e in events if e.kind == 'retry')
        wait_end = next(
            e
            for e in events
            if e.kind == 'rate_limit_wait_end' and e.data.get('wait_type') == 'reactive'
        )
        # Reactive waits do not emit a rate_limit_wait_start event; compare against the retry
        # event's planned delay to verify the same planned value is propagated through the
        # wait-end event.
        self.assertEqual(wait_end.data['planned_ms'], retry_event.data['planned_ms'])
        self.assertEqual(wait_end.data['cause'], 'retry_after')

    def test_proactive_wait_end_planned_ms_matches_start(self):
        events = []

        class FakeBucket:
            def acquire(self, _tokens):
                return 0.25

            def sleep(self, _seconds):
                return None

        with (
            patch('rest_fetcher.client.TokenBucket', return_value=FakeBucket()),
            patch('time.monotonic', side_effect=[1.0, 2.0, 2.4, 3.0]),
        ):
            client = APIClient(
                {
                    'base_url': 'https://example.invalid',
                    'on_event': lambda ev: events.append(ev),
                    'rate_limit': {
                        'strategy': 'token_bucket',
                        'requests_per_second': 1.0,
                        'burst': 1,
                    },
                    'endpoints': {'t': {'method': 'GET', 'path': '/t', 'response_format': 'json'}},
                }
            )
            job = client._make_job('t', {})
            job._apply_rate_limit('https://example.invalid/t')

        wait_start = next(e for e in events if e.kind == 'rate_limit_wait_start')
        wait_end = next(e for e in events if e.kind == 'rate_limit_wait_end')
        self.assertEqual(wait_start.data['planned_ms'], 250.0)
        self.assertEqual(wait_end.data['planned_ms'], wait_start.data['planned_ms'])
        self.assertEqual(wait_end.data['wait_type'], 'proactive')
        self.assertAlmostEqual(wait_end.data['wait_ms'], 400.0)

    def test_terminal_retry_none_respects_max_retry_after(self):
        events = []
        response = _json_response({'err': True}, status=429, headers={'Retry-After': '5'})
        response.ok = False
        with (
            patch('requests.Session.request', return_value=response),
            patch('time.sleep', return_value=None),
        ):
            client = APIClient(
                {
                    'base_url': 'https://example.invalid',
                    'on_event': lambda ev: events.append(ev),
                    'retry': {'max_attempts': 2, 'max_retry_after': 1.0},
                    'rate_limit': {'respect_retry_after': True},
                    'endpoints': {
                        't': {
                            'method': 'GET',
                            'path': '/t',
                            'response_format': 'json',
                            'retry': None,
                        }
                    },
                }
            )
            with self.assertRaises(RateLimitError):
                client.fetch('t')
        self.assertFalse(
            any(
                e.kind == 'rate_limit_wait_end' and e.data.get('wait_type') == 'reactive'
                for e in events
            )
        )

    def test_stream_summary_splits_retry_and_rate_limit_waits(self):
        seen = {}

        def on_complete(summary, state):
            seen['summary'] = summary

        good = _json_response({'ok': True}, status=200)
        with (
            patch('requests.Session.request', side_effect=[requests.ConnectionError('boom'), good]),
            patch('time.sleep', return_value=None),
        ):
            client = APIClient(
                {
                    'base_url': 'https://example.invalid',
                    'retry': {
                        'max_attempts': 2,
                        'backoff': 'linear',
                        'base_delay': 0.25,
                        'max_delay': 0.25,
                    },
                    'endpoints': {
                        't': {
                            'method': 'GET',
                            'path': '/t',
                            'response_format': 'json',
                            'pagination': {'next_request': lambda parsed_body, state: None},
                            'on_complete': on_complete,
                        }
                    },
                }
            )
            list(client.stream('t'))
        summary = seen['summary']
        self.assertGreater(summary.retry_wait_seconds, 0.0)
        self.assertEqual(summary.rate_limit_wait_seconds, 0.0)


class TestBytesReceivedTelemetry(unittest.TestCase):
    """bytes_received: per-response body size in request_end events, run-local state, and StreamSummary."""

    def _simple_schema(self, mock_responses, **endpoint_extras):
        return {
            'base_url': 'https://api.example.com/v1',
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/items',
                    'response_format': 'json',
                    'mock': mock_responses,
                    **endpoint_extras,
                }
            },
        }

    def test_success_response_contributes_bytes_to_request_end(self):
        events = []
        with patch('requests.Session.request') as mock_req:
            resp = _json_response({'items': [1, 2, 3]})
            resp.ok = True
            mock_req.return_value = resp
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'on_event': lambda e: events.append(e),
                    'endpoints': {'ep': {'method': 'GET', 'path': '/items'}},
                }
            )
            client.fetch('ep')
        req_ends = [e for e in events if e.kind == 'request_end']
        self.assertEqual(len(req_ends), 1)
        self.assertGreater(req_ends[0].data['bytes_received'], 0)

    def test_success_response_bytes_match_content_length(self):
        import json

        body = {'items': [1, 2, 3], 'meta': 'test'}
        expected_bytes = len(json.dumps(body).encode('utf-8'))
        events = []
        with patch('requests.Session.request') as mock_req:
            resp = _json_response(body)
            resp.ok = True
            mock_req.return_value = resp
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'on_event': lambda e: events.append(e),
                    'endpoints': {'ep': {'method': 'GET', 'path': '/items'}},
                }
            )
            client.fetch('ep')
        req_ends = [e for e in events if e.kind == 'request_end']
        self.assertEqual(req_ends[0].data['bytes_received'], expected_bytes)

    def test_summary_bytes_received_accumulates_across_pages(self):
        import json

        pages = [
            {'items': [1, 2], 'next': True},
            {'items': [3], 'next': False},
        ]
        expected_total = sum(len(json.dumps(p).encode('utf-8')) for p in pages)
        seen = {}

        def on_complete(summary, state):
            seen['summary'] = summary

        with patch('requests.Session.request') as mock_req:
            responses = [_json_response(p) for p in pages]
            for r in responses:
                r.ok = True
            mock_req.side_effect = responses
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {
                        'ep': {
                            'method': 'GET',
                            'path': '/items',
                            'pagination': {
                                'next_request': lambda resp, state: {'params': {'p': 2}}
                                if resp.get('next')
                                else None,
                            },
                            'on_response': lambda resp, state: resp.get('items', []),
                            'on_complete': on_complete,
                        }
                    },
                }
            )
            list(client.stream('ep'))

        self.assertEqual(seen['summary'].bytes_received, expected_total)

    def test_bytes_received_so_far_visible_in_callback_state(self):
        seen_bytes = []

        def on_page(page, state):
            seen_bytes.append(state.get('bytes_received_so_far', 0))

        with patch('requests.Session.request') as mock_req:
            r = _json_response({'items': [1]})
            r.ok = True
            mock_req.return_value = r
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {
                        'ep': {
                            'method': 'GET',
                            'path': '/items',
                            'on_page': on_page,
                        }
                    },
                }
            )
            client.fetch('ep')

        self.assertEqual(len(seen_bytes), 1)
        self.assertGreater(seen_bytes[0], 0)

    def test_error_response_with_body_contributes_bytes(self):
        events = []
        with patch('requests.Session.request') as mock_req:
            err_resp = _json_response({'error': 'forbidden'}, status=403)
            err_resp.ok = False
            err_resp.raise_for_status = MagicMock()
            mock_req.return_value = err_resp
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'on_event': lambda e: events.append(e),
                    'on_error': lambda exc, state: 'skip',
                    'endpoints': {'ep': {'method': 'GET', 'path': '/items'}},
                }
            )
            client.fetch('ep')

        req_ends = [e for e in events if e.kind == 'request_end']
        self.assertEqual(len(req_ends), 1)
        # non-retryable error: response returned normally, bytes measured before _process_response
        self.assertGreater(req_ends[0].data['bytes_received'], 0)
        self.assertEqual(req_ends[0].data['status_code'], 403)

    def test_retryable_exhaustion_reports_bytes_and_status(self):
        events = []
        with patch('requests.Session.request') as mock_req:
            err_resp = _json_response({'error': 'overloaded'}, status=500)
            err_resp.ok = False
            mock_req.return_value = err_resp
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retry': {
                        'max_attempts': 2,
                        'backoff': 'linear',
                        'base_delay': 0.001,
                        'max_delay': 0.001,
                        'on_codes': [500],
                    },
                    'on_event': lambda e: events.append(e),
                    'on_error': lambda exc, state: 'skip',
                    'endpoints': {'ep': {'method': 'GET', 'path': '/items'}},
                }
            )
            with patch('time.sleep', return_value=None):
                client.fetch('ep')

        req_ends = [e for e in events if e.kind == 'request_end']
        self.assertEqual(len(req_ends), 1)
        self.assertGreater(req_ends[0].data['bytes_received'], 0)
        self.assertEqual(req_ends[0].data['status_code'], 500)

    def test_playback_contributes_bytes(self):
        import json
        import tempfile
        import os

        body = {'items': [1, 2]}
        fixture = [
            {
                'kind': 'raw_response',
                'format': 'json',
                'status_code': 200,
                'headers': {'Content-Type': 'application/json'},
                'url': 'https://api.example.com/v1/items',
                'body': json.dumps(body),
            }
        ]
        tmpdir = tempfile.mkdtemp()
        path = os.path.join(tmpdir, 'fixture.json')
        with open(path, 'w') as f:
            json.dump(fixture, f)

        seen = {}

        def on_complete(summary, state):
            seen['summary'] = summary

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/items',
                        'playback': {'path': path, 'mode': 'load'},
                        'on_complete': on_complete,
                    }
                },
            }
        )
        list(client.stream('ep'))

        self.assertGreater(seen['summary'].bytes_received, 0)
        # cleanup
        os.unlink(path)
        os.rmdir(tmpdir)


class TestRetryBytesReceivedTelemetry(unittest.TestCase):
    """retry_bytes_received: bytes from retry attempts only."""

    def test_no_retry_summary_and_event_are_zero(self):
        events = []
        seen = {}

        def on_complete(summary, state):
            seen['summary'] = summary

        with patch('requests.Session.request') as mock_req:
            resp = _json_response({'items': [1]})
            resp.ok = True
            mock_req.return_value = resp
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'on_event': lambda e: events.append(e),
                    'endpoints': {
                        'ep': {'method': 'GET', 'path': '/items', 'on_complete': on_complete}
                    },
                }
            )
            list(client.stream('ep'))

        req_end = next(e for e in events if e.kind == 'request_end')
        self.assertEqual(req_end.data['retry_bytes_received'], 0)
        self.assertEqual(seen['summary'].retry_bytes_received, 0)

    def test_retry_attempt_bytes_only_count_after_first_attempt(self):
        import json

        events = []
        seen = {}
        bodies = [
            {'error': 'try again'},
            {'items': [1, 2, 3]},
        ]
        expected_retry_bytes = len(json.dumps(bodies[1]).encode('utf-8'))

        def on_complete(summary, state):
            seen['summary'] = summary

        with patch('requests.Session.request') as mock_req:
            err_resp = _json_response(bodies[0], status=500)
            err_resp.ok = False
            ok_resp = _json_response(bodies[1], status=200)
            ok_resp.ok = True
            mock_req.side_effect = [err_resp, ok_resp]
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retry': {
                        'max_attempts': 2,
                        'backoff': 'linear',
                        'base_delay': 0.001,
                        'max_delay': 0.001,
                        'on_codes': [500],
                    },
                    'on_event': lambda e: events.append(e),
                    'endpoints': {
                        'ep': {'method': 'GET', 'path': '/items', 'on_complete': on_complete}
                    },
                }
            )
            with patch('time.sleep', return_value=None):
                list(client.stream('ep'))

        req_ends = [e for e in events if e.kind == 'request_end']
        self.assertEqual(len(req_ends), 1)
        self.assertEqual(req_ends[0].data['retry_bytes_received'], expected_retry_bytes)
        self.assertEqual(seen['summary'].retry_bytes_received, expected_retry_bytes)

    def test_retry_exhaustion_sums_retry_attempt_bytes_including_final_error(self):
        import json

        events = []
        bodies = [
            {'error': 'first'},
            {'error': 'second'},
            {'error': 'third'},
        ]
        expected_retry_bytes = sum(len(json.dumps(body).encode('utf-8')) for body in bodies[1:])

        with patch('requests.Session.request') as mock_req:
            responses = [_json_response(body, status=500) for body in bodies]
            for resp in responses:
                resp.ok = False
            mock_req.side_effect = responses
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retry': {
                        'max_attempts': 3,
                        'backoff': 'linear',
                        'base_delay': 0.001,
                        'max_delay': 0.001,
                        'on_codes': [500],
                    },
                    'on_event': lambda e: events.append(e),
                    'on_error': lambda exc, state: 'skip',
                    'endpoints': {'ep': {'method': 'GET', 'path': '/items'}},
                }
            )
            with patch('time.sleep', return_value=None):
                client.fetch('ep')

        req_end = next(e for e in events if e.kind == 'request_end')
        self.assertEqual(req_end.data['retry_bytes_received'], expected_retry_bytes)

    def test_retry_error_response_body_on_retry_attempt_counts(self):
        import json

        events = []
        bodies = [
            {'error': 'first'},
            {'error': 'second'},
        ]
        expected_retry_bytes = len(json.dumps(bodies[1]).encode('utf-8'))

        with patch('requests.Session.request') as mock_req:
            responses = [_json_response(body, status=500) for body in bodies]
            for resp in responses:
                resp.ok = False
            mock_req.side_effect = responses
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retry': {
                        'max_attempts': 2,
                        'backoff': 'linear',
                        'base_delay': 0.001,
                        'max_delay': 0.001,
                        'on_codes': [500],
                    },
                    'on_event': lambda e: events.append(e),
                    'on_error': lambda exc, state: 'skip',
                    'endpoints': {'ep': {'method': 'GET', 'path': '/items'}},
                }
            )
            with patch('time.sleep', return_value=None):
                client.fetch('ep')

        req_end = next(e for e in events if e.kind == 'request_end')
        self.assertEqual(req_end.data['retry_bytes_received'], expected_retry_bytes)

    def test_request_end_retry_bytes_received_is_per_cycle_not_per_run(self):
        import json

        events = []
        next_calls = {'count': 0}

        def next_request(resp, state):
            next_calls['count'] += 1
            if next_calls['count'] == 1:
                return {'params': {'page': 2}}
            return None

        first_retry_body = {'items': [1]}
        expected_first_cycle_retry_bytes = len(json.dumps(first_retry_body).encode('utf-8'))

        with patch('requests.Session.request') as mock_req:
            first_fail = _json_response({'error': 'retry me'}, status=500)
            first_fail.ok = False
            first_ok = _json_response(first_retry_body, status=200)
            first_ok.ok = True
            second_ok = _json_response({'items': [2]}, status=200)
            second_ok.ok = True
            mock_req.side_effect = [first_fail, first_ok, second_ok]
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retry': {
                        'max_attempts': 2,
                        'backoff': 'linear',
                        'base_delay': 0.001,
                        'max_delay': 0.001,
                        'on_codes': [500],
                    },
                    'on_event': lambda e: events.append(e),
                    'endpoints': {
                        'ep': {
                            'method': 'GET',
                            'path': '/items',
                            'pagination': {'next_request': next_request},
                        }
                    },
                }
            )
            with patch('time.sleep', return_value=None):
                client.fetch('ep')

        req_ends = [e for e in events if e.kind == 'request_end']
        self.assertEqual(len(req_ends), 2)
        self.assertEqual(req_ends[0].data['retry_bytes_received'], expected_first_cycle_retry_bytes)
        self.assertEqual(req_ends[1].data['retry_bytes_received'], 0)

    def test_retry_bytes_visible_in_callback_state(self):
        seen_retry_bytes = []

        def on_page(page, state):
            seen_retry_bytes.append(state.get('retry_bytes_received_so_far', 0))

        with patch('requests.Session.request') as mock_req:
            first = _json_response({'error': 'try again'}, status=500)
            first.ok = False
            second = _json_response({'items': [1]}, status=200)
            second.ok = True
            mock_req.side_effect = [first, second]
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retry': {
                        'max_attempts': 2,
                        'backoff': 'linear',
                        'base_delay': 0.001,
                        'max_delay': 0.001,
                        'on_codes': [500],
                    },
                    'endpoints': {'ep': {'method': 'GET', 'path': '/items', 'on_page': on_page}},
                }
            )
            with patch('time.sleep', return_value=None):
                client.fetch('ep')

        self.assertEqual(len(seen_retry_bytes), 1)
        self.assertGreater(seen_retry_bytes[0], 0)
