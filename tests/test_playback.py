# Domain: playback record/load/scrub behavior.
# Place new tests here by primary behavioral contract when the assertion is
# PlaybackHandler save/load/auto-detect, raw_response fixture serialization,
# fixture round-trip fidelity, URL/header scrubbing, playback mode semantics,
# or fixture format validation (FixtureFormatError / PlaybackError).
# Prefer this existing domain file for that work. If a test spans multiple
# areas, place it where the main assertion belongs.

from __future__ import annotations

import base64
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from rest_fetcher import (
    APIClient,
    FixtureFormatError,
    PlaybackError,
    cursor_pagination,
    offset_pagination,
)
from rest_fetcher.parsing import serialize_response_for_playback as _serialize_playback
from rest_fetcher.playback import PlaybackHandler
from rest_fetcher.types import StreamSummary


def make_response(status=200, body=None, headers=None):
    r = MagicMock()
    r.status_code = status
    r.ok = (status < 400)
    r.url = 'https://api.example.com/v1/test'
    r.headers = headers or {}
    r.json.return_value = body or {}
    r.text = json.dumps(body or {})
    return r


def simple_schema(**endpoint_overrides):
    endpoint = {'method': 'GET', 'path': '/test'}
    endpoint.update(endpoint_overrides)
    return {
        'base_url': 'https://api.example.com/v1',
        'endpoints': {'test': endpoint}
    }


class TestPlayback(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def _path(self, name):
        return os.path.join(self.tmpdir, name)

    def test_auto_mode_saves_when_no_file(self):
        path = self._path('test.json')
        handler = PlaybackHandler({'path': path})
        self.assertFalse(handler.should_load)
        handler.save([{'page': 1}, {'page': 2}])
        self.assertTrue(os.path.exists(path))

    def test_auto_mode_loads_when_file_exists(self):
        path = self._path('test.json')
        with open(path, 'w') as f:
            json.dump([{'page': 1}], f)
        handler = PlaybackHandler({'path': path})
        self.assertTrue(handler.should_load)
        pages = handler.load()
        self.assertEqual(pages, [{'page': 1}])

    def test_load_mode_raises_when_missing(self):
        handler = PlaybackHandler({'path': self._path('missing.json'), 'mode': 'load'})
        with self.assertRaises(PlaybackError):
            handler.load()

    def test_save_mode_always_fetches(self):
        path = self._path('test.json')
        with open(path, 'w') as f:
            json.dump([{'old': True}], f)
        handler = PlaybackHandler({'path': path, 'mode': 'save'})
        self.assertFalse(handler.should_load)  # save mode never loads

    def test_single_dict_normalized_to_list(self):
        path = self._path('single.json')
        with open(path, 'w') as f:
            json.dump({'single': 'page'}, f)
        handler = PlaybackHandler({'path': path})
        pages = handler.load()
        self.assertEqual(pages, [{'single': 'page'}])


    def test_invalid_mode_raises_playback_error(self):
        from rest_fetcher.playback import PlaybackError, _resolve_config
        with self.assertRaises(PlaybackError):
            _resolve_config({'path': 'x.json', 'mode': 'rewind'})

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

class TestRecordedResponse(unittest.TestCase):
    'unit tests for RecordedResponse wire-replay object'

    def _make(self, status_code=200, headers=None, body='', url='', format='json', content_b64=None, encoding=None):
        from requests.structures import CaseInsensitiveDict

        from rest_fetcher.playback import RecordedResponse
        return RecordedResponse(
            status_code=status_code,
            headers=CaseInsensitiveDict(headers or {'Content-Type': 'application/json'}),
            format=format,
            body=body,
            url=url,
            content_b64=content_b64,
            encoding=encoding,
        )

    def test_ok_true_for_2xx(self):
        self.assertTrue(self._make(200).ok)
        self.assertTrue(self._make(201).ok)
        self.assertTrue(self._make(299).ok)

    def test_ok_false_for_4xx(self):
        self.assertFalse(self._make(400).ok)
        self.assertFalse(self._make(404).ok)

    def test_ok_false_for_5xx(self):
        self.assertFalse(self._make(500).ok)
        self.assertFalse(self._make(503).ok)

    def test_text_returns_body(self):
        self.assertEqual(self._make(body='hello').text, 'hello')

    def test_content_returns_utf8_encoded_body(self):
        self.assertEqual(self._make(body='hello').content, b'hello')

    def test_json_parses_body(self):
        self.assertEqual(self._make(body='{"a": 1}').json(), {'a': 1})

    def test_headers_are_case_insensitive(self):
        from requests.structures import CaseInsensitiveDict

        from rest_fetcher.playback import RecordedResponse
        rec = RecordedResponse(200, CaseInsensitiveDict({'content-type': 'application/json'}), 'json', '', '{}')
        self.assertEqual(rec.headers.get('Content-Type'), 'application/json')
        self.assertEqual(rec.headers.get('content-type'), 'application/json')


class TestDeserializePlaybackResponse(unittest.TestCase):

    def _envelope(self, **overrides):
        base = {
            'kind': 'raw_response',
            'format': 'json',
            'status_code': 200,
            'headers': {'Content-Type': 'application/json'},
            'url': 'https://example.com/x',
            'body': '{"data": 1}',
        }
        base.update(overrides)
        return base

    def test_basic_deserialization(self):
        from rest_fetcher.playback import deserialize_playback_response
        rec = deserialize_playback_response(self._envelope())
        self.assertEqual(rec.status_code, 200)
        self.assertEqual(rec.body, '{"data": 1}')
        self.assertEqual(rec.url, 'https://example.com/x')

    def test_missing_kind_raises(self):
        from rest_fetcher.exceptions import PlaybackError
        from rest_fetcher.playback import deserialize_playback_response
        with self.assertRaises(PlaybackError):
            deserialize_playback_response({'body': '{}', 'headers': {}})

    def test_wrong_kind_raises(self):
        from rest_fetcher.exceptions import PlaybackError
        from rest_fetcher.playback import deserialize_playback_response
        with self.assertRaises(PlaybackError):
            deserialize_playback_response({'kind': 'something_else', 'body': '{}', 'headers': {}})

    def test_non_dict_raises(self):
        from rest_fetcher.exceptions import PlaybackError
        from rest_fetcher.playback import deserialize_playback_response
        with self.assertRaises(PlaybackError):
            deserialize_playback_response([1, 2, 3])

    def test_missing_body_raises(self):
        from rest_fetcher.exceptions import PlaybackError
        from rest_fetcher.playback import deserialize_playback_response
        with self.assertRaises(PlaybackError):
            deserialize_playback_response({'kind': 'raw_response', 'format': 'json', 'headers': {}})

    def test_non_string_body_raises(self):
        from rest_fetcher.exceptions import PlaybackError
        from rest_fetcher.playback import deserialize_playback_response
        with self.assertRaises(PlaybackError):
            deserialize_playback_response(self._envelope(body={'not': 'a string'}))

    def test_headers_become_case_insensitive(self):
        from rest_fetcher.playback import deserialize_playback_response
        rec = deserialize_playback_response(self._envelope(headers={'content-type': 'text/plain'}))
        self.assertEqual(rec.headers.get('Content-Type'), 'text/plain')

    def test_none_body_normalised_to_empty_string(self):
        from rest_fetcher.playback import deserialize_playback_response
        rec = deserialize_playback_response(self._envelope(body=None))
        self.assertEqual(rec.body, '')

    def test_default_status_code_is_200(self):
        from rest_fetcher.playback import deserialize_playback_response
        env = {'kind': 'raw_response', 'format': 'json', 'body': '{}', 'headers': {}}
        rec = deserialize_playback_response(env)
        self.assertEqual(rec.status_code, 200)


class TestRawPlaybackRoundTrip(unittest.TestCase):
    'integration tests for raw_response fixture save/load with callback replay'

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _path(self, name):
        return os.path.join(self.tmpdir, name)

    def test_single_page_save_and_load_json(self):
        path = self._path('fixture.json')
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'result': 42}, headers={'Content-Type': 'application/json'})
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'playback': {'path': path, 'mode': 'save'}}}
            })
            saved = client.fetch('ep')

        client2 = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'playback': {'path': path, 'mode': 'load'}}}
        })
        loaded = client2.fetch('ep')
        self.assertEqual(saved, loaded)

    def test_on_response_runs_during_load(self):
        'raw_response load re-runs on_response — validates callback debugging purpose'
        path = self._path('fixture.json')
        transform = lambda resp, state: {k: v * 2 for k, v in resp.items()}

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'x': 1}, headers={'Content-Type': 'application/json'})
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                    'playback': {'path': path, 'mode': 'save'},
                    'on_response': transform}}
            })
            saved = client.fetch('ep')
        self.assertEqual(saved, {'x': 2})

        # Load: same on_response should re-run from raw wire data
        client2 = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                'playback': {'path': path, 'mode': 'load'},
                'on_response': transform}}
        })
        loaded = client2.fetch('ep')
        self.assertEqual(loaded, {'x': 2})

    def test_on_response_can_be_changed_on_load_for_debugging(self):
        'can swap on_response on load to debug a different transformation'
        path = self._path('fixture.json')

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'x': 10, 'y': 20}, headers={'Content-Type': 'application/json'})
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                    'playback': {'path': path, 'mode': 'save'},
                    'on_response': lambda resp, state: resp.get('x')}}
            })
            client.fetch('ep')

        # Load with a DIFFERENT on_response — the raw wire data lets us re-process
        client2 = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                'playback': {'path': path, 'mode': 'load'},
                'on_response': lambda resp, state: resp.get('y')}}
        })
        result = client2.fetch('ep')
        self.assertEqual(result, 20)

    def test_response_parser_runs_during_load(self):
        'custom response_parser also re-runs from raw wire data'
        path = self._path('fixture.json')
        parser = lambda resp: {'parsed_len': len(resp.text)}

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'a': 1}, headers={'Content-Type': 'application/json'})
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                    'playback': {'path': path, 'mode': 'save'},
                    'response_parser': parser}}
            })
            saved = client.fetch('ep')

        client2 = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                'playback': {'path': path, 'mode': 'load'},
                'response_parser': parser}}
        })
        loaded = client2.fetch('ep')
        self.assertEqual(saved, loaded)
        self.assertIn('parsed_len', loaded)



    def test_non_paginated_live_endpoint_runs_on_page_and_on_complete(self):
        calls = []

        def on_response(resp, state):
            calls.append(('on_response', resp['x']))
            return {'x': resp['x']}

        def on_page(page, state):
            calls.append(('on_page', page['x']))

        def on_complete(pages, state):
            calls.append(('on_complete', len(pages)))
            return {'pages': pages, 'count': len(pages)}

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'x': 7}, headers={'Content-Type': 'application/json'})
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                    'on_response': on_response,
                    'on_page': on_page,
                    'on_complete': on_complete}}
            })
            result = client.fetch('ep')

        self.assertEqual(result, {'pages': [{'x': 7}], 'count': 1})
        self.assertEqual(calls, [('on_response', 7), ('on_page', 7), ('on_complete', 1)])

    def test_non_paginated_raw_playback_runs_on_page_and_on_complete(self):
        path = self._path('fixture.json')
        calls = []

        def on_response(resp, state):
            calls.append(('on_response', resp['x']))
            return {'x': resp['x']}

        def on_page(page, state):
            calls.append(('on_page', page['x']))

        def on_complete(pages, state):
            calls.append(('on_complete', len(pages)))
            return {'pages': pages, 'count': len(pages)}

        payload = [{
            'kind': 'raw_response',
            'format': 'json',
            'status_code': 200,
            'headers': {'Content-Type': 'application/json'},
            'url': 'https://api.example.com/x',
            'body': '{"x": 7}',
        }]
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f)

        client = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                'playback': {'path': path, 'mode': 'load'},
                'on_response': on_response,
                'on_page': on_page,
                'on_complete': on_complete}}
        })
        result = client.fetch('ep')

        self.assertEqual(result, {'pages': [{'x': 7}], 'count': 1})
        self.assertEqual(calls, [('on_response', 7), ('on_page', 7), ('on_complete', 1)])

        calls.clear()
        client2 = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                'playback': {'path': path, 'mode': 'load'},
                'on_response': on_response,
                'on_page': on_page,
                'on_complete': on_complete}}
        })
        loaded = client2.fetch('ep')
        self.assertEqual(loaded, {'pages': [{'x': 7}], 'count': 1})
        self.assertEqual(calls, [('on_response', 7), ('on_page', 7), ('on_complete', 1)])

    def test_playback_stream_run_summary_requests_counts_attempts(self):
        path = self._path('fixture_stream.json')
        payload = [
            {
                'kind': 'raw_response',
                'format': 'json',
                'status_code': 200,
                'headers': {'Content-Type': 'application/json'},
                'url': 'https://api.example.com/items',
                'body': '{"items": [1], "next_cursor": "n1"}',
            },
            {
                'kind': 'raw_response',
                'format': 'json',
                'status_code': 200,
                'headers': {'Content-Type': 'application/json'},
                'url': 'https://api.example.com/items',
                'body': '{"items": [2], "next_cursor": null}',
            },
        ]
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f)

        client = APIClient({
            **simple_schema(),
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/items',
                    'playback': {'path': path, 'mode': 'load'},
                    'pagination': cursor_pagination(
                        cursor_param='cursor',
                        next_cursor_path='next_cursor',
                        data_path='items'
                    ),
                }
            }
        })

        run = client.stream_run('ep')
        pages = list(run)

        self.assertEqual(pages, [[1], [2]])
        self.assertIsInstance(run.summary, StreamSummary)
        self.assertEqual(run.summary.source, 'playback')
        self.assertEqual(run.summary.pages, 2)
        self.assertEqual(run.summary.requests, 2)
        self.assertIsNotNone(run.summary.stop)
        self.assertEqual(run.summary.stop.kind, 'next_request_none')

    def test_non_paginated_playback_extra_recorded_responses_raise(self):
        path = self._path('fixture.json')
        payload = [
            {
                'kind': 'raw_response',
                'format': 'json',
                'status_code': 200,
                'headers': {'Content-Type': 'application/json'},
                'url': 'https://api.example.com/x',
                'body': '{"x": 1}',
            },
            {
                'kind': 'raw_response',
                'format': 'json',
                'status_code': 200,
                'headers': {'Content-Type': 'application/json'},
                'url': 'https://api.example.com/x',
                'body': '{"x": 2}',
            },
        ]
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f)

        client = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                'playback': {'path': path, 'mode': 'load'}}}
        })
        with self.assertRaises(PlaybackError) as cm:
            client.fetch('ep')
        self.assertIn('extra recorded responses after execution completed', str(cm.exception))

    def test_text_format_save_and_load(self):
        path = self._path('fixture.json')
        with patch('requests.Session.request') as mock_req:
            r = make_response(200, None)
            r.text = 'hello world'
            r._content = b'hello world'
            r.headers['Content-Type'] = 'text/plain'
            mock_req.return_value = r
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                    'response_format': 'text',
                    'playback': {'path': path, 'mode': 'save'}}}
            })
            saved = client.fetch('ep')
        self.assertEqual(saved, 'hello world')

        client2 = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                'response_format': 'text',
                'playback': {'path': path, 'mode': 'load'}}}
        })
        self.assertEqual(client2.fetch('ep'), 'hello world')

    def test_fixture_exhausted_raises_playback_error(self):
        from rest_fetcher.exceptions import PlaybackError
        path = self._path('fixture.json')
        # Write a fixture with only 1 page, but pagination expects 2
        with open(path, 'w') as f:
            json.dump([{
                'kind': 'raw_response',
                'format': 'json',
                'status_code': 200,
                'headers': {'Content-Type': 'application/json'},
                'url': 'https://api.example.com/x',
                'body': json.dumps({'items': [1], 'next': True}),
            }], f)

        client = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                'playback': {'path': path, 'mode': 'load'},
                'pagination': {
                    'next_request': lambda r, s: {'params': {}} if r.get('next') else None,
                },
                'on_response': lambda r, s: r.get('items', []),
            }},
        })
        with self.assertRaises(PlaybackError) as cm:
            client.fetch('ep')
        self.assertIn('exhausted', str(cm.exception))

    def test_extra_pages_in_fixture_raises_playback_error(self):
        from rest_fetcher.exceptions import PlaybackError
        path = self._path('fixture.json')
        with open(path, 'w') as f:
            json.dump([
                {'kind': 'raw_response', 'format': 'json', 'status_code': 200,
                 'headers': {'Content-Type': 'application/json'},
                 'url': 'https://api.example.com/x',
                 'body': json.dumps({'items': [1]})},
                {'kind': 'raw_response', 'format': 'json', 'status_code': 200,
                 'headers': {'Content-Type': 'application/json'},
                 'url': 'https://api.example.com/x',
                 'body': json.dumps({'items': [2]})},
            ], f)

        client = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                'playback': {'path': path, 'mode': 'load'},
                'pagination': {
                    'next_request': lambda r, s: None,  # stops after 1 page
                },
                'on_response': lambda r, s: r.get('items', []),
            }},
        })
        with self.assertRaises(PlaybackError) as cm:
            client.fetch('ep')
        self.assertIn('extra', str(cm.exception))

    def test_caps_do_not_apply_during_raw_playback(self):
        'max_pages should not fire during playback replay'
        path = self._path('fixture.json')
        pages = [
            {'kind': 'raw_response', 'format': 'json', 'status_code': 200,
             'headers': {'Content-Type': 'application/json'},
             'url': f'https://api.example.com/x?page={i}',
             'body': json.dumps({'items': [i], 'next': i < 5})}
            for i in range(1, 6)  # 5 pages; last page has next=False
        ]
        with open(path, 'w') as f:
            json.dump(pages, f)

        client = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                'playback': {'path': path, 'mode': 'load'},
                'pagination': {
                    'next_request': lambda r, s: {'params': {}} if r.get('next') else None,
                },
                'on_response': lambda r, s: r.get('items', []),
            }},
        })
        # max_pages=2 is set but should be ignored during playback
        result = client.fetch('ep', max_pages=2)
        self.assertEqual(result, [[1], [2], [3], [4], [5]])

    def test_fixture_written_when_stream_is_broken_early(self):
        '''playback fixture is saved even when the caller breaks out of stream() mid-way'''
        path = self._path('fixture.json')
        responses = [
            make_response(200, {'items': [i], 'next': i < 5},
                          headers={'Content-Type': 'application/json'})
            for i in range(1, 6)
        ]
        for i, r in enumerate(responses, 1):
            r.url = f'https://api.example.com/x?page={i}'

        with patch('requests.Session.request', side_effect=responses):
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {
                    'method': 'GET', 'path': '/x',
                    'playback': {'path': path, 'mode': 'save'},
                    'pagination': {
                        'next_request': lambda r, s: {'params': {'page': 2}} if r.get('next') else None,
                    },
                    'on_response': lambda r, s: r.get('items', []),
                }}
            })
            pages_seen = []
            for page in client.stream('ep'):
                pages_seen.append(page)
                if len(pages_seen) >= 2:
                    break   # early exit — fixture must still be written

        self.assertTrue(os.path.exists(path), 'fixture file must exist after early break')
        with open(path, encoding='utf-8') as f:
            payload = json.load(f)
        self.assertEqual(len(payload), 2, 'fixture should contain exactly the 2 pages that were fetched')
        self.assertEqual(payload[0]['kind'], 'raw_response')


    def test_non_paginated_save_with_on_complete_writes_fixture(self):
        path = self._path('fixture.json')

        def on_complete(pages, state):
            return {'pages': pages, 'count': len(pages)}

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'x': 7}, headers={'Content-Type': 'application/json'})
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                    'playback': {'path': path, 'mode': 'save'},
                    'on_complete': on_complete}}
            })
            result = client.fetch('ep')

        self.assertEqual(result, {'pages': [{'x': 7}], 'count': 1})
        self.assertTrue(os.path.exists(path), 'fixture file must exist after non-paginated save with on_complete')
        with open(path, encoding='utf-8') as f:
            payload = json.load(f)
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]['kind'], 'raw_response')

    def test_paginated_fetch_with_on_complete_writes_fixture(self):
        path = self._path('fixture.json')

        def on_complete(pages, state):
            return {'pages': pages, 'count': len(pages)}

        r1 = make_response(200, {'items': [1, 2], 'next': True}, headers={'Content-Type': 'application/json'})
        r1.url = 'https://api.example.com/x?page=1'
        r2 = make_response(200, {'items': [3], 'next': False}, headers={'Content-Type': 'application/json'})
        r2.url = 'https://api.example.com/x?page=2'

        with patch('requests.Session.request', side_effect=[r1, r2]):
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                    'playback': {'path': path, 'mode': 'save'},
                    'pagination': {
                        'next_request': lambda r, s: {'params': {'page': 2}} if r.get('next') else None,
                    },
                    'on_response': lambda r, s: r.get('items', []),
                    'on_complete': on_complete,
                }},
            })
            result = client.fetch('ep')

        self.assertEqual(result, {'pages': [[1, 2], [3]], 'count': 2})
        self.assertTrue(os.path.exists(path), 'fixture file must exist after paginated fetch with on_complete')
        with open(path, encoding='utf-8') as f:
            payload = json.load(f)
        self.assertEqual(len(payload), 2)
        self.assertTrue(all(page['kind'] == 'raw_response' for page in payload))

    def test_fixture_records_raw_response_envelope(self):
        path = self._path('fixture.json')
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'k': 'v'}, headers={'Content-Type': 'application/json'})
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                    'playback': {'path': path, 'mode': 'save'}}}
            })
            client.fetch('ep')

        with open(path) as f:
            payload = json.load(f)
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]['kind'], 'raw_response')
        self.assertIn('status_code', payload[0])
        self.assertIn('headers', payload[0])
        self.assertIn('body', payload[0])
        self.assertEqual(json.loads(payload[0]['body']), {'k': 'v'})

    def test_404_recorded_and_raises_on_load(self):
        from rest_fetcher.exceptions import RequestError
        path = self._path('fixture.json')
        with open(path, 'w') as f:
            json.dump([{
                'kind': 'raw_response',
                'format': 'json',
                'status_code': 404,
                'headers': {'Content-Type': 'application/json'},
                'url': 'https://api.example.com/x',
                'body': json.dumps({'message': 'not found'}),
            }], f)

        client = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                'playback': {'path': path, 'mode': 'load'}}}
        })
        with self.assertRaises(RequestError) as cm:
            client.fetch('ep')
        self.assertEqual(cm.exception.status_code, 404)

    def test_lowercase_content_type_header_detected_correctly(self):
        'RecordedResponse.headers must use case-insensitive lookup'
        path = self._path('fixture.json')
        with open(path, 'w') as f:
            json.dump([{
                'kind': 'raw_response',
                'format': 'json',
                'status_code': 200,
                'headers': {'content-type': 'application/json'},  # lowercase
                'url': 'https://api.example.com/x',
                'body': json.dumps({'n': 7}),
            }], f)

        client = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                'playback': {'path': path, 'mode': 'load'}}}
        })
        result = client.fetch('ep')
        self.assertEqual(result, {'n': 7})


class TestPlaybackScrubbing(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, 'fixture.json')

    def tearDown(self):
        self.tmp.cleanup()

    def test_paginated_stream_writes_raw_playback_fixture(self):
        schema = {
            'base_url': 'https://api.example.com',
            'auth': {'type': 'bearer', 'token': 'tok-secret'},
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/items',
                    'playback': {'path': self.path, 'mode': 'save'},
                    'pagination': offset_pagination(
                        limit=2,
                        data_path='items',
                        total_path='meta.total',
                    ),
                    'params': {'page': 1},
                }
            }
        }
        r1 = make_response(200, {'items': [{'id': 1}, {'id': 2}], 'meta': {'total': 3}},
                           headers={'Content-Type': 'application/json'})
        r1.url = 'https://api.example.com/items?page=1'
        r2 = make_response(200, {'items': [{'id': 3}], 'meta': {'total': 3}},
                           headers={'Content-Type': 'application/json'})
        r2.url = 'https://api.example.com/items?page=2'
        with patch('requests.Session.request', side_effect=[r1, r2]):
            client = APIClient(schema)
            pages = list(client.stream('ep'))
        self.assertEqual(len(pages), 2)
        self.assertTrue(os.path.exists(self.path))
        with open(self.path, encoding='utf-8') as f:
            payload = json.load(f)
        self.assertEqual(len(payload), 2)
        self.assertEqual(payload[0]['kind'], 'raw_response')
        self.assertEqual(payload[1]['kind'], 'raw_response')
        self.assertIn('page=1', payload[0]['url'])
        self.assertIn('page=2', payload[1]['url'])

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, 'fixture.json')

    def tearDown(self):
        self.tmp.cleanup()

    def test_raw_playback_fixture_scrubs_builtin_query_params(self):
        '''builtin defaults redact access_token from the recorded URL without any config'''
        schema = {
            'base_url': 'https://api.example.com',
            'auth': {'type': 'bearer', 'token': 'tok-secret'},
            'endpoints': {'ep': {'method': 'GET', 'path': '/items',
                'playback': {'path': self.path, 'mode': 'save'},
                'params': {'access_token': 'leaked', 'page': 1},
            }},
        }
        r = make_response(200, {'ok': True}, headers={'Content-Type': 'application/json'})
        r.url = 'https://api.example.com/items?access_token=leaked&page=1'
        with patch('requests.Session.request', return_value=r):
            APIClient(schema).fetch('ep')
        with open(self.path, encoding='utf-8') as f:
            env = json.load(f)[0]
        self.assertIn('access_token=[REDACTED]', env['url'])
        self.assertIn('page=1', env['url'])

    def test_raw_playback_saves_scrubbed_url_and_request_headers(self):
        schema = {
            'base_url': 'https://api.example.com',
            'auth': {'type': 'bearer', 'token': 'tok-secret'},
            'scrub_headers': ['X-Tenant-Id'],
            'scrub_query_params': ['tenant_token'],
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/items',
                    'playback': {'path': self.path, 'mode': 'save'},
                    'params': {'tenant_token': 'abc123', 'page': 1},
                    'headers': {'X-Tenant-Id': 'tenant-42'}
                }
            }
        }
        r = make_response(200, {'ok': True}, headers={'Content-Type': 'application/json'})
        r.url = 'https://api.example.com/items?tenant_token=abc123&page=1'
        with patch('requests.Session.request', return_value=r):
            client = APIClient(schema)
            client.fetch('ep')
        with open(self.path, encoding='utf-8') as f:
            payload = json.load(f)
        env = payload[0]
        self.assertEqual(env['kind'], 'raw_response')
        self.assertIn('tenant_token=[REDACTED]', env['url'])
        self.assertIn('page=1', env['url'])
        self.assertEqual(env['request_headers']['Authorization'], '***')
        self.assertEqual(env['request_headers']['X-Tenant-Id'], '***')
    def test_raw_playback_saves_unscrubbed_response_headers_and_body(self):
        schema = {
            'base_url': 'https://api.example.com',
            'auth': {'type': 'bearer', 'token': 'tok-secret'},
            'scrub_query_params': ['tenant_token'],
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/items',
                    'playback': {'path': self.path, 'mode': 'save'},
                    'params': {'tenant_token': 'abc123', 'page': 1},
                }
            }
        }
        r = make_response(200, {'token_echo': 'abc123'}, headers={'Content-Type': 'application/json', 'X-Response-Token': 'resp-secret'})
        r.url = 'https://api.example.com/items?tenant_token=abc123&page=1'
        with patch('requests.Session.request', return_value=r):
            client = APIClient(schema)
            client.fetch('ep')
        with open(self.path, encoding='utf-8') as f:
            payload = json.load(f)
        env = payload[0]
        self.assertEqual(env['headers']['X-Response-Token'], 'resp-secret')
        self.assertIn('abc123', env['body'])

    def test_endpoint_scrub_query_params_applies_to_playback_url(self):
        schema = {
            'base_url': 'https://api.example.com',
            'auth': {'type': 'bearer', 'token': 'tok-secret'},
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/items',
                    'playback': {'path': self.path, 'mode': 'save'},
                    'scrub_query_params': ['tenant_token'],
                    'params': {'tenant_token': 'abc123', 'page': 1},
                }
            }
        }
        r = make_response(200, {'ok': True}, headers={'Content-Type': 'application/json'})
        r.url = 'https://api.example.com/items?tenant_token=abc123&page=1'
        with patch('requests.Session.request', return_value=r):
            APIClient(schema).fetch('ep')
        with open(self.path, encoding='utf-8') as f:
            env = json.load(f)[0]
        self.assertIn('tenant_token=[REDACTED]', env['url'])

    def test_call_time_scrub_query_params_applies_to_playback_url(self):
        schema = {
            'base_url': 'https://api.example.com',
            'auth': {'type': 'bearer', 'token': 'tok-secret'},
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/items',
                    'playback': {'path': self.path, 'mode': 'save'},
                    'params': {'tenant_token': 'abc123', 'page': 1},
                }
            }
        }
        r = make_response(200, {'ok': True}, headers={'Content-Type': 'application/json'})
        r.url = 'https://api.example.com/items?tenant_token=abc123&page=1'
        with patch('requests.Session.request', return_value=r):
            APIClient(schema).fetch('ep', scrub_query_params=['tenant_token'])
        with open(self.path, encoding='utf-8') as f:
            env = json.load(f)[0]
        self.assertIn('tenant_token=[REDACTED]', env['url'])



class TestPlaybackModeNone(unittest.TestCase):
    def test_playback_mode_none_disables_record_and_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'fixture.json')
            schema = {
                'base_url': 'https://api.example.com',
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'playback': {'mode': 'none'},
                    }
                }
            }
            r = make_response(200, {'ok': True}, headers={'Content-Type': 'application/json'})
            r.url = 'https://api.example.com/x'
            with patch('requests.Session.request', return_value=r) as req:
                result = APIClient(schema).fetch('ep')
            self.assertEqual(result, {'ok': True})
            self.assertEqual(req.call_count, 1)
            self.assertFalse(os.path.exists(path))

    def test_playback_mode_none_in_builder(self):
        from rest_fetcher.types import SchemaBuilder
        schema = (SchemaBuilder('https://api.example.com')
                  .endpoint('ep', method='GET', path='/x', playback={'mode': 'none'})
                  .build())
        self.assertEqual(schema['endpoints']['ep']['playback']['mode'], 'none')


    def test_playback_mode_none_disables_playback_without_path(self):
        schema = {
            'base_url': 'https://api.example.com',
            'endpoints': {
                'users': {
                    'method': 'GET',
                    'path': '/users',
                    'playback': {'mode': 'none'},
                }
            }
        }
        client = APIClient(schema)
        with patch('requests.Session.request', return_value=make_response(200, [{'id': 1}], headers={'Content-Type': 'application/json'})):
            result = client.fetch('users')
        self.assertEqual(result, [{'id': 1}])


class TestPlaybackFixtureFormat(unittest.TestCase):
    def test_non_raw_playback_fixture_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'fixture.json')
            with open(path, 'w', encoding='utf-8') as f:
                json.dump([{'items': [1, 2]}, {'items': [3]}], f)
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                    'playback': {'path': path, 'mode': 'load'}}}
            })
            with self.assertRaises(PlaybackError) as cm:
                client.fetch('ep')
            self.assertIn('playback files must contain raw response envelopes', str(cm.exception))

    def test_mock_playback_save_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'fixture.json')
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x',
                    'mock': lambda request, *, run_state: {'x': 1},
                    'playback': {'path': path, 'mode': 'save'}}}
            })
            with self.assertRaises(PlaybackError) as cm:
                client.fetch('ep')
            self.assertIn('playback save is not supported with mock responses', str(cm.exception))


    def test_chunked_content_b64_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'fixture.json')
            with open(path, 'w', encoding='utf-8') as f:
                json.dump([{
                    'kind': 'raw_response',
                    'format': 'bytes',
                    'status_code': 200,
                    'headers': {'Content-Type': 'application/octet-stream'},
                    'url': 'https://api.example.com/x',
                    'content_b64': ['YWJj', 'ZA=='],
                }], f)
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'response_format': 'bytes',
                    'playback': {'path': path, 'mode': 'load'}}}
            })
            with self.assertRaises(PlaybackError) as cm:
                client.fetch('ep')
            self.assertIn('chunked content_b64 (list form) is not supported', str(cm.exception))

    def test_bytes_playback_fixture_round_trips_exact_bytes(self):
        payload = b'\x00\x01hello\xff'
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'fixture.json')
            with open(path, 'w', encoding='utf-8') as f:
                json.dump([{
                    'kind': 'raw_response',
                    'format': 'bytes',
                    'status_code': 200,
                    'headers': {'Content-Type': 'application/octet-stream'},
                    'url': 'https://api.example.com/x',
                    'content_b64': base64.b64encode(payload).decode('ascii'),
                }], f)
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'response_format': 'bytes',
                    'playback': {'path': path, 'mode': 'load'}}}
            })
            self.assertEqual(client.fetch('ep'), payload)

    def test_playback_record_as_bytes_for_textual_format_preserves_logical_format(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'fixture.json')
            body = {'items': [{'id': 1}], 'meta': {'next_cursor': 'tok'}}
            with patch('requests.Session.request') as mock_req:
                mock_req.return_value = make_response(200, body, headers={'Content-Type': 'application/json'})
                client = APIClient({
                    'base_url': 'https://api.example.com',
                    'endpoints': {
                        'ep': {
                            'method': 'GET',
                            'path': '/x',
                            'response_format': 'json',
                            'playback': {'path': path, 'mode': 'save', 'record_as_bytes': True},
                        }
                    }
                })
                result = client.fetch('ep')
            self.assertEqual(result, body)
            with open(path, encoding='utf-8') as f:
                env = json.load(f)[0]
            self.assertEqual(env['format'], 'json')
            self.assertIn('content_b64', env)
            self.assertNotIn('body', env)
            replay_client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'response_format': 'json',
                        'playback': {'path': path, 'mode': 'load'},
                    }
                }
            })
            self.assertEqual(replay_client.fetch('ep'), body)


    def test_recording_textual_fixture_fails_on_undecodable_bytes(self):
        bad_bytes = b'\xff\xfe\xff'
        resp = requests.Response()
        resp.status_code = 200
        resp._content = bad_bytes
        resp.headers = {'Content-Type': 'text/plain'}
        resp.url = 'https://api.example.com/v1/x'
        with self.assertRaises(FixtureFormatError):
            _serialize_playback(resp, response_format='text', encoding='utf-8', record_as_bytes=False)


    def test_recording_textual_fixture_fails_on_invalid_encoding(self):
        resp = make_response(200, {'ok': True}, headers={'Content-Type': 'text/plain'})
        with self.assertRaises(FixtureFormatError):
            _serialize_playback(resp, response_format='text', encoding='definitely-not-a-real-codec', record_as_bytes=False)

    def test_xml_textual_recording_normalizes_declaration_for_utf8_body(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'fixture.json')
            xml_bytes = b'<?xml version="1.0" encoding="windows-1251"?><root><item>ok</item></root>'
            r = MagicMock()
            r.status_code = 200
            r.ok = True
            r.headers = {'Content-Type': 'application/xml; charset=windows-1251'}
            r.content = xml_bytes
            r.text = xml_bytes.decode('windows-1251')
            r.url = 'https://api.example.com/x'
            with patch('requests.Session.request', return_value=r):
                client = APIClient({
                    'base_url': 'https://api.example.com',
                    'endpoints': {
                        'ep': {
                            'method': 'GET',
                            'path': '/x',
                            'response_format': 'xml',
                            'playback': {'path': path, 'mode': 'save'},
                        }
                    }
                })
                node = client.fetch('ep')
            self.assertEqual(node.tag, 'root')
            with open(path, encoding='utf-8') as f:
                env = json.load(f)[0]
            self.assertIn('body', env)
            self.assertNotIn('content_b64', env)
            self.assertNotIn('windows-1251', env['body'].lower())



class TestPlaybackLoadBehavior(unittest.TestCase):
    """Additional load-mode behavioral tests extracted from stale containers."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _path(self, name):
        return os.path.join(self.tmpdir, name)

    def test_playback_load_still_bypasses_caps_without_cached_flag(self):
        'playback load still ignores OperationContext caps after _playback_load_mode removal'
        fixture_path = self._path('raw_fixture.json')
        with open(fixture_path, 'w', encoding='utf-8') as f:
            json.dump([{
                'kind': 'raw_response',
                'status_code': 200,
                'format': 'json',
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'items': [1, 2, 3]}),
                'url': 'https://api.example.com/x',
            }], f)

        client = APIClient({
            'base_url': 'https://api.example.com',
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/x',
                    'playback': {'path': fixture_path, 'mode': 'load'},
                    'on_response': lambda resp, state: resp['items'],
                }
            }
        })

        result = client.fetch('ep', max_pages=0)
        self.assertEqual(result, [1, 2, 3])


    def test_playback_load_reapplies_on_response(self):
        with tempfile.TemporaryDirectory() as tmp:
            playback_file = os.path.join(tmp, 'pages.json')
            with open(playback_file, 'w', encoding='utf-8') as f:
                json.dump([{
                    'kind': 'raw_response',
                    'format': 'text',
                    'status_code': 200,
                    'headers': {'Content-Type': 'text/plain'},
                    'url': 'https://api.example.com/x',
                    'body': 'hello',
                }], f)
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {
                    'x': {
                        'method': 'GET',
                        'path': '/x',
                        'playback': {'path': playback_file, 'mode': 'load'},
                        'response_format': 'text',
                        'on_response': lambda resp, state: resp.upper(),
                    }
                }
            })
            self.assertEqual(client.fetch('x'), 'HELLO')


    def test_playback_load_replays_pagination_stack_from_raw_responses(self):
        with tempfile.TemporaryDirectory() as tmp:
            playback_file = os.path.join(tmp, 'pages.json')
            with open(playback_file, 'w', encoding='utf-8') as f:
                json.dump([
                    {
                        'kind': 'raw_response',
                        'format': 'json',
                        'status_code': 200,
                        'headers': {'Content-Type': 'application/json'},
                        'url': 'https://api.example.com/x?page=1',
                        'body': json.dumps({'items': [1, 2], 'next': True}),
                    },
                    {
                        'kind': 'raw_response',
                        'format': 'json',
                        'status_code': 200,
                        'headers': {'Content-Type': 'application/json'},
                        'url': 'https://api.example.com/x?page=2',
                        'body': json.dumps({'items': [3], 'next': False}),
                    },
                ], f)
            client = APIClient({
                'base_url': 'https://api.example.com',
                'endpoints': {
                    'x': {
                        'method': 'GET',
                        'path': '/x',
                        'playback': {'path': playback_file, 'mode': 'load'},
                        'pagination': {
                            'next_request': lambda resp, state: {'params': {'page': 2}} if resp.get('next') else None,
                        },
                        'on_response': lambda resp, state: resp.get('items', []),
                    }
                }
            })
            self.assertEqual(client.fetch('x'), [[1, 2], [3]])


    def test_playback_save_records_raw_json_response(self):
        with tempfile.TemporaryDirectory() as tmp:
            playback_file = os.path.join(tmp, 'pages.json')
            with patch('requests.Session.request') as mock_req:
                mock_req.return_value = make_response(200, {'items': [1]}, headers={'Content-Type': 'application/json'})
                client = APIClient({
                    'base_url': 'https://api.example.com',
                    'endpoints': {
                        'x': {
                            'method': 'GET',
                            'path': '/x',
                            'playback': {'path': playback_file, 'mode': 'save'},
                        }
                    }
                })
                client.fetch('x')
            with open(playback_file, encoding='utf-8') as f:
                payload = json.load(f)
            self.assertEqual(payload[0]['kind'], 'raw_response')
            self.assertEqual(payload[0]['format'], 'json')


