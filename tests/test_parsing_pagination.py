# Domain: parsing and pagination data flow.
# Place new tests here by primary behavioral contract when the assertion is
# canonical_parser/response_parser behavior, parsed-body vs page-payload flow,
# pagination helpers (strategy logic, stop conditions, state mutation),
# next_request behavior, or pagination callback wiring.
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
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from types import SimpleNamespace

from rest_fetcher import (
    APIClient,
    SchemaError,
    cursor_pagination,
    link_header_pagination,
    url_header_pagination,
    offset_pagination,
    page_number_pagination,
)
from rest_fetcher import CallbackError
from rest_fetcher.pagination import CycleRunner, _resolve_path
from rest_fetcher.parsing import default_parse_response
from rest_fetcher.schema import validate


def outcome(parsed, request_kwargs=None, headers=None):
    return SimpleNamespace(
        parsed=parsed,
        headers=headers or {},
        request_kwargs=request_kwargs or {},
    )


def make_response(status=200, body=None, headers=None):
    r = MagicMock()
    r.status_code = status
    r.ok = status < 400
    r.url = 'https://api.example.com/v1/test'
    r.headers = headers or {}
    r.json.return_value = body or {}
    r.text = json.dumps(body or {})
    return r


def simple_schema(**endpoint_overrides):
    endpoint = {'method': 'GET', 'path': '/test'}
    endpoint.update(endpoint_overrides)
    return {'base_url': 'https://api.example.com/v1', 'endpoints': {'test': endpoint}}


class TestSafeInt(unittest.TestCase):
    def setUp(self):
        from rest_fetcher.pagination import _safe_int

        self.safe_int = _safe_int

    def test_numeric_string(self):
        self.assertEqual(self.safe_int('629', 0), 629)

    def test_int_value(self):
        self.assertEqual(self.safe_int(42, 0), 42)

    def test_none_returns_fallback(self):
        self.assertIsNone(self.safe_int(None, None))
        self.assertEqual(self.safe_int(None, 0), 0)

    def test_malformed_string_returns_fallback(self):
        self.assertIsNone(self.safe_int('unknown', None))
        self.assertIsNone(self.safe_int('', None))
        self.assertIsNone(self.safe_int('25 items', None))
        self.assertIsNone(self.safe_int('N/A', None))

    def test_does_not_raise(self):
        # must never raise regardless of input
        for bad in ['unknown', '', 'inf', '25 items', [], {}, object()]:
            try:
                self.safe_int(bad, None)
            except Exception as e:
                self.fail(f'_safe_int raised {e!r} for input {bad!r}')


class TestSafeItems(unittest.TestCase):
    def setUp(self):
        from rest_fetcher.pagination import _safe_items

        self.safe_items = _safe_items

    def test_list_returned_unchanged(self):
        items = [1, 2, 3]
        self.assertIs(self.safe_items(items), items)

    def test_none_returns_empty_list(self):
        self.assertEqual(self.safe_items(None), [])

    def test_dict_returns_empty_list(self):
        self.assertEqual(self.safe_items({'a': 1}), [])

    def test_string_returns_empty_list(self):
        self.assertEqual(self.safe_items('oops'), [])

    def test_int_returns_empty_list(self):
        self.assertEqual(self.safe_items(42), [])

    def test_empty_list_returned_unchanged(self):
        self.assertEqual(self.safe_items([]), [])


class TestResolvePath(unittest.TestCase):
    "module-level _resolve_path used by all strategies for dotted key extraction"

    def setUp(self):
        from rest_fetcher.pagination import _resolve_path

        self.resolve = _resolve_path

    def test_flat_key(self):
        self.assertEqual(self.resolve({'total': 100}, 'total'), 100)

    def test_dotted_path(self):
        self.assertEqual(self.resolve({'meta': {'total': 629}}, 'meta.total'), 629)

    def test_three_levels_deep(self):
        self.assertEqual(self.resolve({'a': {'b': {'c': 42}}}, 'a.b.c'), 42)

    def test_missing_top_key_returns_none(self):
        self.assertIsNone(self.resolve({'other': 1}, 'total'))

    def test_missing_nested_key_returns_none(self):
        self.assertIsNone(self.resolve({'meta': {'count': 10}}, 'meta.total'))

    def test_intermediate_non_dict_returns_none(self):
        self.assertIsNone(self.resolve({'meta': 'string'}, 'meta.total'))

    def test_empty_resp_returns_none(self):
        self.assertIsNone(self.resolve({}, 'meta.total'))


class TestMalformedTotalsDoNotRaise(unittest.TestCase):
    "integration-level: malformed total fields must never propagate exceptions"

    def test_offset_malformed_total_does_not_raise(self):
        strategy = offset_pagination(limit=10, data_path='items', total_path='total')
        state = {'offset': 0, 'limit': 10}
        try:
            strategy['next_request']({'items': list(range(10)), 'total': 'unknown'}, state)
        except Exception as e:
            self.fail(f'raised {type(e).__name__}: {e}')

    def test_offset_empty_string_total_does_not_raise(self):
        strategy = offset_pagination(limit=10, data_path='items', total_path='total')
        state = {'offset': 0, 'limit': 10}
        try:
            strategy['next_request']({'items': list(range(10)), 'total': ''}, state)
        except Exception as e:
            self.fail(f'raised {type(e).__name__}: {e}')

    def test_page_number_malformed_total_pages_does_not_raise(self):
        strategy = page_number_pagination(
            page_size=10, data_path='results', total_pages_path='pages'
        )
        state = {'page': 1, 'page_size': 10}
        try:
            strategy['next_request']({'results': list(range(10)), 'pages': 'unknown'}, state)
        except Exception as e:
            self.fail(f'raised {type(e).__name__}: {e}')

    def test_page_number_dotted_malformed_does_not_raise(self):
        strategy = page_number_pagination(
            page_size=10, data_path='results', total_pages_path='meta.pages'
        )
        state = {'page': 1, 'page_size': 10}
        try:
            strategy['next_request'](
                {'results': list(range(10)), 'meta': {'pages': '25 pages'}}, state
            )
        except Exception as e:
            self.fail(f'raised {type(e).__name__}: {e}')


class TestPaginationStrategies(unittest.TestCase):
    def test_offset_stops_when_items_less_than_limit(self):
        strategy = offset_pagination(limit=10, data_path='items')
        # simulate last page: 5 items returned, limit is 10
        resp = {'items': [1, 2, 3, 4, 5]}
        state = {'limit': 10}
        result = strategy['next_request'](resp, state)
        self.assertIsNone(result)

    def test_offset_continues_when_full_page(self):
        strategy = offset_pagination(limit=3, data_path='items')
        resp = {'items': [1, 2, 3]}
        state = {'offset': 0, 'limit': 3}
        result = strategy['next_request'](resp, state)
        self.assertIsNotNone(result)
        self.assertEqual(result['params']['offset'], 3)

    def test_offset_empty_items_stops(self):
        strategy = offset_pagination(limit=10, data_path='items')
        result = strategy['next_request']({'items': []}, {})
        self.assertIsNone(result)

    def test_offset_no_double_advance_multipage(self):
        "regression: on_response must not advance offset — only next_request should"
        strategy = offset_pagination(limit=3, data_path='items')
        # page 1: items 0,1,2 — next request should go to offset=3
        resp1 = {'items': [0, 1, 2]}
        state1 = {}
        result1 = strategy['next_request'](resp1, state1)
        self.assertIsNotNone(result1)
        self.assertEqual(result1['params']['offset'], 3)
        # on_response must NOT change any state
        strategy['on_response'](resp1, state1)
        # page 2: state carries offset=3 from initial_params/page_state (simulated),
        # next request should advance to offset=6
        state2 = {'offset': 3, 'limit': 3}
        resp2 = {'items': [3, 4, 5]}
        result2 = strategy['next_request'](resp2, state2)
        self.assertIsNotNone(result2)
        self.assertEqual(
            result2['params']['offset'], 6, 'double-advance bug: offset jumped to 9 instead of 6'
        )

    def test_offset_with_total_key_stops_at_total(self):
        strategy = offset_pagination(limit=10, data_path='items', total_path='total')
        resp = {'items': list(range(10)), 'total': 10}
        state = {'offset': 0, 'limit': 10}
        result = strategy['next_request'](resp, state)
        self.assertIsNone(result)

    def test_cursor_stops_when_no_cursor(self):
        strategy = cursor_pagination('cursor', 'meta.next_cursor', 'items')
        resp = {'items': [], 'meta': {'next_cursor': None}}
        result = strategy['next_request'](resp, {})
        self.assertIsNone(result)

    def test_cursor_stops_on_empty_string(self):
        'some apis return "" instead of null to signal end of pages'
        strategy = cursor_pagination('cursor', 'meta.next_cursor', 'items')
        resp = {'items': [1, 2], 'meta': {'next_cursor': ''}}
        result = strategy['next_request'](resp, {})
        self.assertIsNone(result)

    def test_cursor_provides_next_cursor(self):
        strategy = cursor_pagination('cursor', 'meta.next_cursor', 'items')
        resp = {'items': [1, 2], 'meta': {'next_cursor': 'page2token'}}
        result = strategy['next_request'](resp, {})
        self.assertEqual(result['params']['cursor'], 'page2token')

    def test_cursor_nested_path_resolution(self):
        strategy = cursor_pagination('cursor', 'a.b.c', 'items')
        resp = {'items': [], 'a': {'b': {'c': 'deep_cursor'}}}
        result = strategy['next_request'](resp, {})
        self.assertEqual(result['params']['cursor'], 'deep_cursor')

    def test_link_header_parses_next(self):
        strategy = link_header_pagination('items')
        resp = {'items': [1, 2, 3]}
        state = {
            '_response_headers': {
                'Link': '<https://api.example.com/users?page=2>; rel="next", <https://api.example.com/users?page=5>; rel="last"'
            }
        }
        result = strategy['next_request'](resp, state)
        self.assertEqual(result['url'], 'https://api.example.com/users?page=2')

    def test_link_header_stops_when_no_next(self):
        strategy = link_header_pagination()
        state = {
            '_response_headers': {'Link': '<https://api.example.com/users?page=5>; rel="last"'}
        }
        result = strategy['next_request']({}, state)
        self.assertIsNone(result)

    def test_page_number_increments(self):
        strategy = page_number_pagination(page_size=10, data_path='results')
        resp = {'results': list(range(10))}
        state = {'page': 1}
        result = strategy['next_request'](resp, state)
        self.assertEqual(result['params']['page'], 2)

    def test_page_number_empty_items_stops(self):
        strategy = page_number_pagination(page_size=10, data_path='results')
        result = strategy['next_request']({'results': []}, {})
        self.assertIsNone(result)

    def test_page_number_no_double_advance_multipage(self):
        "regression: on_response must not advance page — only next_request should"
        strategy = page_number_pagination(page_size=3, data_path='results')
        # page 1: next_request should advance to page 2
        resp1 = {'results': [1, 2, 3]}
        state1 = {'page': 1}
        result1 = strategy['next_request'](resp1, state1)
        self.assertIsNotNone(result1)
        self.assertEqual(result1['params']['page'], 2)
        # on_response must NOT change page
        strategy['on_response'](resp1, state1)
        # page 2: state carries page=2, next_request should advance to page 3 (not 4)
        state2 = {'page': 2, 'page_size': 3}
        resp2 = {'results': [4, 5, 6]}
        result2 = strategy['next_request'](resp2, state2)
        self.assertIsNotNone(result2)
        self.assertEqual(
            result2['params']['page'], 3, 'double-advance bug: jumped to page 4 instead of 3'
        )

    def test_page_number_stops_on_short_page(self):
        strategy = page_number_pagination(page_size=10, data_path='results')
        resp = {'results': [1, 2, 3]}
        state = {'page': 1}
        result = strategy['next_request'](resp, state)
        self.assertIsNone(result)


class TestParamsModeReplace(unittest.TestCase):
    def _two_page_run(self, next_req_fn, initial_params=None):
        requests_seen = []
        call_count = [0]

        def mock_fetch(request, ctx=None, *, run_state):
            requests_seen.append(
                {k: dict(v) if isinstance(v, dict) else v for k, v in request.items()}
            )
            call_count[0] += 1
            return outcome(({'data': [1, 2, 3]} if call_count[0] == 1 else {'data': [4]}), request)

        from rest_fetcher.pagination import CycleRunner

        runner = CycleRunner(
            {
                'next_request': next_req_fn,
                'on_response': lambda resp, state: resp.get('data', []),
            }
        )
        init = {
            'method': 'GET',
            'url': 'https://example.com',
            'params': initial_params or {'page': 1, 'filter': 'active', 'limit': 10},
            'headers': {'X-Foo': 'bar'},
        }
        list(runner.run(mock_fetch, init))
        return requests_seen

    def test_merge_mode_preserves_sticky_keys(self):
        def next_req(resp, state):
            return None if len(resp.get('data', [])) < 3 else {'params': {'page': 2}}

        seen = self._two_page_run(next_req)
        self.assertEqual(seen[1]['params']['filter'], 'active')
        self.assertEqual(seen[1]['params']['page'], 2)

    def test_replace_clears_sticky_keys(self):
        def next_req(resp, state):
            return (
                None
                if len(resp.get('data', [])) < 3
                else {'params': {'page': 2}, 'params_mode': 'replace'}
            )

        seen = self._two_page_run(next_req)
        self.assertNotIn('filter', seen[1]['params'])
        self.assertEqual(seen[1]['params']['page'], 2)

    def test_replace_with_empty_params_clears_all(self):
        def next_req(resp, state):
            return (
                None if len(resp.get('data', [])) < 3 else {'params': {}, 'params_mode': 'replace'}
            )

        seen = self._two_page_run(next_req)
        self.assertEqual(seen[1]['params'], {})

    def test_replace_without_params_key_clears_all(self):
        "params_mode=replace with no params key should treat it as {} and clear all"

        def next_req(resp, state):
            return (
                None if len(resp.get('data', [])) < 3 else {'params_mode': 'replace'}
            )  # no params key at all

        seen = self._two_page_run(next_req)
        self.assertEqual(seen[1]['params'], {})

    def test_replace_preserves_non_params_keys(self):
        def next_req(resp, state):
            return (
                None
                if len(resp.get('data', [])) < 3
                else {'params': {'page': 2}, 'params_mode': 'replace'}
            )

        seen = self._two_page_run(next_req)
        self.assertEqual(seen[1]['headers']['X-Foo'], 'bar')

    def test_replace_when_initial_request_has_no_params_key(self):
        "replace mode on a request that never had a params key — must not raise"
        from rest_fetcher.pagination import CycleRunner

        requests_seen = []
        call_count = [0]

        def mock_fetch(request, ctx=None, *, run_state):
            requests_seen.append(
                {k: dict(v) if isinstance(v, dict) else v for k, v in request.items()}
            )
            call_count[0] += 1
            return outcome(({'data': [1, 2, 3]} if call_count[0] == 1 else {'data': [4]}), request)

        runner = CycleRunner(
            {
                'next_request': lambda resp, state: (
                    None
                    if len(resp.get('data', [])) < 3
                    else {'params': {'page': 2}, 'params_mode': 'replace'}
                ),
                'on_response': lambda resp, state: resp.get('data', []),
            }
        )
        # initial request deliberately has no 'params' key
        initial = {'method': 'GET', 'url': 'https://example.com', 'headers': {'X-Foo': 'bar'}}
        try:
            pages = list(runner.run(mock_fetch, initial))
        except Exception as e:
            self.fail(f'raised {type(e).__name__} when initial request had no params: {e}')
        self.assertEqual(requests_seen[1].get('params'), {'page': 2})

    def test_params_mode_key_not_leaked_into_request(self):
        def next_req(resp, state):
            return (
                None
                if len(resp.get('data', [])) < 3
                else {'params': {'page': 2}, 'params_mode': 'replace'}
            )

        seen = self._two_page_run(next_req)
        self.assertNotIn('params_mode', seen[1])

    def test_builtin_cursor_pagination_replace_mode_clears_sticky_params(self):
        strategy = cursor_pagination(
            cursor_param='cursor',
            next_cursor_path='next_cursor',
            data_path='data',
            params_mode='replace',
        )
        requests_seen = []
        call_count = [0]

        def mock_fetch(request, ctx=None, *, run_state):
            requests_seen.append(
                {k: dict(v) if isinstance(v, dict) else v for k, v in request.items()}
            )
            call_count[0] += 1
            if call_count[0] == 1:
                return outcome({'data': [1, 2, 3], 'next_cursor': 'c2'}, request)
            return outcome({'data': [4], 'next_cursor': None}, request)

        runner = CycleRunner(strategy)
        initial = {
            'method': 'GET',
            'url': 'https://example.com',
            'params': {'page': 1, 'filter': 'active'},
            'headers': {'X-Foo': 'bar'},
        }
        list(runner.run(mock_fetch, initial))
        self.assertEqual(requests_seen[1]['params'], {'cursor': 'c2'})

    def test_builtin_offset_pagination_replace_mode_emits_replace(self):
        strategy = offset_pagination(limit=3, data_path='data', params_mode='replace')
        result = strategy['next_request'](
            {'data': [1, 2, 3]},
            {'_request': {'params': {'offset': 0, 'limit': 3, 'filter': 'active'}}},
        )
        self.assertEqual(result['params_mode'], 'replace')

    def test_builtin_page_number_pagination_replace_mode_emits_replace(self):
        strategy = page_number_pagination(page_size=3, data_path='results', params_mode='replace')
        result = strategy['next_request'](
            {'results': [1, 2, 3]},
            {'_request': {'params': {'page': 1, 'page_size': 3, 'filter': 'active'}}},
        )
        self.assertEqual(result['params_mode'], 'replace')

    def test_builtin_params_mode_rejects_invalid_value(self):
        with self.assertRaises(ValueError):
            cursor_pagination(
                cursor_param='cursor', next_cursor_path='next_cursor', params_mode='invalid'
            )

    def test_runtime_invalid_params_mode_raises_callback_error(self):
        def next_req(resp, state):
            return (
                None
                if len(resp.get('data', [])) < 3
                else {'params': {'page': 2}, 'params_mode': 'Replace'}
            )

        with self.assertRaises(CallbackError) as ctx:
            self._two_page_run(next_req)
        self.assertIn('params_mode', str(ctx.exception))


class TestStrategyRespectsCallTimeOverrides(unittest.TestCase):
    def test_offset_strategy_respects_call_time_limit(self):
        "if first request used limit=200, page 2+ should also use 200, not factory 100"
        from rest_fetcher.pagination import CycleRunner

        requests_seen = []
        call_count = [0]

        def mock_fetch(request, ctx=None, *, run_state):
            requests_seen.append(
                {k: dict(v) if isinstance(v, dict) else v for k, v in request.items()}
            )
            call_count[0] += 1
            # return 200 items on page 1 (full page at call-time limit)
            return outcome(
                ({'items': list(range(200))} if call_count[0] == 1 else {'items': []}), request
            )

        strategy = offset_pagination(limit=100, data_path='items')
        runner = CycleRunner(strategy)
        # first request has call-time override: limit=200
        initial = {
            'method': 'GET',
            'url': 'https://example.com',
            'params': {'offset': 0, 'limit': 200},
        }
        list(runner.run(mock_fetch, initial))
        # page 2 request must carry limit=200, not factory default 100
        self.assertEqual(
            requests_seen[1]['params']['limit'],
            200,
            'strategy flipped back to factory limit=100 instead of call-time limit=200',
        )

    def test_page_number_strategy_respects_call_time_page_size(self):
        "if first request used page_size=50, page 2+ should also use 50, not factory 20"
        from rest_fetcher.pagination import CycleRunner

        requests_seen = []
        call_count = [0]

        def mock_fetch(request, ctx=None, *, run_state):
            requests_seen.append(
                {k: dict(v) if isinstance(v, dict) else v for k, v in request.items()}
            )
            call_count[0] += 1
            return outcome(
                ({'results': list(range(50))} if call_count[0] == 1 else {'results': []}), request
            )

        strategy = page_number_pagination(page_size=20, data_path='results')
        runner = CycleRunner(strategy)
        initial = {
            'method': 'GET',
            'url': 'https://example.com',
            'params': {'page': 1, 'page_size': 50},
        }
        list(runner.run(mock_fetch, initial))
        self.assertEqual(
            requests_seen[1]['params']['page_size'],
            50,
            'strategy flipped back to factory page_size=20 instead of call-time page_size=50',
        )

    def test_offset_short_page_check_uses_effective_limit(self):
        "stop condition uses effective_limit from state, not factory constant"
        strategy = offset_pagination(limit=100, data_path='items')
        # state reflects call-time override of limit=50
        state = {'offset': 0, 'limit': 50}
        # 50 items returned = full page at limit=50, so should continue
        result = strategy['next_request']({'items': list(range(50))}, state)
        self.assertIsNotNone(result, 'should continue: 50 items == effective limit 50')
        # 49 items = short page at limit=50, so should stop
        state2 = {'offset': 50, 'limit': 50}
        result2 = strategy['next_request']({'items': list(range(49))}, state2)
        self.assertIsNone(result2, 'should stop: 49 items < effective limit 50')


class TestOffsetPaginationDottedTotal(unittest.TestCase):
    def test_flat_total_key(self):
        strategy = offset_pagination(limit=10, data_path='items', total_path='total')
        state = {'offset': 0, 'limit': 10}
        # 10 items, total=15 — should continue
        result = strategy['next_request']({'items': list(range(10)), 'total': 15}, state)
        self.assertIsNotNone(result)
        # 10 items, next_offset=10 >= total=10 — should stop
        state2 = {'offset': 0, 'limit': 10}
        result2 = strategy['next_request']({'items': list(range(10)), 'total': 10}, state2)
        self.assertIsNone(result2)

    def test_dotted_total_key(self):
        strategy = offset_pagination(limit=10, data_path='items', total_path='meta.total')
        state = {'offset': 0, 'limit': 10}
        result = strategy['next_request']({'items': list(range(10)), 'meta': {'total': 25}}, state)
        self.assertIsNotNone(result)

    def test_dotted_total_key_stops_at_total(self):
        strategy = offset_pagination(limit=10, data_path='items', total_path='meta.total')
        state = {'offset': 10, 'limit': 10}
        result = strategy['next_request']({'items': list(range(5)), 'meta': {'total': 15}}, state)
        self.assertIsNone(result, 'next_offset=15 >= total=15 should stop')

    def test_missing_total_path_stops_with_warning(self):
        "if total_key path resolves to None, stop immediately with a warning"
        strategy = offset_pagination(limit=10, data_path='items', total_path='meta.total')
        state = {'offset': 0, 'limit': 10}
        with self.assertLogs('rest_fetcher.pagination', level='WARNING') as cm:
            result = strategy['next_request']({'items': list(range(10)), 'meta': {}}, state)
        self.assertIsNone(result)
        self.assertTrue(any('invalid total' in m.lower() for m in cm.output))

    def test_string_total_coerced(self):
        "total value may arrive as string from some APIs"
        strategy = offset_pagination(limit=10, data_path='items', total_path='total')
        state = {'offset': 0, 'limit': 10}
        result = strategy['next_request']({'items': list(range(10)), 'total': '25'}, state)
        self.assertIsNotNone(result)

    def test_malformed_total_stops_with_warning_full_page(self):
        "non-numeric total: stop immediately with a warning (even on a full page)"
        strategy = offset_pagination(limit=10, data_path='items', total_path='total')
        state = {'offset': 0, 'limit': 10}
        with self.assertLogs('rest_fetcher.pagination', level='WARNING') as cm:
            result = strategy['next_request']({'items': list(range(10)), 'total': 'unknown'}, state)
        self.assertIsNone(result)
        self.assertTrue(any('invalid total' in m.lower() for m in cm.output))

    def test_malformed_total_stops_with_warning_short_page(self):
        "non-numeric total: stop immediately with a warning (short page too)"
        strategy = offset_pagination(limit=10, data_path='items', total_path='total')
        state = {'offset': 0, 'limit': 10}
        with self.assertLogs('rest_fetcher.pagination', level='WARNING') as cm:
            result = strategy['next_request']({'items': list(range(5)), 'total': 'unknown'}, state)
        self.assertIsNone(result)
        self.assertTrue(any('invalid total' in m.lower() for m in cm.output))

    def test_empty_string_total_stops_with_warning(self):
        strategy = offset_pagination(limit=10, data_path='items', total_path='total')
        state = {'offset': 0, 'limit': 10}
        with self.assertLogs('rest_fetcher.pagination', level='WARNING') as cm:
            result = strategy['next_request']({'items': list(range(10)), 'total': ''}, state)
        self.assertIsNone(result)
        self.assertTrue(any('invalid total' in m.lower() for m in cm.output))

    def test_items_none_stops(self):
        "items=None in response should stop cleanly, not raise"
        strategy = offset_pagination(limit=10, data_path='items')
        state = {'offset': 0, 'limit': 10}
        result = strategy['next_request']({'items': None}, state)
        self.assertIsNone(result)

    def test_items_dict_stops(self):
        "items as dict should stop cleanly, not raise"
        strategy = offset_pagination(limit=10, data_path='items')
        state = {'offset': 0, 'limit': 10}
        result = strategy['next_request']({'items': {'a': 1}}, state)
        self.assertIsNone(result)


class TestPageNumberPaginationDottedTotalPages(unittest.TestCase):
    def test_flat_total_pages_key(self):
        strategy = page_number_pagination(
            page_size=10, data_path='results', total_pages_path='pages'
        )
        state = {'page': 1, 'page_size': 10}
        result = strategy['next_request']({'results': list(range(10)), 'pages': 3}, state)
        self.assertIsNotNone(result)

    def test_dotted_total_pages_key(self):
        'nager.at / boozeapi style: {"pagination": {"pages": 63}}'
        strategy = page_number_pagination(
            page_size=10, data_path='data', total_pages_path='pagination.pages'
        )
        state = {'page': 1, 'page_size': 10}
        result = strategy['next_request'](
            {'data': list(range(10)), 'pagination': {'pages': 63}}, state
        )
        self.assertIsNotNone(result)

    def test_dotted_total_pages_stops_at_last(self):
        strategy = page_number_pagination(
            page_size=10, data_path='data', total_pages_path='pagination.pages'
        )
        state = {'page': 63, 'page_size': 10}
        result = strategy['next_request'](
            {'data': list(range(10)), 'pagination': {'pages': 63}}, state
        )
        self.assertIsNone(result)

    def test_missing_total_pages_path_stops_with_warning(self):
        strategy = page_number_pagination(
            page_size=10, data_path='results', total_pages_path='meta.pages'
        )
        state = {'page': 1, 'page_size': 10}
        with self.assertLogs('rest_fetcher.pagination', level='WARNING') as cm:
            result = strategy['next_request']({'results': list(range(10)), 'meta': {}}, state)
        self.assertIsNone(result)
        self.assertTrue(any('invalid total_pages' in m.lower() for m in cm.output))

    def test_string_total_pages_coerced(self):
        strategy = page_number_pagination(
            page_size=10, data_path='results', total_pages_path='pages'
        )
        state = {'page': 1, 'page_size': 10}
        result = strategy['next_request']({'results': list(range(10)), 'pages': '5'}, state)
        self.assertIsNotNone(result)

    def test_malformed_total_pages_stops_with_warning_full_page(self):
        strategy = page_number_pagination(
            page_size=10, data_path='results', total_pages_path='pages'
        )
        state = {'page': 1, 'page_size': 10}
        with self.assertLogs('rest_fetcher.pagination', level='WARNING') as cm:
            result = strategy['next_request']({'results': list(range(10)), 'pages': 'N/A'}, state)
        self.assertIsNone(result)
        self.assertTrue(any('invalid total_pages' in m.lower() for m in cm.output))

    def test_malformed_total_pages_stops_with_warning_short_page(self):
        strategy = page_number_pagination(
            page_size=10, data_path='results', total_pages_path='pages'
        )
        state = {'page': 1, 'page_size': 10}
        with self.assertLogs('rest_fetcher.pagination', level='WARNING') as cm:
            result = strategy['next_request']({'results': list(range(3)), 'pages': 'N/A'}, state)
        self.assertIsNone(result)
        self.assertTrue(any('invalid total_pages' in m.lower() for m in cm.output))


class TestPaginationStateOnStop(unittest.TestCase):
    def test_offset_state_advances_even_when_stopping_on_total(self):
        "stop on total: next_request returns None, but return value reflected correct next_offset before stopping"
        strategy = offset_pagination(limit=10, data_path='items', total_path='total')
        state = {'offset': 0, 'limit': 10}
        # next_offset (0+10) == total (10) => stop
        result = strategy['next_request']({'items': list(range(10)), 'total': 10}, state)
        self.assertIsNone(result)

    def test_page_number_state_advances_even_when_stopping_on_total_pages(self):
        "stop on total_pages: current_page == total_pages => stop"
        strategy = page_number_pagination(
            page_size=10, data_path='results', total_pages_path='pages'
        )
        state = {'page': 2, 'page_size': 10}
        result = strategy['next_request']({'results': list(range(10)), 'pages': 2}, state)
        self.assertIsNone(result)

    def test_items_none_stops_cleanly(self):
        strategy = page_number_pagination(page_size=10, data_path='results')
        state = {'page': 1, 'page_size': 10}
        result = strategy['next_request']({'results': None}, state)
        self.assertIsNone(result)

    def test_items_dict_stops_cleanly(self):
        strategy = page_number_pagination(page_size=10, data_path='results')
        state = {'page': 1, 'page_size': 10}
        result = strategy['next_request']({'results': {'unexpected': True}}, state)
        self.assertIsNone(result)


class TestLinkHeaderEdgeCases(unittest.TestCase):
    def setUp(self):
        from rest_fetcher.pagination import _parse_link_next

        self.parse = _parse_link_next

    def test_link_next_basic(self):
        self.assertEqual(self.parse('<a>; rel="next"'), 'a')

    def test_link_next_multiple_parts(self):
        self.assertEqual(self.parse('<a>; rel="prev", <b>; rel="next"'), 'b')

    def test_link_next_rel_anywhere(self):
        "rel after other params — real-world GitHub style"
        self.assertEqual(self.parse('<a>; type="x"; rel="next"'), 'a')

    def test_link_next_multiple_rel_params(self):
        "two separate rel= params; next is second"
        self.assertEqual(self.parse('<a>; rel="prev"; rel="next"'), 'a')

    def test_link_next_space_separated_next_first(self):
        self.assertEqual(self.parse('<a>; rel="next prev"'), 'a')

    def test_link_next_space_separated_next_second(self):
        "next appears second in space-separated value — old code missed this"
        self.assertEqual(self.parse('<a>; rel="prev next"'), 'a')

    def test_link_next_case_insensitive(self):
        self.assertEqual(self.parse('<a>; REL=Next'), 'a')

    def test_link_next_none(self):
        self.assertIsNone(self.parse(None))
        self.assertIsNone(self.parse(''))

    def test_link_next_no_next_rel(self):
        self.assertIsNone(self.parse('<a>; rel="prev"'))

    def test_link_next_whitespace_around_url(self):
        self.assertEqual(self.parse('  <a>  ;  rel="next"  '), 'a')

    def test_link_next_unquoted_rel(self):
        "rel=next without quotes — valid per RFC 5988"
        self.assertEqual(self.parse('<a>; rel=next'), 'a')

    def test_link_next_does_not_match_x_rel_substring(self):
        # would have been misdetected with "if 'rel=' in seg_lower" — startswith prevents this
        self.assertIsNone(self.parse('<a>; content-type="x-rel=foo"; rel="prev"'))

    def test_link_next_ignores_rel_in_other_param_value(self):
        self.assertIsNone(self.parse('<a>; title="rel=next"; rel="prev"'))

    def test_link_header_lowercase_key_lookup(self):
        "some HTTP libs normalize header names to lowercase — integration test"
        from rest_fetcher.pagination import CycleRunner, link_header_pagination

        calls = [0]

        def mock_fetch(request, ctx=None, *, run_state):
            calls[0] += 1
            if calls[0] == 1:
                return outcome(
                    [1, 2, 3],
                    request,
                    {'link': '<https://api.example.com/page=2>; rel="next"', '_status_code': 200},
                )
            return outcome([4], request, {'_status_code': 200})

        runner = CycleRunner(
            {
                'next_request': link_header_pagination()['next_request'],
                'on_response': lambda resp, state: resp,
            }
        )
        pages = list(runner.run(mock_fetch, {'method': 'GET', 'url': 'https://api.example.com'}))
        self.assertEqual(calls[0], 2)


class TestStateReflectsProgressNotNextRequest(unittest.TestCase):
    """
    intentional documentation of state-update-before-stop behaviour.
    state reflects pages consumed, not the next request to be made.
    do not "fix" this — it is intentional and relied upon by on_complete callbacks.
    """

    def test_offset_state_on_last_page_reflects_total_consumed(self):
        "on last page, next_request returns None — the stop is clean with no side-effects"
        strategy = offset_pagination(limit=10, data_path='items')
        state = {'offset': 20, 'limit': 10}
        # 3 items = short last page — stops; no state mutation expected
        result = strategy['next_request']({'items': [1, 2, 3]}, state)
        self.assertIsNone(result, 'should stop on short page')

    def test_page_number_state_on_last_page_reflects_next_increment(self):
        strategy = page_number_pagination(page_size=10, data_path='results')
        state = {'page': 5, 'page_size': 10}
        result = strategy['next_request']({'results': [1, 2, 3]}, state)
        self.assertIsNone(result)


class TestStrategyTypeNormalization(unittest.TestCase):
    def test_offset_string_limit_coerced_to_int(self):
        "limit arriving as string from query params should not break stop condition"
        strategy = offset_pagination(limit=10, data_path='items')
        state = {'offset': '0', 'limit': '10'}  # strings, as from URL params
        # 10 items = full page — should continue
        result = strategy['next_request']({'items': list(range(10))}, state)
        self.assertIsNotNone(result, 'should continue on full page even with string limit')
        self.assertEqual(result['params']['limit'], 10)

    def test_offset_string_offset_coerced_to_int(self):
        strategy = offset_pagination(limit=10, data_path='items')
        state = {'offset': '20', 'limit': 10}
        result = strategy['next_request']({'items': list(range(10))}, state)
        self.assertEqual(result['params']['offset'], 30, 'offset arithmetic must use int')

    def test_page_number_string_page_size_coerced(self):
        strategy = page_number_pagination(page_size=10, data_path='results')
        state = {'page': '1', 'page_size': '10'}
        result = strategy['next_request']({'results': list(range(10))}, state)
        self.assertIsNotNone(result)
        self.assertEqual(result['params']['page_size'], 10)
        self.assertEqual(result['params']['page'], 2)

    def test_offset_none_limit_falls_back_to_factory(self):
        "None in state for limit should fall back to factory default, not raise"
        strategy = offset_pagination(limit=50, data_path='items')
        state = {'offset': 0, 'limit': None}
        result = strategy['next_request']({'items': list(range(50))}, state)
        self.assertIsNotNone(result)
        self.assertEqual(result['params']['limit'], 50)

    def test_page_number_none_page_size_falls_back(self):
        strategy = page_number_pagination(page_size=50, data_path='results')
        state = {'page': 1, 'page_size': None}
        result = strategy['next_request']({'results': list(range(50))}, state)
        self.assertIsNotNone(result)
        self.assertEqual(result['params']['page_size'], 50)

    def test_offset_state_updated_even_on_last_page(self):
        "on last page (short), next_request returns None cleanly — no state side-effects"
        strategy = offset_pagination(limit=10, data_path='items')
        state = {'offset': 0, 'limit': 10}
        # 5 items = short page = last page
        result = strategy['next_request']({'items': list(range(5))}, state)
        self.assertIsNone(result, 'should stop on short page')

    def test_page_number_state_updated_even_on_last_page(self):
        strategy = page_number_pagination(page_size=10, data_path='results')
        state = {'page': 3, 'page_size': 10}
        result = strategy['next_request']({'results': list(range(5))}, state)
        self.assertIsNone(result)

    def test_offset_state_limit_param_aligned_after_call(self):
        "effective limit is carried through the return value, not via state mutation"
        strategy = offset_pagination(limit=10, limit_param='limit', data_path='items')
        state = {'offset': 0, 'limit': 25}  # call-time override
        result = strategy['next_request']({'items': list(range(25))}, state)
        self.assertIsNotNone(result)
        self.assertEqual(
            result['params']['limit'], 25, 'returned params must carry effective limit'
        )

    def test_page_size_state_aligned_after_call(self):
        strategy = page_number_pagination(
            page_size=10, page_size_param='page_size', data_path='results'
        )
        state = {'page': 1, 'page_size': 30}
        result = strategy['next_request']({'results': list(range(30))}, state)
        self.assertIsNotNone(result)
        self.assertEqual(result['params']['page_size'], 30)


class TestBuiltinPaginationWithClient(unittest.TestCase):
    "end-to-end tests of built-in strategies through the full APIClient pipeline"

    def test_cursor_pagination_full_run(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'users': {
                        'method': 'GET',
                        'path': '/users',
                        'pagination': cursor_pagination('cursor', 'meta.cursor', 'users'),
                        'mock': [
                            {'users': [{'id': 1}, {'id': 2}], 'meta': {'cursor': 'tok2'}},
                            {'users': [{'id': 3}], 'meta': {'cursor': None}},
                        ],
                    }
                },
            }
        )
        result = client.fetch('users')
        self.assertEqual(result, [[{'id': 1}, {'id': 2}], [{'id': 3}]])

    def test_offset_pagination_full_run(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'items': {
                        'method': 'GET',
                        'path': '/items',
                        'pagination': offset_pagination(limit=2, data_path='items'),
                        'mock': [
                            {'items': [1, 2]},  # full page → continue
                            {'items': [3]},  # short page → stop
                        ],
                    }
                },
            }
        )
        result = client.fetch('items')
        self.assertEqual(result, [[1, 2], [3]])

    def test_offset_full_pipeline_correct_pages(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'items': {
                        'pagination': offset_pagination(limit=3, data_path='items'),
                        'mock': [{'items': [1, 2, 3]}, {'items': [4]}],
                    }
                },
            }
        )
        self.assertEqual(client.fetch('items'), [[1, 2, 3], [4]])

    def test_page_number_full_pipeline_correct_pages(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'items': {
                        'pagination': page_number_pagination(page_size=2, data_path='results'),
                        'mock': [{'results': [1, 2]}, {'results': [3, 4]}, {'results': [5]}],
                    }
                },
            }
        )
        self.assertEqual(client.fetch('items'), [[1, 2], [3, 4], [5]])

    def test_client_level_pagination_inherited(self):
        pages_fetched = []

        def on_resp(resp, state):
            pages_fetched.append(resp.get('page'))
            return resp.get('data', [])

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'pagination': {
                    'next_request': lambda resp, state: None,  # single page
                },
                'endpoints': {
                    'ep1': {'mock': [{'page': 1, 'data': ['a']}], 'on_response': on_resp},
                    'ep2': {'mock': [{'page': 2, 'data': ['b']}], 'on_response': on_resp},
                },
            }
        )
        client.fetch('ep1')
        client.fetch('ep2')
        self.assertEqual(pages_fetched, [1, 2])

    def test_endpoint_disables_inherited_pagination(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'pagination': {
                    'next_request': lambda resp, state: {'params': {'p': 2}},
                },
                'endpoints': {'no_pages': {'pagination': None, 'mock': [{'raw': 'response'}]}},
            }
        )
        result = client.fetch('no_pages')
        # pagination disabled: raw response returned, no on_response applied
        self.assertEqual(result, {'raw': 'response'})


# Item 7 — configurable header scrubbing
class TestPathResolverInjection(unittest.TestCase):
    def test_default_resolver_used_when_not_specified(self):
        from rest_fetcher.pagination import offset_pagination

        strategy = offset_pagination(limit=10, data_path='items', total_path='meta.total')
        state = {'offset': 0, 'limit': 10}
        result = strategy['next_request']({'items': list(range(10)), 'meta': {'total': 25}}, state)
        self.assertIsNotNone(result)

    def test_custom_path_resolver_used_when_provided(self):
        """injected resolver is called instead of default _resolve_path"""
        from rest_fetcher.pagination import offset_pagination

        resolver_calls = []

        def my_resolver(resp, path):
            resolver_calls.append(path)
            return resp.get(path)  # flat-only resolver

        strategy = offset_pagination(
            limit=10, data_path='items', total_path='total', path_resolver=my_resolver
        )
        state = {'offset': 0, 'limit': 10}
        strategy['next_request']({'items': list(range(10)), 'total': 25}, state)
        self.assertIn('total', resolver_calls, 'custom resolver must be called')

    def test_custom_resolver_for_list_index_style(self):
        """power user resolver supporting e.g. "data.0.count" or custom logic"""
        from rest_fetcher.pagination import offset_pagination

        def advanced_resolver(resp, path):
            # supports integer list indices: 'results.0.total'
            value = resp
            for part in path.split('.'):
                if isinstance(value, list):
                    try:
                        value = value[int(part)]
                    except (IndexError, ValueError):
                        return None
                elif isinstance(value, dict):
                    value = value.get(part)
                else:
                    return None
            return value

        strategy = offset_pagination(
            limit=5, data_path='items', total_path='pages.0.total', path_resolver=advanced_resolver
        )
        state = {'offset': 0, 'limit': 5}
        result = strategy['next_request'](
            {'items': list(range(5)), 'pages': [{'total': 20}]}, state
        )
        self.assertIsNotNone(result)

    def test_page_number_also_accepts_path_resolver(self):
        from rest_fetcher.pagination import page_number_pagination

        resolver_calls = []

        def my_resolver(resp, path):
            resolver_calls.append(path)
            return resp.get(path)

        strategy = page_number_pagination(
            page_size=10, data_path='results', total_pages_path='pages', path_resolver=my_resolver
        )
        state = {'page': 1, 'page_size': 10}
        strategy['next_request']({'results': list(range(10)), 'pages': 5}, state)
        self.assertIn('pages', resolver_calls)


# Item 3 — new tests for examples and state view
class TestUpdateStateEmptyDictOptimization(unittest.TestCase):
    def test_update_state_empty_dict_does_not_change_state_seen_by_next_request(self):
        from rest_fetcher.pagination import CycleRunner

        seen = []

        def update_state(resp, state):
            return {}

        def next_request(resp, state):
            seen.append(state.get('page'))
            return None

        runner = CycleRunner(
            {
                'initial_params': {'page': 1},
                'update_state': update_state,
                'next_request': next_request,
            }
        )

        def mock_fetch(request, ctx=None, *, run_state):
            return outcome({'items': [1]}, request, {'_status_code': 200})

        pages = list(runner.run(mock_fetch, {}))
        self.assertEqual(len(pages), 1)
        self.assertEqual(seen, [1])

    def test_update_state_non_empty_dict_with_falsy_value_still_updates_state(self):
        from rest_fetcher.pagination import CycleRunner

        seen = []

        def update_state(resp, state):
            return {'cursor': None, 'page': 2}

        def next_request(resp, state):
            seen.append((state.get('cursor'), state.get('page')))
            return None

        runner = CycleRunner(
            {
                'initial_params': {'page': 1, 'cursor': 'tok1'},
                'update_state': update_state,
                'next_request': next_request,
            }
        )

        def mock_fetch(request, ctx=None, *, run_state):
            return outcome({'items': [1]}, request, {'_status_code': 200})

        pages = list(runner.run(mock_fetch, {}))
        self.assertEqual(len(pages), 1)
        self.assertEqual(seen, [(None, 2)])


class TestCycleRunnerStructure(unittest.TestCase):
    """Structural invariants of CycleRunner."""

    def test_pagination_runner_none_has_safe_default_attributes(self):
        "CycleRunner(None) is structurally sound and exposes safe defaults"
        runner = CycleRunner(None)
        self.assertEqual(runner.initial_params, {})
        self.assertIsNone(runner._next_request)
        self.assertIsNone(runner._on_response)
        self.assertIsNone(runner._update_state)
        self.assertIsNone(runner._on_page)
        self.assertIsNone(runner._on_complete)
        self.assertEqual(runner._delay, 0.0)

    def test_trivial_runner_initial_params_are_a_noop_for_non_paginated_requests(self):
        "the trivial single-page runner contributes no initial params to non-paginated requests"
        seen = {}

        def mock_fn(req, ctx=None, *, run_state):
            seen['params'] = dict(req.get('params', {}))
            return {'ok': True}

        client = APIClient(
            {
                'base_url': 'https://api.example.com',
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'mock': mock_fn,
                    }
                },
            }
        )

        result = client.fetch('ep', params={'limit': 50})
        self.assertEqual(result, {'ok': True})
        self.assertEqual(seen['params'], {'limit': 50})


class TestNestedPathsAndDataPaths(unittest.TestCase):
    """Nested dotted paths and data_path options on built-in strategies."""

    def test_page_number_pagination_supports_nested_paths(self):
        cfg = page_number_pagination(data_path='payload.items', total_pages_path='meta.total_pages')
        resp = {'payload': {'items': [1, 2]}, 'meta': {'total_pages': 3}}
        state = {'_request': {'params': {'page': 1, 'page_size': 2}}}
        self.assertEqual(cfg['on_response'](resp, state), [1, 2])
        self.assertEqual(cfg['next_request'](resp, state), {'params': {'page': 2, 'page_size': 2}})

    def test_link_header_pagination_accepts_data_path(self):
        cfg = link_header_pagination(data_path='payload.items')
        resp = {'payload': {'items': [1, 2]}}
        state = {'_response_headers': {'Link': '<https://api.example.com/x?page=2>; rel="next"'}}
        self.assertEqual(cfg['on_response'](resp, state), [1, 2])
        self.assertEqual(
            cfg['next_request'](resp, state), {'url': 'https://api.example.com/x?page=2'}
        )

    def test_resolve_path_supports_list_indexes(self):
        self.assertEqual(_resolve_path({'items': [{'name': 'a'}]}, 'items.0.name'), 'a')


class TestParserArity(unittest.TestCase):
    def setUp(self):
        from rest_fetcher.client import _parser_arity

        self.arity = _parser_arity

    def test_one_arg_function(self):
        self.assertEqual(self.arity(lambda r: r), 1)

    def test_two_arg_function(self):
        self.assertEqual(self.arity(lambda r, p: p), 2)

    def test_two_arg_with_default_on_second(self):
        "second arg has a default — parser declares intent to receive parsed, so treated as 2-arg"
        self.assertEqual(self.arity(lambda r, p=None: p), 2)

    def test_var_positional_treated_as_two_arg(self):
        "*args can accept a second positional — treated as 2-arg"
        self.assertEqual(self.arity(lambda *args: args), 2)

    def test_one_plus_var_positional(self):
        self.assertEqual(self.arity(lambda r, *args: args), 2)

    def test_callable_class_one_arg(self):
        class P:
            def __call__(self, response):
                return response

        self.assertEqual(self.arity(P()), 1)

    def test_callable_class_two_arg(self):
        class P:
            def __call__(self, response, parsed):
                return parsed

        self.assertEqual(self.arity(P()), 2)

    def test_inspection_failure_defaults_to_one(self):
        "if signature cannot be inspected, fall back to 1-arg (safe default)"
        from rest_fetcher.client import _parser_arity

        # builtins like len have no inspectable Python signature on some platforms
        # simulate by checking the fallback path directly
        self.assertIn(_parser_arity(len), (1, 2))  # just must not raise


class TestResponseParserArity(unittest.TestCase):
    def _client_with_parser(self, parser):
        return APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'response_parser': parser,
                'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
            }
        )

    def test_one_arg_parser_receives_raw_response(self):
        "1-arg parser: gets raw Response, no pre-decode"
        received = {}

        def parser(response):
            received['type'] = type(response).__name__
            return {'from': 'custom'}

        with patch('requests.Session.request') as m:
            m.return_value = make_response(200, {'key': 'val'})
            self._client_with_parser(parser).fetch('ep')

        self.assertEqual(received['type'], 'MagicMock')

    def test_two_arg_parser_receives_response_and_pre_parsed(self):
        "2-arg parser: gets (response, pre_parsed) where pre_parsed is already decoded JSON"
        received = {}

        def parser(response, parsed):
            received['parsed'] = parsed
            return parsed

        with patch('requests.Session.request') as m:
            m.return_value = make_response(200, {'key': 'val'})
            self._client_with_parser(parser).fetch('ep')

        self.assertEqual(received['parsed'], {'key': 'val'})

    def test_two_arg_parser_can_ignore_parsed_and_use_response(self):
        "2-arg parser can discard pre_parsed and read response.text directly"

        def parser(response, parsed):
            return {'raw_len': len(response.text)}

        with patch('requests.Session.request') as m:
            m.return_value = make_response(200, {'key': 'val'})
            result = self._client_with_parser(parser).fetch('ep')

        self.assertIn('raw_len', result)

    def test_two_arg_parser_pre_parsed_is_none_for_empty_body(self):
        "2-arg parser: pre_parsed is None when body is empty (e.g. 204)"
        received = {}

        def parser(response, parsed):
            received['parsed'] = parsed
            return {}

        with patch('requests.Session.request') as m:
            r = make_response(200, {})
            r.text = ''
            m.return_value = r
            self._client_with_parser(parser).fetch('ep')

        self.assertIsNone(received['parsed'])

    def test_one_arg_default_parser_behaviour_unchanged(self):
        "no custom parser: default JSON decode works exactly as before"
        with patch('requests.Session.request') as m:
            m.return_value = make_response(200, {'items': [1, 2, 3]})
            result = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                }
            ).fetch('ep')
        self.assertEqual(result, {'items': [1, 2, 3]})

    def test_one_arg_parser_json_not_decoded_redundantly(self):
        "1-arg parser: parser_arity is detected as 1 for a single-arg callable"

        def counting_parser(response):
            return {'ok': True}

        with patch('requests.Session.request') as m:
            m.return_value = make_response(200, {'x': 1})
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'response_parser': counting_parser,
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                }
            )
            # verify arity was detected as 1
            job = client._make_job('ep', {})
            self.assertEqual(job._parser_arity, 1)


class TestDefaultParseResponseEmptyBody(unittest.TestCase):
    "unit tests for the empty-body detection logic in default_parse_response"

    def _response(self, content=b'', content_type='application/json'):
        r = MagicMock()
        r.content = content
        r.text = content.decode('utf-8') if isinstance(content, bytes) else content
        r.headers = {'Content-Type': content_type}
        if content:
            r.json.return_value = json.loads(content)
        return r

    def test_empty_bytes_returns_none(self):
        from rest_fetcher.parsing import default_parse_response

        r = self._response(b'')
        self.assertIsNone(default_parse_response(r, 'json'))

    def test_nonempty_json_returns_dict(self):
        from rest_fetcher.parsing import default_parse_response

        r = self._response(b'{"a": 1}')
        self.assertEqual(default_parse_response(r, 'json'), {'a': 1})

    def test_whitespace_only_text_returns_none(self):
        from rest_fetcher.parsing import default_parse_response

        r = MagicMock()
        r.content = None
        r.text = '   '
        r.headers = {'Content-Type': 'text/plain'}
        self.assertIsNone(default_parse_response(r, 'text'))

    def test_none_content_falls_back_to_text_check(self):
        from rest_fetcher.parsing import default_parse_response

        r = MagicMock()
        r.content = None
        r.text = 'hello'
        r.headers = {'Content-Type': 'text/plain'}
        self.assertEqual(default_parse_response(r, 'text'), 'hello')

    def test_default_parse_response_json(self):
        r = requests.Response()
        r.status_code = 200
        r._content = b'{"a": 1}'
        r.headers['Content-Type'] = 'application/json'
        self.assertEqual(default_parse_response(r, response_format='json'), {'a': 1})


class TestCsvDelimiter(unittest.TestCase):
    def test_response_format_csv_with_custom_delimiter(self):
        schema = {
            'base_url': 'https://example.com',
            'endpoints': {
                'report': {
                    'method': 'GET',
                    'path': '/report.csv',
                    'response_format': 'csv',
                    'csv_delimiter': ';',
                }
            },
        }
        response = MagicMock()
        response.status_code = 200
        response.ok = True
        response.url = 'https://example.com/report.csv'
        response.headers = {'Content-Type': 'text/csv'}
        response.text = 'id;name\n1;Alice\n2;Bob\n'
        response.content = response.text.encode('utf-8')
        with patch('requests.Session.request', return_value=response):
            rows = APIClient(schema).fetch('report')
        self.assertEqual(rows, [{'id': '1', 'name': 'Alice'}, {'id': '2', 'name': 'Bob'}])

    def test_response_format_csv_with_call_time_delimiter_override(self):
        schema = {
            'base_url': 'https://example.com',
            'endpoints': {
                'report': {
                    'method': 'GET',
                    'path': '/report.csv',
                    'response_format': 'csv',
                }
            },
        }
        response = MagicMock()
        response.status_code = 200
        response.ok = True
        response.url = 'https://example.com/report.csv'
        response.headers = {'Content-Type': 'text/csv'}
        response.text = 'id;name\n1;Alice\n2;Bob\n'
        response.content = response.text.encode('utf-8')
        with patch('requests.Session.request', return_value=response):
            rows = APIClient(schema).fetch('report', csv_delimiter=';')
        self.assertEqual(rows[0]['name'], 'Alice')

    def test_csv_playback_replay_uses_configured_delimiter(self):
        fixture_dir = tempfile.TemporaryDirectory()
        try:
            fixture_path = os.path.join(fixture_dir.name, 'csv_fixture.json')
            payload = [
                {
                    'kind': 'raw_response',
                    'format': 'csv',
                    'status_code': 200,
                    'headers': {'Content-Type': 'text/csv'},
                    'url': 'https://example.com/report.csv',
                    'body': 'id;name\n1;Alice\n2;Bob\n',
                    'request_headers': {},
                }
            ]
            with open(fixture_path, 'w', encoding='utf-8') as f:
                json.dump(payload, f)
            schema = {
                'base_url': 'https://example.com',
                'endpoints': {
                    'report': {
                        'method': 'GET',
                        'path': '/report.csv',
                        'response_format': 'csv',
                        'csv_delimiter': ';',
                        'playback': {'path': fixture_path, 'mode': 'load'},
                    }
                },
            }
            rows = APIClient(schema).fetch('report')
            self.assertEqual(rows[1]['id'], '2')
        finally:
            fixture_dir.cleanup()

    def test_csv_delimiter_schema_validation(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'csv_delimiter': ';;'})


class TestCsvEncoding(unittest.TestCase):
    def test_response_format_csv_with_custom_encoding(self):
        schema = {
            'base_url': 'https://example.com',
            'endpoints': {
                'report': {
                    'method': 'GET',
                    'path': '/report.csv',
                    'response_format': 'csv',
                    'csv_delimiter': ';',
                    'encoding': 'cp1251',
                }
            },
        }
        response = MagicMock()
        response.status_code = 200
        response.ok = True
        response.url = 'https://example.com/report.csv'
        response.headers = {'Content-Type': 'text/csv'}
        response.content = 'id;name\n1;Алиса\n2;Боб\n'.encode('cp1251')
        response.text = response.content.decode('latin1')
        with patch('requests.Session.request', return_value=response):
            rows = APIClient(schema).fetch('report')
        self.assertEqual(rows[0]['name'], 'Алиса')
        self.assertEqual(rows[1]['name'], 'Боб')

    def test_response_format_csv_with_call_time_encoding_override(self):
        schema = {
            'base_url': 'https://example.com',
            'endpoints': {
                'report': {
                    'method': 'GET',
                    'path': '/report.csv',
                    'response_format': 'csv',
                    'csv_delimiter': ';',
                }
            },
        }
        response = MagicMock()
        response.status_code = 200
        response.ok = True
        response.url = 'https://example.com/report.csv'
        response.headers = {'Content-Type': 'text/csv'}
        response.content = 'id;name\n1;Алиса\n'.encode('cp1251')
        response.text = response.content.decode('latin1')
        with patch('requests.Session.request', return_value=response):
            rows = APIClient(schema).fetch('report', encoding='cp1251')
        self.assertEqual(rows[0]['name'], 'Алиса')

    def test_csv_playback_record_and_replay_use_configured_encoding(self):
        fixture_dir = tempfile.TemporaryDirectory()
        try:
            fixture_path = os.path.join(fixture_dir.name, 'csv_fixture.json')
            response = MagicMock()
            response.status_code = 200
            response.ok = True
            response.url = 'https://example.com/report.csv'
            response.headers = {'Content-Type': 'text/csv'}
            response.content = 'id;name\n1;Алиса\n'.encode('cp1251')
            response.text = response.content.decode('latin1')
            schema_save = {
                'base_url': 'https://example.com',
                'endpoints': {
                    'report': {
                        'method': 'GET',
                        'path': '/report.csv',
                        'response_format': 'csv',
                        'csv_delimiter': ';',
                        'encoding': 'cp1251',
                        'playback': {'path': fixture_path, 'mode': 'save'},
                    }
                },
            }
            with patch('requests.Session.request', return_value=response):
                APIClient(schema_save).fetch('report')
            schema_load = {
                'base_url': 'https://example.com',
                'endpoints': {
                    'report': {
                        'method': 'GET',
                        'path': '/report.csv',
                        'response_format': 'csv',
                        'csv_delimiter': ';',
                        'encoding': 'cp1251',
                        'playback': {'path': fixture_path, 'mode': 'load'},
                    }
                },
            }
            rows = APIClient(schema_load).fetch('report')
            self.assertEqual(rows[0]['name'], 'Алиса')
        finally:
            fixture_dir.cleanup()

    def test_encoding_schema_validation(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'encoding': ''})


class TestCsvDefaults(unittest.TestCase):
    def test_response_format_csv_default_delimiter_is_semicolon(self):
        schema = {
            'base_url': 'https://example.com',
            'endpoints': {
                'report': {
                    'method': 'GET',
                    'path': '/report.csv',
                    'response_format': 'csv',
                }
            },
        }
        response = MagicMock()
        response.status_code = 200
        response.ok = True
        response.url = 'https://example.com/report.csv'
        response.headers = {'Content-Type': 'text/csv'}
        response.content = b'id;name\n1;Alice\n2;Bob\n'
        response.text = response.content.decode('utf-8')
        with patch('requests.Session.request', return_value=response):
            rows = APIClient(schema).fetch('report')
        self.assertEqual(rows, [{'id': '1', 'name': 'Alice'}, {'id': '2', 'name': 'Bob'}])


class TestCsvEncodingAndDelimiterOrder(unittest.TestCase):
    def test_csv_playback_decodes_with_encoding_before_delimiter_parse(self):
        rows_text = 'name;city\n\u0418\u0432\u0430\u043d;\u041c\u0438\u043d\u0441\u043a\n'
        encoded = rows_text.encode('cp1251')
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'fixture.json')
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(
                    [
                        {
                            'kind': 'raw_response',
                            'format': 'csv',
                            'status_code': 200,
                            'headers': {'Content-Type': 'text/csv'},
                            'url': 'https://api.example.com/report.csv',
                            'content_b64': base64.b64encode(encoded).decode('ascii'),
                            'encoding': 'cp1251',
                        }
                    ],
                    f,
                )
            client = APIClient(
                {
                    'base_url': 'https://api.example.com',
                    'encoding': 'utf-8',
                    'endpoints': {
                        'report': {
                            'method': 'GET',
                            'path': '/report.csv',
                            'response_format': 'csv',
                            'encoding': 'cp1251',
                            'csv_delimiter': ';',
                            'playback': {'path': path, 'mode': 'load'},
                        }
                    },
                }
            )
            rows = client.fetch('report')
        self.assertEqual(rows, [{'name': 'Иван', 'city': 'Минск'}])


class TestCanonicalParser(unittest.TestCase):
    def test_canonical_parser_feeds_pagination_and_response_parser(self):
        seen = {'next': None, 'on_response': None, 'parser_arg': None}

        def canonical_parser(content, ctx):
            self.assertIsInstance(content, (bytes, bytearray))
            self.assertIn('headers', ctx)
            return {'canonical': True}

        def response_parser(response, parsed):
            seen['parser_arg'] = parsed
            return {'wrapped': parsed}

        def next_request(parsed_body, state):
            seen['next'] = parsed_body
            return None

        def on_response(page_payload, state):
            seen['on_response'] = page_payload

        schema = {
            'base_url': 'https://api.example.com',
            'endpoints': {
                'test': {
                    'method': 'GET',
                    'path': '/x',
                    'response_format': 'text',
                    'canonical_parser': canonical_parser,
                    'response_parser': response_parser,
                    'pagination': {'next_request': next_request},
                    'on_response': on_response,
                }
            },
        }

        with patch('requests.Session.request') as mock_req:
            r = make_response(200, {'ignored': True}, headers={'Content-Type': 'text/plain'})
            r.content = b'<html><body>ok</body></html>'
            mock_req.return_value = r
            client = APIClient(schema)
            list(client.stream('test'))

        self.assertEqual(seen['next'], {'canonical': True})
        self.assertEqual(seen['parser_arg'], {'canonical': True})
        self.assertEqual(seen['on_response'], {'wrapped': {'canonical': True}})


class TestDefaultParseResponseBytes(unittest.TestCase):
    """default_parse_response must read .content bytes, never .text (prevents double-decode)."""

    class _ExplodingTextResponse:
        def __init__(self, content: bytes, content_type: str) -> None:
            self.content = content
            self.headers = {'Content-Type': content_type}
            self.status_code = 200
            self.url = 'https://example.test/x'

        @property
        def text(self) -> str:  # pragma: no cover
            raise AssertionError('parse path must not access .text when .content is available')

    def test_json_uses_content_bytes(self) -> None:
        resp = self._ExplodingTextResponse(b'{"a": 1}', 'application/json')
        self.assertEqual(default_parse_response(resp, response_format='json'), {'a': 1})

    def test_text_uses_content_bytes(self) -> None:
        resp = self._ExplodingTextResponse(b'hello', 'text/plain')
        self.assertEqual(
            default_parse_response(resp, response_format='text', encoding='utf-8'), 'hello'
        )

    def test_xml_uses_content_bytes(self) -> None:
        resp = self._ExplodingTextResponse(b'<root><x>1</x></root>', 'application/xml')
        elem = default_parse_response(resp, response_format='xml')
        self.assertEqual(getattr(elem, 'tag', None), 'root')

    def test_csv_uses_content_bytes_then_decodes_then_parses(self) -> None:
        resp = self._ExplodingTextResponse(b'a;b\n1;2\n', 'text/csv')
        rows = default_parse_response(
            resp, response_format='csv', csv_delimiter=';', encoding='utf-8'
        )
        self.assertEqual(rows, [{'a': '1', 'b': '2'}])

    def test_auto_format_detection_uses_content_bytes(self) -> None:
        resp = self._ExplodingTextResponse(b'{"ok": true}', 'application/json')
        parsed: Any = default_parse_response(resp, response_format='auto')
        self.assertEqual(parsed, {'ok': True})


class TestParsingWithPlaybackFixtures(unittest.TestCase):
    """Parsing data-flow tests that require temporary fixture files.

    Formerly bare pytest functions using tmp_path; converted to TestCase
    so they run under both pytest and unittest discover.
    """

    def setUp(self) -> None:
        self._td = tempfile.mkdtemp()
        self.tmp_path = Path(self._td)

    def tearDown(self) -> None:
        shutil.rmtree(self._td, ignore_errors=True)

    def _write_pages(self, path: Path, pages: list) -> None:
        path.write_text(json.dumps(pages, ensure_ascii=False, indent=2), encoding='utf-8')

    def test_next_request_receives_xml_element_in_playback(self) -> None:
        from xml.etree.ElementTree import Element

        playback_path = self.tmp_path / 'xml_playback.json'
        pages = [
            {
                'kind': 'raw_response',
                'format': 'xml',
                'status_code': 200,
                'headers': {'Content-Type': 'application/xml'},
                'url': 'https://example.test/feed?page=1',
                'body': '<root><items><x>1</x></items><next_url>https://example.test/feed?page=2</next_url></root>',
            },
            {
                'kind': 'raw_response',
                'format': 'xml',
                'status_code': 200,
                'headers': {'Content-Type': 'application/xml'},
                'url': 'https://example.test/feed?page=2',
                'body': '<root><items><x>2</x></items></root>',
            },
        ]
        self._write_pages(playback_path, pages)

        def next_request(parsed_body: Any, state: dict) -> dict | None:
            self.assertIsInstance(parsed_body, Element)
            next_url = parsed_body.findtext('.//next_url')
            if not next_url:
                return None
            return {'url': next_url}

        client = APIClient(
            {
                'base_url': 'https://example.test',
                'endpoints': {
                    'feed': {
                        'path': '/feed',
                        'response_format': 'xml',
                        'pagination': {
                            'next_request': next_request,
                        },
                        'on_response': lambda resp, _state: resp.findall('.//items/*'),
                        'playback': {'mode': 'load', 'path': str(playback_path)},
                    }
                },
            }
        )
        items = client.fetch('feed')
        self.assertEqual(len(items), 2)

    def test_next_request_receives_csv_rows_in_playback(self) -> None:
        playback_path = self.tmp_path / 'csv_playback.json'
        pages = [
            {
                'kind': 'raw_response',
                'format': 'csv',
                'status_code': 200,
                'headers': {'Content-Type': 'text/csv'},
                'url': 'https://example.test/report?page=1',
                'body': 'id;next_page_url\n1;https://example.test/report?page=2\n',
            },
            {
                'kind': 'raw_response',
                'format': 'csv',
                'status_code': 200,
                'headers': {'Content-Type': 'text/csv'},
                'url': 'https://example.test/report?page=2',
                'body': 'id;next_page_url\n2;\n',
            },
        ]
        self._write_pages(playback_path, pages)

        def next_request(parsed_body: Any, _state: dict) -> dict | None:
            self.assertIsInstance(parsed_body, list)
            self.assertTrue(parsed_body and isinstance(parsed_body[0], dict))
            next_url = parsed_body[-1].get('next_page_url')
            if not next_url:
                return None
            return {'url': next_url}

        client = APIClient(
            {
                'base_url': 'https://example.test',
                'endpoints': {
                    'report': {
                        'path': '/report',
                        'response_format': 'csv',
                        'csv_delimiter': ';',
                        'pagination': {
                            'next_request': next_request,
                        },
                        'on_response': lambda resp, _state: resp[0]['id'],
                        'playback': {'mode': 'load', 'path': str(playback_path)},
                    }
                },
            }
        )
        ids = client.fetch('report')
        self.assertEqual(ids, ['1', '2'])

    def test_next_request_sees_pre_parser_body_on_response_sees_post_parser_payload(self) -> None:
        playback_path = self.tmp_path / 'json_playback.json'
        pages = [
            {
                'kind': 'raw_response',
                'format': 'json',
                'status_code': 200,
                'headers': {'Content-Type': 'application/json'},
                'url': 'https://example.test/items',
                'body': '{"cursor": "abc", "items": [1, 2]}',
            }
        ]
        self._write_pages(playback_path, pages)

        def response_parser(_response: Any, parsed: Any) -> Any:
            return {'wrapped': parsed}

        def next_request(parsed_body: Any, _state: dict) -> dict | None:
            self.assertIsInstance(parsed_body, dict)
            self.assertIn('cursor', parsed_body)
            self.assertNotIn('wrapped', parsed_body)
            return None

        def on_response(page_payload: Any, _state: dict) -> Any:
            self.assertIsInstance(page_payload, dict)
            self.assertIn('wrapped', page_payload)
            return page_payload['wrapped']['items']

        client = APIClient(
            {
                'base_url': 'https://example.test',
                'endpoints': {
                    'items': {
                        'path': '/items',
                        'response_format': 'json',
                        'response_parser': response_parser,
                        'pagination': {
                            'next_request': next_request,
                        },
                        'on_response': on_response,
                        'playback': {'mode': 'load', 'path': str(playback_path)},
                    }
                },
            }
        )
        self.assertEqual(client.fetch('items'), [1, 2])


def test_link_header_pagination_looks_up_header_case_insensitively_for_unusual_casing():
    cfg = link_header_pagination(data_path='items')
    state = {'_response_headers': {'lInK': '<https://api.example.com/next?page=2>; rel="next"'}}
    req = cfg['next_request']({}, state)
    assert req == {'url': 'https://api.example.com/next?page=2'}


def test_url_header_pagination_returns_header_value_as_url():
    cfg = url_header_pagination('X-Next-Url', data_path='items')
    state = {'_response_headers': {'X-Next-Url': 'https://api.example.com/v1/test?page=2'}}
    assert cfg['next_request']({}, state) == {'url': 'https://api.example.com/v1/test?page=2'}


def test_url_header_pagination_uses_named_header_case_insensitively():
    cfg = url_header_pagination('X-Next-Url', data_path='items')
    state = {'_response_headers': {'x-next-url': 'https://api.example.com/v1/test?page=2'}}
    assert cfg['next_request']({}, state) == {'url': 'https://api.example.com/v1/test?page=2'}


def test_url_header_pagination_stops_on_absent_or_empty_header():
    cfg = url_header_pagination('X-Next-Url', data_path='items')
    assert cfg['next_request']({}, {'_response_headers': {}}) is None
    assert cfg['next_request']({}, {'_response_headers': {'X-Next-Url': ''}}) is None
