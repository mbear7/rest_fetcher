# Domain: example-backed and replay-backed integration coverage.
# Place new tests here by primary behavioral contract when the assertion is
# that documented examples or replay scenarios work as user-facing reference
# behavior. Prefer this existing domain file for that work. If a test spans
# multiple areas, place it where the main assertion belongs.

import unittest
from pathlib import Path
from typing import Any

from rest_fetcher import APIClient
from rest_fetcher.pagination import (
    cursor_pagination,
    link_header_pagination,
    offset_pagination,
    page_number_pagination,
)

FIXTURES = Path(__file__).resolve().parent.parent / 'examples' / 'fixtures'


class TestExampleReplayScenarios(unittest.TestCase):
    def make_client(
        self,
        endpoint_name: str,
        endpoint_config: dict[str, Any],
        *,
        base_url: str = 'https://api.example.com/v1',
        auth: dict[str, Any] | None = None,
    ) -> APIClient:
        schema: dict[str, Any] = {
            'base_url': base_url,
            'endpoints': {endpoint_name: endpoint_config},
        }
        if auth is not None:
            schema['auth'] = auth
        return APIClient(schema)

    def playback_config(self, fixture_name: str) -> dict[str, str]:
        return {'path': str(FIXTURES / fixture_name), 'mode': 'load'}

    def test_example_02_cursor_pagination_replay(self):
        client = self.make_client(
            'list_events',
            {
                'method': 'GET',
                'path': '/events',
                'params': {'page_size': 200},
                'playback': self.playback_config('example_02_list_events.json'),
                'pagination': cursor_pagination(
                    cursor_param='cursor',
                    next_cursor_path='meta.next_cursor',
                    data_path='items',
                ),
            },
            auth={'type': 'bearer', 'token': 'my-secret-token'},
        )
        pages = client.fetch('list_events')
        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0][0]['id'], 'evt_1001')
        self.assertEqual(pages[1][0]['id'], 'evt_1003')

    def test_example_19_link_header_pagination_replay(self):
        client = self.make_client(
            'list_repos',
            {
                'method': 'GET',
                'path': '/orgs/acme/repos',
                'params': {'per_page': 100},
                'playback': self.playback_config('example_19_link_header_repos.json'),
                'pagination': link_header_pagination(data_path='items'),
            },
            base_url='https://api.github.com',
        )
        pages = client.fetch('list_repos')
        self.assertEqual(len(pages), 2)
        self.assertEqual(len(pages[0]), 2)
        self.assertEqual(pages[1][0]['name'], 'repo-c')

    def test_example_36_xml_replay(self):
        client = self.make_client(
            'feed',
            {
                'method': 'GET',
                'path': '/feed.xml',
                'response_format': 'xml',
                'playback': self.playback_config('example_36_xml_feed.json'),
            },
            base_url='https://example.com',
        )
        root = client.fetch('feed')
        self.assertEqual(root.tag, 'feed')
        self.assertEqual(root.findtext('.//title'), 'Example Feed')

    def test_example_37_csv_replay(self):
        client = self.make_client(
            'report',
            {
                'method': 'GET',
                'path': '/report.csv',
                'response_format': 'csv',
                'playback': self.playback_config('example_37_csv_report.json'),
            },
            base_url='https://example.com',
        )
        rows = client.fetch('report')
        self.assertEqual(rows[0]['name'], 'Alice')
        self.assertEqual(rows[1]['id'], '2')

    def test_example_38_nested_pagination_replay(self):
        client = self.make_client(
            'nested_users',
            {
                'method': 'GET',
                'path': '/users',
                'playback': self.playback_config('example_38_nested_pagination.json'),
                'pagination': offset_pagination(
                    limit=100,
                    data_path='payload.items',
                    total_path='meta.total',
                ),
            },
        )
        pages = client.fetch('nested_users')
        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0][1]['id'], 2)
        self.assertEqual(pages[1][0]['id'], 3)

    def test_example_39_callback_debugging_replay(self):
        def extract_names(resp, state):
            return [item['name'] for item in resp.get('items', [])]

        def debug_update_state(resp, state):
            return {'offset': state.get('offset', 0) + len(resp.get('items', []))}

        def debug_next_request(resp, state):
            current_offset = state.get('offset', 0)
            total = ((resp.get('meta') or {}).get('total')) or 0
            if current_offset >= total:
                return None
            return {'params': {'offset': current_offset, 'limit': 100}}

        client = self.make_client(
            'users_debug',
            {
                'method': 'GET',
                'path': '/users',
                'playback': self.playback_config('example_39_callback_debugging.json'),
                'pagination': {
                    'initial_params': {'offset': 0, 'limit': 100},
                    'next_request': debug_next_request,
                },
                'on_response': extract_names,
                'update_state': debug_update_state,
            },
        )
        pages = client.fetch('users_debug')
        self.assertEqual(pages, [['Alice', 'Bob'], ['Cara']])

    def test_example_05_custom_pagination_state_replay(self):
        def update_state(resp, state):
            return {'offset': state.get('offset', 0) + len(resp.get('data', []))}

        def next_request(resp, state):
            offset = state.get('offset', 0)
            if resp.get('data') is None or offset >= resp.get('total_rows', 0):
                return None
            return {'params': {'offset': offset}}

        client = self.make_client(
            'report',
            {
                'method': 'GET',
                'path': '/stat/v1/data',
                'playback': self.playback_config('example_05_custom_pagination_state.json'),
                'pagination': {
                    'next_request': next_request,
                },
                'update_state': update_state,
                'on_response': lambda resp, state: resp.get('data', []),
                'on_complete': lambda pages, state: [row for page in pages for row in page],
            },
            base_url='https://api-metrika.yandex.net',
            auth={'type': 'callback', 'handler': lambda req, config: req},
        )
        result = client.fetch('report')
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['date'], '2025-01-01')
        self.assertEqual(result[2]['date'], '2025-01-03')

    def test_example_18_offset_pagination_replay(self):
        client = self.make_client(
            'users',
            {
                'method': 'GET',
                'path': '/users',
                'playback': self.playback_config('example_18_offset_users.json'),
                'pagination': offset_pagination(
                    offset_param='offset',
                    limit_param='limit',
                    limit=100,
                    data_path='data',
                    total_path='total',
                ),
            },
            auth={'type': 'bearer', 'token': 'my-token'},
        )
        pages = client.fetch('users')
        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0][0]['id'], 1)
        self.assertEqual(pages[0][1]['id'], 2)
        self.assertEqual(pages[1][0]['id'], 3)

    def test_example_18_page_number_pagination_replay(self):
        client = self.make_client(
            'posts',
            {
                'method': 'GET',
                'path': '/posts',
                'playback': self.playback_config('example_18_page_number_posts.json'),
                'pagination': page_number_pagination(
                    page_param='page',
                    page_size_param='per_page',
                    page_size=50,
                    data_path='results',
                    total_pages_path='meta.total_pages',
                ),
            },
            auth={'type': 'bearer', 'token': 'my-token'},
        )
        pages = client.fetch('posts')
        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0][0]['id'], 'p1')
        self.assertEqual(pages[1][0]['id'], 'p3')

    def test_example_26_playback_users_replay(self):
        client = self.make_client(
            'users',
            {
                'method': 'GET',
                'path': '/users',
                'playback': self.playback_config('example_26_playback_users.json'),
            },
            auth={'type': 'bearer', 'token': 'my-token'},
        )
        users = client.fetch('users')
        self.assertEqual(len(users), 2)
        self.assertEqual(users[0]['name'], 'Alice')
        self.assertEqual(users[1]['id'], 2)

    def test_example_35_text_format_replay(self):
        client = self.make_client(
            'robots',
            {
                'method': 'GET',
                'path': '/robots.txt',
                'response_format': 'text',
                'playback': self.playback_config('example_35_text_status.json'),
            },
            base_url='https://example.com',
        )
        result = client.fetch('robots')
        self.assertIsInstance(result, str)
        self.assertIn('User-agent: *', result)
        self.assertIn('Disallow: /private/', result)
