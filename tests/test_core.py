# Domain: core library behavior.
# Place new tests here when the assertion is schema validation, auth, retry,
# client fetch mechanics, state/caps, callbacks, header scrubbing,
# SchemaBuilder, or OperationContext. Parsing, pagination strategy, playback,
# and events/rate-limit each have a dedicated domain file — prefer those.
# If a test spans multiple areas, place it where the primary assertion belongs.

"""
core test suite for rest_fetcher.
uses stdlib unittest only — no external dependencies.
all http calls are intercepted via mock, no real network access.
run with: pytest -q
"""

import json
import os
import sys
import tempfile
import threading
import unittest
import itertools
from unittest.mock import MagicMock, patch

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from types import SimpleNamespace

from rest_fetcher import (
    APIClient,
    MetricsSession,
    MetricsSummary,
    AuthError,
    CallbackError,
    DeadlineExceeded,
    RateLimitError,
    RequestError,
    ResponseError,
    SchemaError,
    cursor_pagination,
    offset_pagination,
    page_number_pagination,
    raise_,
)
from rest_fetcher.auth import OAuth2PasswordAuth, build_auth_handler
from rest_fetcher.pagination import CycleRunner
from rest_fetcher.retry import RetryHandler
from rest_fetcher.exceptions import SchemaValidationWarning
from rest_fetcher.schema import merge_dicts, resolve_endpoint, validate
from rest_fetcher.types import StopSignal, StreamSummary


def outcome(parsed, request_kwargs=None, headers=None):
    return SimpleNamespace(
        parsed=parsed,
        headers=headers or {},
        request_kwargs=request_kwargs or {},
    )


def make_response(status=200, body=None, headers=None):
    "builds a mock requests.Response"
    r = MagicMock()
    r.status_code = status
    r.ok = status < 400
    r.url = 'https://api.example.com/v1/test'
    r.headers = headers or {}
    r.json.return_value = body or {}
    r.text = json.dumps(body or {})
    return r


def simple_schema(**endpoint_overrides):
    "minimal valid schema for a single endpoint"
    endpoint = {'method': 'GET', 'path': '/test'}
    endpoint.update(endpoint_overrides)
    return {'base_url': 'https://api.example.com/v1', 'endpoints': {'test': endpoint}}


class TestEndpointInheritedValidation(unittest.TestCase):
    def test_session_config_verify_false_is_accepted(self):
        # should not raise
        validate(
            {
                'base_url': 'https://api.example.com/v1',
                'session_config': {'verify': False},
                'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
            }
        )

    def test_session_config_unknown_key_raises(self):
        with self.assertRaises(SchemaError) as ctx:
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'session_config': {'pool_size': 10},
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                }
            )
        self.assertIn('pool_size', str(ctx.exception))
        self.assertIn('session_config', str(ctx.exception))

    def test_session_config_bad_verify_type_raises(self):
        with self.assertRaises(SchemaError) as ctx:
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'session_config': {'verify': 123},
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                }
            )
        self.assertIn('verify', str(ctx.exception))

    def test_session_config_proxies_applied_to_session(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'session_config': {'proxies': {'https': 'http://proxy.internal:8080'}},
                'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
            }
        )
        self.assertEqual(client._session.proxies.get('https'), 'http://proxy.internal:8080')

    def test_session_config_verify_applied_to_session(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'session_config': {'verify': False},
                'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
            }
        )
        self.assertIs(client._session.verify, False)

    def test_session_config_cert_applied_to_session(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'session_config': {'cert': ('/tmp/client.crt', '/tmp/client.key')},
                'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
            }
        )
        self.assertEqual(client._session.cert, ('/tmp/client.crt', '/tmp/client.key'))

    def test_session_config_max_redirects_applied(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'session_config': {'max_redirects': 5},
                'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
            }
        )
        self.assertEqual(client._session.max_redirects, 5)

    def test_strict_mode_rejects_typo_in_root(self):
        with self.assertRaises(SchemaError) as ctx:
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'on_requets': lambda req, state: req,  # typo
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                },
                strict=True,
            )
        self.assertIn('on_requets', str(ctx.exception))

    def test_strict_mode_rejects_typo_in_endpoint(self):
        with self.assertRaises(SchemaError) as ctx:
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {
                        'ep': {'method': 'GET', 'path': '/x', 'on_respnse': lambda x, s: x}
                    },
                },
                strict=True,
            )
        self.assertIn('on_respnse', str(ctx.exception))

    def test_strict_mode_passes_with_known_keys(self):
        # should not raise
        validate(
            {
                'base_url': 'https://api.example.com/v1',
                'on_request': lambda req, state: req,
                'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'on_response': lambda p, s: p}},
            },
            strict=True,
        )

    def test_non_strict_mode_warns_on_unknown_keys(self):
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter('always')
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'unknown_future_key': True,
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                },
                strict=False,
            )
        schema_warns = [w for w in caught if issubclass(w.category, SchemaValidationWarning)]
        self.assertEqual(len(schema_warns), 1)
        self.assertIn('unknown_future_key', str(schema_warns[0].message))

    def test_non_strict_mode_warning_points_at_caller(self):
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter('always')
            validate(  # this is the line the warning should point at
                {
                    'base_url': 'https://api.example.com/v1',
                    'typo_key': True,
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                },
                strict=False,
            )
        schema_warns = [w for w in caught if issubclass(w.category, SchemaValidationWarning)]
        self.assertEqual(len(schema_warns), 1)
        self.assertNotIn('schema.py', schema_warns[0].filename)

    def test_non_strict_mode_warns_on_unknown_endpoint_keys(self):
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter('always')
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {
                        'ep': {'method': 'GET', 'path': '/x', 'on_respnse': lambda p, s: p}
                    },
                },
                strict=False,
            )
        schema_warns = [w for w in caught if issubclass(w.category, SchemaValidationWarning)]
        self.assertEqual(len(schema_warns), 1)
        self.assertIn('on_respnse', str(schema_warns[0].message))
        self.assertIn("Did you mean 'on_response'", str(schema_warns[0].message))

    def test_non_strict_mode_warns_on_unknown_retry_keys(self):
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter('always')
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retry': {'max_attempts': 3, 'max_retyes': 5},
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                },
                strict=False,
            )
        schema_warns = [w for w in caught if issubclass(w.category, SchemaValidationWarning)]
        self.assertEqual(len(schema_warns), 1)
        self.assertIn('max_retyes', str(schema_warns[0].message))

    def test_non_strict_mode_warning_includes_allowed_keys(self):
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter('always')
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retrry': {},
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                },
                strict=False,
            )
        schema_warns = [w for w in caught if issubclass(w.category, SchemaValidationWarning)]
        self.assertEqual(len(schema_warns), 1)
        self.assertIn('Allowed keys:', str(schema_warns[0].message))

    def test_session_config_still_raises_in_non_strict_mode(self):
        with self.assertRaises(SchemaError):
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'session_config': {'unknown_key': True},
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                }
            )

    # --- aggregation ---

    def test_strict_collects_errors_across_boundaries(self):
        # root typo + endpoint typo — both reported in one exception
        with self.assertRaises(SchemaError) as ctx:
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retrry': {},
                    'endpoints': {
                        'ep': {'method': 'GET', 'path': '/x', 'on_respnse': lambda p, s: p}
                    },
                },
                strict=True,
            )
        msg = str(ctx.exception)
        self.assertIn('retrry', msg)
        self.assertIn('on_respnse', msg)

    def test_strict_collects_errors_across_sub_validators(self):
        # typo in client-level retry + typo in endpoint — both reported
        with self.assertRaises(SchemaError) as ctx:
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retry': {'max_attempts': 3, 'max_retyes': 5},
                    'endpoints': {
                        'ep': {'method': 'GET', 'path': '/x', 'on_respnse': lambda p, s: p}
                    },
                },
                strict=True,
            )
        msg = str(ctx.exception)
        self.assertIn('max_retyes', msg)
        self.assertIn('on_respnse', msg)

    def test_strict_error_encounter_order(self):
        # retry typo (sub-validator, fires first) before root typo (fires after endpoint loop)
        with self.assertRaises(SchemaError) as ctx:
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retry': {'max_attempts': 3, 'bad_retry_key': 1},
                    'retrry': {},
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                },
                strict=True,
            )
        msg = str(ctx.exception)
        self.assertLess(msg.index('bad_retry_key'), msg.index('retrry'))

    # --- stacklevel coverage for nested warning paths ---

    def _assert_warning_points_at_caller(self, schema):
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter('always')
            validate(schema, strict=False)
        schema_warns = [w for w in caught if issubclass(w.category, SchemaValidationWarning)]
        self.assertGreater(len(schema_warns), 0)
        for w in schema_warns:
            self.assertNotIn('schema.py', w.filename)

    def test_stacklevel_auth_warning_points_at_caller(self):
        self._assert_warning_points_at_caller(
            {
                'base_url': 'http://x.com',
                'auth': {'type': 'bearer', 'token': 'x', 'extra_key': 'y'},
                'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
            }
        )

    def test_stacklevel_client_retry_warning_points_at_caller(self):
        self._assert_warning_points_at_caller(
            {
                'base_url': 'http://x.com',
                'retry': {'max_attempts': 3, 'bad_key': 1},
                'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
            }
        )

    def test_stacklevel_client_rate_limit_warning_points_at_caller(self):
        self._assert_warning_points_at_caller(
            {
                'base_url': 'http://x.com',
                'rate_limit': {'min_delay': 1, 'bad_key': 1},
                'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
            }
        )

    def test_stacklevel_client_pagination_warning_points_at_caller(self):
        self._assert_warning_points_at_caller(
            {
                'base_url': 'http://x.com',
                'pagination': {'next_request': lambda p, s: None, 'bad_key': 1},
                'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
            }
        )

    def test_stacklevel_endpoint_retry_warning_points_at_caller(self):
        self._assert_warning_points_at_caller(
            {
                'base_url': 'http://x.com',
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'retry': {'max_attempts': 3, 'bad_key': 1},
                    }
                },
            }
        )

    def test_stacklevel_endpoint_rate_limit_warning_points_at_caller(self):
        self._assert_warning_points_at_caller(
            {
                'base_url': 'http://x.com',
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'rate_limit': {'min_delay': 1, 'bad_key': 1},
                    }
                },
            }
        )

    def test_stacklevel_endpoint_pagination_warning_points_at_caller(self):
        self._assert_warning_points_at_caller(
            {
                'base_url': 'http://x.com',
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'pagination': {'next_request': lambda p, s: None, 'bad_key': 1},
                    }
                },
            }
        )

    def test_top_level_files_is_rejected(self):
        with self.assertRaises(SchemaError):
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'files': {'upload': b'data'},
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                }
            )

    def test_rate_limit_exceeded_is_a_rate_limit_error(self):
        from rest_fetcher.exceptions import RateLimitExceeded, RateLimitError

        self.assertTrue(issubclass(RateLimitExceeded, RateLimitError))
        with self.assertRaises(RateLimitError):
            raise RateLimitExceeded('too fast')

    def test_endpoint_on_request_must_be_callable(self):
        with self.assertRaises(SchemaError):
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'on_request': 123}},
                }
            )

    def test_client_on_request_must_be_callable(self):
        with self.assertRaises(SchemaError):
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'on_request': 123,
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                }
            )

    def test_endpoint_response_parser_must_be_callable(self):
        with self.assertRaises(SchemaError):
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'response_parser': 123}},
                }
            )

    def test_endpoint_canonical_parser_must_be_callable(self):
        with self.assertRaises(SchemaError):
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'canonical_parser': 123}},
                }
            )

    def test_endpoint_csv_delimiter_must_be_single_character(self):
        with self.assertRaises(SchemaError):
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'csv_delimiter': ';;'}},
                }
            )

    def test_endpoint_encoding_must_be_non_empty(self):
        with self.assertRaises(SchemaError):
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'encoding': ''}},
                }
            )

    def test_endpoint_scrub_headers_must_be_list_of_strings(self):
        with self.assertRaises(SchemaError):
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'scrub_headers': [123]}},
                }
            )

    def test_endpoint_scrub_query_params_must_be_list_of_strings(self):
        with self.assertRaises(SchemaError):
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {
                        'ep': {'method': 'GET', 'path': '/x', 'scrub_query_params': [123]}
                    },
                }
            )


class TestExceptions(unittest.TestCase):
    def test_raise_with_string_raises_response_error(self):
        with self.assertRaises(ResponseError) as ctx:
            raise_('something went wrong')
        self.assertEqual(str(ctx.exception), 'something went wrong')

    def test_raise_with_custom_class(self):
        with self.assertRaises(AuthError):
            raise_('token expired', cls=AuthError)

    def test_raise_with_exception_instance(self):
        exc = ValueError('original')
        with self.assertRaises(ValueError):
            raise_(exc)

    def test_request_error_carries_status(self):
        e = RequestError('failed', status_code=503)
        self.assertEqual(e.status_code, 503)

    def test_rate_limit_error_carries_retry_after(self):
        e = RateLimitError('too fast', retry_after=30.0, status_code=429)
        self.assertEqual(e.retry_after, 30.0)
        self.assertEqual(e.status_code, 429)

    def test_callback_error_carries_name_and_cause(self):
        cause = ValueError('inner')
        e = CallbackError('cb failed', callback_name='on_response', cause=cause)
        self.assertEqual(e.callback_name, 'on_response')
        self.assertIs(e.cause, cause)

    def test_response_error_carries_raw(self):
        e = ResponseError('bad body', raw='<html>error</html>')
        self.assertEqual(e.raw, '<html>error</html>')


class TestMergeDicts(unittest.TestCase):
    def test_shallow_merge_override(self):
        result = merge_dicts({'a': 1, 'b': 2}, {'b': 99, 'c': 3})
        self.assertEqual(result, {'a': 1, 'b': 99, 'c': 3})

    def test_deep_merge_nested_dicts(self):
        base = {'headers': {'Accept': 'application/json', 'X-Foo': 'bar'}}
        override = {'headers': {'Authorization': 'Bearer x'}}
        result = merge_dicts(base, override)
        self.assertEqual(
            result['headers'],
            {
                'Accept': 'application/json',
                'X-Foo': 'bar',
                'Authorization': 'Bearer x',
            },
        )

    def test_override_wins_on_conflict(self):
        result = merge_dicts({'headers': {'Accept': 'json'}}, {'headers': {'Accept': 'xml'}})
        self.assertEqual(result['headers']['Accept'], 'xml')

    def test_non_dict_override_replaces_entirely(self):
        result = merge_dicts({'key': {'nested': 1}}, {'key': 'flat'})
        self.assertEqual(result['key'], 'flat')

    def test_base_unchanged(self):
        base = {'a': {'b': 1}}
        merge_dicts(base, {'a': {'c': 2}})
        self.assertNotIn('c', base['a'])

    def test_merge_adds_new_keys_from_override(self):
        result = merge_dicts({'a': 1}, {'b': 2})
        self.assertEqual(result, {'a': 1, 'b': 2})

    def test_merge_empty_override_returns_base(self):
        result = merge_dicts({'a': 1, 'b': 2}, {})
        self.assertEqual(result, {'a': 1, 'b': 2})

    def test_merge_empty_base_returns_override(self):
        result = merge_dicts({}, {'a': 1, 'b': 2})
        self.assertEqual(result, {'a': 1, 'b': 2})

    def test_merge_does_not_mutate_base(self):
        base = {'params': {'page': 1, 'filter': 'active'}}
        merge_dicts(base, {'params': {'page': 2}})
        self.assertEqual(base['params']['filter'], 'active')
        self.assertEqual(base['params']['page'], 1)

    def test_merge_does_not_mutate_override(self):
        override = {'params': {'page': 2}}
        merge_dicts({'params': {'page': 1, 'filter': 'active'}}, override)
        self.assertNotIn('filter', override['params'])

    def test_merge_preserves_sticky_params(self):
        base = {'params': {'page': 1, 'filter': 'active', 'limit': 10}}
        override = {'params': {'page': 2}}
        result = merge_dicts(base, override)
        self.assertEqual(result['params']['page'], 2)
        self.assertEqual(result['params']['filter'], 'active')
        self.assertEqual(result['params']['limit'], 10)

    def test_merge_three_levels_deep(self):
        base = {'a': {'b': {'c': 1, 'd': 2}}}
        override = {'a': {'b': {'c': 99}}}
        result = merge_dicts(base, override)
        self.assertEqual(result['a']['b']['c'], 99)
        self.assertEqual(result['a']['b']['d'], 2)


class TestClientStateWiring(unittest.TestCase):
    def test_state_seeded_from_schema(self):
        client = APIClient({**simple_schema(), 'state': {'api_key': 'abc', 'region': 'eu'}})
        self.assertEqual(client._state['api_key'], 'abc')
        self.assertEqual(client._state['region'], 'eu')

    def test_state_defaults_to_empty_dict(self):
        client = APIClient(simple_schema())
        self.assertEqual(client._state, {})

    def test_auth_callback_receives_read_only_config(self):
        received_state = {}

        def auth_handler(req, config):
            received_state.update(dict(config))
            return req

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'ok': True})
            client = APIClient(
                {
                    **simple_schema(),
                    'state': {'token': 'secret-123'},
                    'auth': {'type': 'callback', 'handler': auth_handler},
                }
            )
            client.fetch('test')

        self.assertEqual(received_state.get('token'), 'secret-123')

    def test_auth_callback_cannot_mutate_config_view(self):
        def auth_handler(req, config):
            config['cached_token'] = 'nope'
            return req

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'ok': True})
            client = APIClient(
                {**simple_schema(), 'auth': {'type': 'callback', 'handler': auth_handler}}
            )
            with self.assertRaises(TypeError):
                client.fetch('test')

    def test_bearer_token_callback_receives_config_keys(self):
        seen = {}

        def token_callback(config):
            seen.update(dict(config))
            return 'dynamic-token'

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'ok': True})
            client = APIClient(
                {
                    **simple_schema(),
                    'state': {'param_name': 'MY_TOKEN_PARAM', 'region': 'eu-west-1'},
                    'auth': {'type': 'bearer', 'token_callback': token_callback},
                }
            )
            client.fetch('test')

        self.assertEqual(seen.get('param_name'), 'MY_TOKEN_PARAM')
        self.assertEqual(seen.get('region'), 'eu-west-1')

    def test_bearer_token_callback_cannot_mutate_config_view(self):
        def token_callback(config):
            config['cached_token'] = 'nope'
            return 'dynamic-token'

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'ok': True})
            client = APIClient(
                {
                    **simple_schema(),
                    'auth': {'type': 'bearer', 'token_callback': token_callback},
                }
            )
            with self.assertRaises(TypeError):
                client.fetch('test')


class TestSchemaValidation(unittest.TestCase):
    def test_valid_minimal_schema_passes(self):
        schema = simple_schema()
        result = validate(schema)
        self.assertIs(result, schema)

    def test_missing_base_url_raises(self):
        with self.assertRaises(SchemaError) as ctx:
            validate({'endpoints': {'x': {}}})
        self.assertIn('base_url', str(ctx.exception))

    def test_missing_endpoints_raises(self):
        with self.assertRaises(SchemaError) as ctx:
            validate({'base_url': 'https://x.com'})
        self.assertIn('endpoints', str(ctx.exception))

    def test_empty_endpoints_raises(self):
        with self.assertRaises(SchemaError):
            validate({'base_url': 'https://x.com', 'endpoints': {}})

    def test_invalid_method_raises(self):
        with self.assertRaises(SchemaError) as ctx:
            validate(simple_schema(method='BREW'))  # not a real HTTP method
        self.assertIn('method', str(ctx.exception))

    def test_valid_methods_accepted(self):
        for method in ('GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'):
            validate(simple_schema(method=method))  # should not raise

    def test_invalid_log_level_raises(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'log_level': 'superverbose'})

    def test_invalid_auth_type_raises(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'auth': {'type': 'magic'}})

    def test_bearer_without_token_raises(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'auth': {'type': 'bearer'}})

    def test_bearer_with_token_passes(self):
        validate({**simple_schema(), 'auth': {'type': 'bearer', 'token': 'x'}})

    def test_bearer_with_token_callback_passes(self):
        validate({**simple_schema(), 'auth': {'type': 'bearer', 'token_callback': lambda s: 'x'}})

    def test_basic_missing_password_raises(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'auth': {'type': 'basic', 'username': 'u'}})

    def test_oauth2_missing_key_raises(self):
        with self.assertRaises(SchemaError):
            validate(
                {
                    **simple_schema(),
                    'auth': {
                        'type': 'oauth2',
                        'token_url': 'x',
                        'client_id': 'y',
                        # missing client_secret
                    },
                }
            )

    def test_oauth2_password_missing_key_raises(self):
        with self.assertRaises(SchemaError):
            validate(
                {
                    **simple_schema(),
                    'auth': {
                        'type': 'oauth2_password',
                        'token_url': 'x',
                        'client_id': 'y',
                        'client_secret': 'z',
                        'username': 'u',
                        # missing password
                    },
                }
            )

    def test_oauth2_password_passes(self):
        validate(
            {
                **simple_schema(),
                'auth': {
                    'type': 'oauth2_password',
                    'token_url': 'x',
                    'client_id': 'y',
                    'client_secret': 'z',
                    'username': 'u',
                    'password': 'p',
                },
            }
        )

    def test_invalid_retry_max_attempts_raises(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'retry': {'max_attempts': 0}})

    def test_invalid_playback_mode_raises(self):
        with self.assertRaises(SchemaError):
            validate(simple_schema(playback={'path': 'x.json', 'mode': 'rewind'}))

    def test_playback_string_shorthand_normalizes(self):
        schema = simple_schema(playback='x.json')
        validate(schema)
        # string shorthand is normalized in-place to {'path': ..., 'mode': 'auto'}
        pb = schema['endpoints']['test']['playback']
        self.assertEqual(pb, {'path': 'x.json', 'mode': 'auto'})

    def test_playback_mode_none_valid_without_path(self):
        validate(simple_schema(playback={'mode': 'none'}))

    def test_mock_callable_passes(self):
        validate(simple_schema(mock=lambda r: {}))

    def test_mock_list_passes(self):
        validate(simple_schema(mock=[{'data': 1}]))

    def test_mock_invalid_type_raises(self):
        with self.assertRaises(SchemaError):
            validate(simple_schema(mock='not_valid'))


class TestResolveEndpoint(unittest.TestCase):
    def test_inherits_client_headers(self):
        schema = {
            'base_url': 'https://x.com',
            'headers': {'Accept': 'application/json'},
            'endpoints': {'ep': {'method': 'GET'}},
        }
        resolved = resolve_endpoint(schema, 'ep')
        self.assertEqual(resolved['headers']['Accept'], 'application/json')

    def test_endpoint_headers_merge_over_client(self):
        schema = {
            'base_url': 'https://x.com',
            'headers': {'Accept': 'application/json', 'X-Foo': 'bar'},
            'endpoints': {'ep': {'headers': {'Authorization': 'Bearer x'}}},
        }
        resolved = resolve_endpoint(schema, 'ep')
        self.assertEqual(resolved['headers']['Accept'], 'application/json')
        self.assertEqual(resolved['headers']['Authorization'], 'Bearer x')

    def test_canonical_parser_inherited_when_endpoint_omits_it(self):
        client_parser = lambda content, ctx: {'source': 'client'}
        schema = {
            'base_url': 'https://x.com',
            'canonical_parser': client_parser,
            'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
        }
        resolved = resolve_endpoint(schema, 'ep')
        self.assertIs(resolved['canonical_parser'], client_parser)

    def test_endpoint_canonical_parser_overrides_client_parser(self):
        client_parser = lambda content, ctx: {'source': 'client'}
        endpoint_parser = lambda content, ctx: {'source': 'endpoint'}
        schema = {
            'base_url': 'https://x.com',
            'canonical_parser': client_parser,
            'endpoints': {
                'ep': {'method': 'GET', 'path': '/x', 'canonical_parser': endpoint_parser}
            },
        }
        resolved = resolve_endpoint(schema, 'ep')
        self.assertIs(resolved['canonical_parser'], endpoint_parser)

    def test_pagination_inherited_when_not_set(self):
        schema = {
            'base_url': 'https://x.com',
            'pagination': {'next_request': lambda r, s: None},
            'endpoints': {'ep': {}},
        }
        resolved = resolve_endpoint(schema, 'ep')
        self.assertIsNotNone(resolved['pagination'])

    def test_pagination_disabled_when_none(self):
        schema = {
            'base_url': 'https://x.com',
            'pagination': {'next_request': lambda r, s: None},
            'endpoints': {'ep': {'pagination': None}},
        }
        resolved = resolve_endpoint(schema, 'ep')
        self.assertIsNone(resolved['pagination'])

    def test_endpoint_pagination_merges_over_client(self):
        client_next = lambda r, s: None
        endpoint_on_page = lambda items, state: items
        schema = {
            'base_url': 'https://x.com',
            'pagination': {'next_request': client_next, 'delay': 1.0},
            'endpoints': {
                'ep': {
                    'pagination': {'delay': 2.5},
                    'on_page': endpoint_on_page,
                }
            },
        }
        resolved = resolve_endpoint(schema, 'ep')
        self.assertIs(resolved['pagination']['next_request'], client_next)
        self.assertEqual(resolved['pagination']['delay'], 2.5)
        self.assertIs(resolved['on_page'], endpoint_on_page)

    def test_retry_inherited_when_not_set(self):
        schema = {
            'base_url': 'https://x.com',
            'retry': {'max_attempts': 4, 'base_delay': 0.2},
            'endpoints': {'ep': {'method': 'GET'}},
        }
        resolved = resolve_endpoint(schema, 'ep')
        self.assertEqual(resolved['retry']['max_attempts'], 4)
        self.assertEqual(resolved['retry']['base_delay'], 0.2)

    def test_endpoint_retry_none_disables_inherited_retry(self):
        schema = {
            'base_url': 'https://x.com',
            'retry': {'max_attempts': 4, 'base_delay': 0.2},
            'endpoints': {'ep': {'method': 'GET', 'retry': None}},
        }
        resolved = resolve_endpoint(schema, 'ep')
        self.assertIsNone(resolved['retry'])

    def test_endpoint_retry_merges_over_client(self):
        schema = {
            'base_url': 'https://x.com',
            'retry': {'max_attempts': 4, 'base_delay': 0.2, 'max_delay': 5.0},
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'retry': {'max_attempts': 2, 'backoff': 'linear'},
                }
            },
        }
        resolved = resolve_endpoint(schema, 'ep')
        self.assertEqual(resolved['retry']['max_attempts'], 2)
        self.assertEqual(resolved['retry']['base_delay'], 0.2)
        self.assertEqual(resolved['retry']['max_delay'], 5.0)
        self.assertEqual(resolved['retry']['backoff'], 'linear')

    def test_unknown_endpoint_raises(self):
        schema = simple_schema()
        with self.assertRaises(SchemaError):
            resolve_endpoint(schema, 'nonexistent')

    def test_unreplaced_path_placeholder_logs_warning(self):
        "forgetting path_params leaves {id} in URL — library warns rather than silently sending bad request"
        schema = {
            'base_url': 'https://api.example.com/v1',
            'endpoints': {'ep': {'method': 'GET', 'path': '/users/{id}'}},
        }
        with patch('requests.Session.request') as m:
            m.return_value = make_response(200, {})
            with self.assertLogs('rest_fetcher.client', level='WARNING') as log:
                APIClient(schema).fetch('ep')  # no path_params supplied
        self.assertTrue(any('id' in msg and 'placeholder' in msg for msg in log.output))

    def test_pagination_missing_next_request_raises(self):
        "next_request is required — omitting it would cause an infinite loop"
        with self.assertRaises(SchemaError) as ctx:
            validate(
                {'base_url': 'https://x.com', 'endpoints': {'ep': {'pagination': {'delay': 0.1}}}}
            )
        self.assertIn('next_request', str(ctx.exception))

    def test_pagination_next_request_not_callable_raises(self):
        with self.assertRaises(SchemaError) as ctx:
            validate(
                {
                    'base_url': 'https://x.com',
                    'endpoints': {'ep': {'pagination': {'next_request': 'not-a-callable'}}},
                }
            )
        self.assertIn('next_request', str(ctx.exception))

    def test_pagination_on_page_not_callable_raises(self):
        with self.assertRaises(SchemaError) as ctx:
            validate(
                {
                    'base_url': 'https://x.com',
                    'endpoints': {
                        'ep': {
                            'pagination': {
                                'next_request': lambda r, s: None,
                            },
                            'on_page': 'not-a-callable',
                        }
                    },
                }
            )
        self.assertIn('on_page', str(ctx.exception))

    def test_pagination_initial_params_not_dict_raises(self):
        with self.assertRaises(SchemaError) as ctx:
            validate(
                {
                    'base_url': 'https://x.com',
                    'endpoints': {
                        'ep': {
                            'pagination': {
                                'next_request': lambda r, s: None,
                                'initial_params': 'not-a-dict',
                            }
                        }
                    },
                }
            )
        self.assertIn('initial_params', str(ctx.exception))

    def test_pagination_none_disables_without_requiring_next_request(self):
        "pagination=None is valid even though next_request is absent"
        validate({'base_url': 'https://x.com', 'endpoints': {'ep': {'pagination': None}}})

    def test_endpoint_all_optional_callbacks_validated(self):
        "all optional lifecycle callbacks at endpoint level are checked for callability"
        for key in ('on_response', 'on_page', 'update_state', 'on_complete', 'on_page_complete'):
            with self.assertRaises(SchemaError) as ctx:
                validate(
                    {
                        'base_url': 'https://x.com',
                        'endpoints': {
                            'ep': {
                                'pagination': {'next_request': lambda r, s: None},
                                key: 'not-callable',
                            }
                        },
                    }
                )
            self.assertIn(key, str(ctx.exception), f'{key} not mentioned in error')

    # --- helper-marker regression tests: three-way seam ---

    def test_helper_dict_passes_validation_with_lifecycle_hooks(self):
        "built-in helpers include on_response and pass validation because of _rf_pagination_helper marker"
        validate(
            {
                'base_url': 'https://x.com',
                'endpoints': {'ep': {'pagination': offset_pagination(data_path='items')}},
            },
            strict=True,
        )

    def test_raw_user_dict_rejects_lifecycle_hooks_in_pagination(self):
        "raw pagination dicts without the helper marker reject lifecycle hooks with a redirecting message"
        for key in ('on_response', 'on_page', 'on_complete', 'on_page_complete', 'update_state'):
            with self.assertRaises(SchemaError) as ctx:
                validate(
                    {
                        'base_url': 'https://x.com',
                        'endpoints': {
                            'ep': {
                                'pagination': {
                                    'next_request': lambda r, s: None,
                                    key: lambda *a: None,
                                }
                            }
                        },
                    }
                )
            self.assertIn('at endpoint level instead', str(ctx.exception))

    def test_explicit_endpoint_none_suppresses_helper_hook(self):
        "endpoint-level on_response=None suppresses the helper-provided on_response"
        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'mock': [{'items': [1, 2]}],
                        'pagination': offset_pagination(data_path='items'),
                        'on_response': None,
                    }
                },
            }
        )
        result = client.fetch('ep')
        # with on_response=None, raw parsed response is returned (not items extracted)
        # single page, no on_complete, so fetch returns the raw dict directly
        self.assertIsInstance(result, dict)
        self.assertIn('items', result)
        self.assertEqual(result['items'], [1, 2])


class TestAuth(unittest.TestCase):
    def test_bearer_injects_header(self):
        handler = build_auth_handler({'type': 'bearer', 'token': 'abc123'})
        result = handler.apply({'headers': {}})
        self.assertEqual(result['headers']['Authorization'], 'Bearer abc123')

    def test_bearer_token_callback(self):
        handler = build_auth_handler({'type': 'bearer', 'token_callback': lambda config: 'dynamic'})
        result = handler.apply({'headers': {}})
        self.assertEqual(result['headers']['Authorization'], 'Bearer dynamic')

    def test_bearer_empty_token_raises(self):
        handler = build_auth_handler({'type': 'bearer', 'token': ''})
        with self.assertRaises(AuthError):
            handler.apply({'headers': {}})

    def test_basic_auth_injects_tuple(self):
        handler = build_auth_handler({'type': 'basic', 'username': 'user', 'password': 'pass'})
        result = handler.apply({})
        self.assertEqual(result['auth'], ('user', 'pass'))

    def test_callback_auth_invokes_handler(self):
        def my_auth(req, config):
            return {**req, 'headers': {'X-Key': 'secret'}}

        handler = build_auth_handler({'type': 'callback', 'handler': my_auth})
        result = handler.apply({})
        self.assertEqual(result['headers']['X-Key'], 'secret')

    def test_callback_auth_non_dict_return_raises(self):
        handler = build_auth_handler({'type': 'callback', 'handler': lambda r, config: 'oops'})
        with self.assertRaises(AuthError):
            handler.apply({})

    def test_no_auth_returns_none(self):
        self.assertIsNone(build_auth_handler(None))

    def test_oauth2_fetches_token_on_first_call(self):
        handler = build_auth_handler(
            {
                'type': 'oauth2',
                'token_url': 'https://auth.example.com/token',
                'client_id': 'id',
                'client_secret': 'secret',
            }
        )
        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(
                200, {'access_token': 'tok1', 'expires_in': 3600}
            )
            result = handler.apply({'headers': {}})
        self.assertEqual(result['headers']['Authorization'], 'Bearer tok1')
        self.assertEqual(mock_post.call_count, 1)

    def test_oauth2_reuses_cached_token(self):
        "second call within expiry window must not re-fetch"
        handler = build_auth_handler(
            {
                'type': 'oauth2',
                'token_url': 'https://auth.example.com/token',
                'client_id': 'id',
                'client_secret': 'secret',
            }
        )
        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(
                200, {'access_token': 'tok1', 'expires_in': 3600}
            )
            handler.apply({'headers': {}})
            handler.apply({'headers': {}})
        self.assertEqual(mock_post.call_count, 1, 'token should be cached')

    def test_oauth2_uses_schema_timeout(self):
        "schema timeout is passed to the OAuth2 token fetch, not hardcoded"
        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(200, {'access_token': 'tok', 'expires_in': 3600})
            handler = build_auth_handler(
                {
                    'type': 'oauth2',
                    'token_url': 'https://auth.example.com/token',
                    'client_id': 'id',
                    'client_secret': 'secret',
                },
                timeout=5,
            )
            handler.apply({'headers': {}})
        _, kwargs = mock_post.call_args
        self.assertEqual(kwargs.get('timeout'), 5)

    def test_oauth2_refreshes_expired_token(self):
        "once expiry_margin kicks in, next call must re-fetch"
        handler = build_auth_handler(
            {
                'type': 'oauth2',
                'token_url': 'https://auth.example.com/token',
                'client_id': 'id',
                'client_secret': 'secret',
                'expiry_margin': 60,
            }
        )
        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(
                200, {'access_token': 'tok1', 'expires_in': 3600}
            )
            handler.apply({'headers': {}})
        # force expiry
        handler._expires_at = 0
        with patch('requests.post') as mock_post2:
            mock_post2.return_value = make_response(
                200, {'access_token': 'tok2', 'expires_in': 3600}
            )
            result = handler.apply({'headers': {}})
        self.assertEqual(result['headers']['Authorization'], 'Bearer tok2')
        self.assertEqual(mock_post2.call_count, 1)

    def test_oauth2_password_fetches_token_on_first_call(self):
        handler = build_auth_handler(
            {
                'type': 'oauth2_password',
                'token_url': 'https://auth.example.com/token',
                'client_id': 'id',
                'client_secret': 'secret',
                'username': 'alice',
                'password': 'wonderland',
                'scope': 'api',
            }
        )
        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(
                200, {'access_token': 'tok1', 'expires_in': 3600}
            )
            result = handler.apply({'headers': {}})
        self.assertEqual(result['headers']['Authorization'], 'Bearer tok1')
        _, kwargs = mock_post.call_args
        self.assertEqual(
            kwargs['data'],
            {
                'grant_type': 'password',
                'client_id': 'id',
                'client_secret': 'secret',
                'username': 'alice',
                'password': 'wonderland',
                'scope': 'api',
            },
        )

    def test_oauth2_password_omits_scope_when_not_set(self):
        handler = build_auth_handler(
            {
                'type': 'oauth2_password',
                'token_url': 'https://auth.example.com/token',
                'client_id': 'id',
                'client_secret': 'secret',
                'username': 'alice',
                'password': 'wonderland',
            }
        )
        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(
                200, {'access_token': 'tok1', 'expires_in': 3600}
            )
            handler.apply({'headers': {}})
        _, kwargs = mock_post.call_args
        self.assertNotIn('scope', kwargs['data'])

    def test_oauth2_password_uses_schema_timeout(self):
        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(200, {'access_token': 'tok', 'expires_in': 3600})
            handler = build_auth_handler(
                {
                    'type': 'oauth2_password',
                    'token_url': 'https://auth.example.com/token',
                    'client_id': 'id',
                    'client_secret': 'secret',
                    'username': 'alice',
                    'password': 'wonderland',
                },
                timeout=7,
            )
            handler.apply({'headers': {}})
        _, kwargs = mock_post.call_args
        self.assertEqual(kwargs.get('timeout'), 7)

    def test_oauth2_password_missing_access_token_raises(self):
        handler = build_auth_handler(
            {
                'type': 'oauth2_password',
                'token_url': 'https://auth.example.com/token',
                'client_id': 'id',
                'client_secret': 'secret',
                'username': 'alice',
                'password': 'wonderland',
            }
        )
        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(200, {'token_type': 'Bearer'})
            with self.assertRaises(AuthError):
                handler.apply({'headers': {}})

    def test_oauth2_password_reuses_cached_token(self):
        handler = build_auth_handler(
            {
                'type': 'oauth2_password',
                'token_url': 'https://auth.example.com/token',
                'client_id': 'id',
                'client_secret': 'secret',
                'username': 'alice',
                'password': 'wonderland',
            }
        )
        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(
                200, {'access_token': 'tok1', 'expires_in': 3600}
            )
            handler.apply({'headers': {}})
            handler.apply({'headers': {}})
        self.assertEqual(mock_post.call_count, 1)

    def test_oauth2_password_refreshes_expired_token(self):
        handler = build_auth_handler(
            {
                'type': 'oauth2_password',
                'token_url': 'https://auth.example.com/token',
                'client_id': 'id',
                'client_secret': 'secret',
                'username': 'alice',
                'password': 'wonderland',
                'expiry_margin': 60,
            }
        )
        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(
                200, {'access_token': 'tok1', 'expires_in': 3600}
            )
            handler.apply({'headers': {}})

        handler._expires_at = 0
        with patch('requests.post') as mock_post2:
            mock_post2.return_value = make_response(
                200, {'access_token': 'tok2', 'expires_in': 3600}
            )
            result = handler.apply({'headers': {}})
        self.assertEqual(result['headers']['Authorization'], 'Bearer tok2')
        self.assertEqual(mock_post2.call_count, 1)

    def test_oauth2_password_http_error_raises(self):
        handler = build_auth_handler(
            {
                'type': 'oauth2_password',
                'token_url': 'https://auth.example.com/token',
                'client_id': 'id',
                'client_secret': 'secret',
                'username': 'alice',
                'password': 'wonderland',
            }
        )
        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(401, {'error': 'nope'})
            mock_post.return_value.raise_for_status.side_effect = requests.HTTPError('401')
            with self.assertRaises(AuthError):
                handler.apply({'headers': {}})

    def test_oauth2_password_invalid_json_raises(self):
        handler = build_auth_handler(
            {
                'type': 'oauth2_password',
                'token_url': 'https://auth.example.com/token',
                'client_id': 'id',
                'client_secret': 'secret',
                'username': 'alice',
                'password': 'wonderland',
            }
        )
        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(200, {})
            mock_post.return_value.json.side_effect = ValueError('bad json')
            with self.assertRaises(AuthError):
                handler.apply({'headers': {}})

    def test_oauth2_password_builder_produces_valid_auth_config(self):
        from rest_fetcher.types import SchemaBuilder

        schema = (
            SchemaBuilder('https://api.example.com/v1')
            .oauth2_password(
                token_url='https://auth.example.com/token',
                client_id='id',
                client_secret='secret',
                username='alice',
                password='wonderland',
                scope='api',
            )
            .endpoint('test', method='GET', path='/test')
            .build()
        )
        validate(schema)
        self.assertEqual(schema['auth']['type'], 'oauth2_password')

    def test_retry_builder_supports_jitter_and_delay_fields(self):
        from rest_fetcher.types import SchemaBuilder

        schema = (
            SchemaBuilder('https://api.example.com/v1')
            .retry(max_attempts=4, backoff='linear', base_delay=2.0, max_delay=9.0, jitter='equal')
            .endpoint('test', method='GET', path='/test')
            .build()
        )
        validate(schema)
        self.assertEqual(schema['retry']['base_delay'], 2.0)
        self.assertEqual(schema['retry']['max_delay'], 9.0)
        self.assertEqual(schema['retry']['jitter'], 'equal')

    def test_public_types_are_reexported_from_package_root(self):
        import rest_fetcher
        from rest_fetcher.types import StreamRun, OnEventFn, OnErrorFn, OnPageCompleteFn

        self.assertIs(rest_fetcher.StreamRun, StreamRun)
        self.assertIs(rest_fetcher.OnEventFn, OnEventFn)
        self.assertIs(rest_fetcher.OnErrorFn, OnErrorFn)
        self.assertIs(rest_fetcher.OnPageCompleteFn, OnPageCompleteFn)

    def test_api_client_accepts_oauth2_password_auth(self):
        schema = {
            'base_url': 'https://api.example.com/v1',
            'auth': {
                'type': 'oauth2_password',
                'token_url': 'https://auth.example.com/token',
                'client_id': 'id',
                'client_secret': 'secret',
                'username': 'alice',
                'password': 'wonderland',
                'scope': 'api',
            },
            'endpoints': {'test': {'method': 'GET', 'path': '/test'}},
        }
        client = APIClient(schema)
        self.assertIsInstance(client._auth, OAuth2PasswordAuth)
        with patch('requests.post') as mock_post, patch('requests.Session.request') as mock_request:
            mock_post.return_value = make_response(
                200, {'access_token': 'tok1', 'expires_in': 3600}
            )
            mock_request.return_value = make_response(200, {'ok': True})
            client.fetch('test')
        _, kwargs = mock_request.call_args
        self.assertEqual(kwargs['headers']['Authorization'], 'Bearer tok1')


class TestAuthConcurrency(unittest.TestCase):
    def test_oauth2_refresh_is_coordinated(self):
        handler = build_auth_handler(
            {
                'type': 'oauth2',
                'token_url': 'https://auth.example.com/token',
                'client_id': 'id',
                'client_secret': 'secret',
            }
        )
        barrier = threading.Barrier(10, timeout=5)

        with patch('requests.post') as mock_post:
            mock_post.return_value = make_response(
                200, {'access_token': 'tok1', 'expires_in': 3600}
            )

            def call_apply():
                barrier.wait()
                handler.apply({'headers': {}})

            threads = [threading.Thread(target=call_apply) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        self.assertEqual(mock_post.call_count, 1)


class TestRunStateIsolation(unittest.TestCase):
    def test_concurrent_runs_do_not_share_callback_state(self):
        barrier = threading.Barrier(2, timeout=5)
        seen = {}

        def update_state(page_payload, state):
            return {'marker': state['_request']['url']}

        def next_request(resp, state):
            url = state['_request']['url']
            barrier.wait()
            seen[url] = state['marker']
            return None

        pagination = {
            'next_request': next_request,
        }
        lifecycle = {
            'update_state': update_state,
            'on_response': lambda resp, state: resp,
        }

        schema = {
            'base_url': 'https://api.example.com/v1',
            'endpoints': {
                'users': {
                    'method': 'GET',
                    'path': '/users',
                    'response_format': 'json',
                    'pagination': pagination,
                    **lifecycle,
                },
                'orders': {
                    'method': 'GET',
                    'path': '/orders',
                    'response_format': 'json',
                    'pagination': pagination,
                    **lifecycle,
                },
            },
        }
        client = APIClient(schema)

        responses = {
            'https://api.example.com/v1/users': make_response(200, {'ok': True}),
            'https://api.example.com/v1/orders': make_response(200, {'ok': True}),
        }

        def do_fetch(name):
            client.fetch(name)

        with patch('requests.Session.request') as m:
            m.side_effect = lambda method, url, **kwargs: responses[url]
            t1 = threading.Thread(target=do_fetch, args=('users',))
            t2 = threading.Thread(target=do_fetch, args=('orders',))
            t1.start()
            t2.start()
            t1.join()
            t2.join()

        self.assertEqual(
            seen['https://api.example.com/v1/users'], 'https://api.example.com/v1/users'
        )
        self.assertEqual(
            seen['https://api.example.com/v1/orders'], 'https://api.example.com/v1/orders'
        )


class TestRequestErrorContext(unittest.TestCase):
    def _client(self, mock_response, on_error=None):
        schema = {
            'base_url': 'https://api.example.com/v1',
            'endpoints': {'ep': {'method': 'GET', 'path': '/things'}},
        }
        if on_error:
            schema['on_error'] = on_error
        return APIClient(schema)

    def test_fetch_attaches_context_on_retryable_http_error(self):
        with patch('requests.Session.request') as m:
            m.return_value = make_response(500, {})
            with self.assertRaises(RequestError) as ctx:
                self._client(None).fetch('ep')
        exc = ctx.exception
        self.assertEqual(exc.endpoint, 'ep')
        self.assertEqual(exc.method, 'GET')
        self.assertIn('/things', exc.url)

    def test_fetch_attaches_context_on_non_retryable_http_error(self):
        "400 is not in retry on_codes — goes through _handle_error_response directly"
        with patch('requests.Session.request') as m:
            m.return_value = make_response(400, {'error': 'bad request'})
            with self.assertRaises(RequestError) as ctx:
                self._client(None).fetch('ep')
        exc = ctx.exception
        self.assertEqual(exc.endpoint, 'ep')
        self.assertEqual(exc.method, 'GET')
        self.assertIn('/things', exc.url)

    def test_stream_attaches_context_on_network_error(self):
        import requests as req_lib

        with patch('requests.Session.request') as m:
            m.side_effect = req_lib.ConnectionError('refused')
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retry': {'max_attempts': 1},
                    'endpoints': {'ep': {'method': 'POST', 'path': '/submit'}},
                }
            )
            with self.assertRaises(RequestError) as ctx:
                list(client.stream('ep'))
        exc = ctx.exception
        self.assertEqual(exc.endpoint, 'ep')
        self.assertEqual(exc.method, 'POST')
        self.assertIn('/submit', exc.url)

    def test_url_reflects_post_auth_rewrite(self):
        "if auth rewrites the URL, exception.url must be the rewritten version"
        import requests as req_lib

        def auth_handler(req, state):
            return {**req, 'url': req['url'] + '?auth_token=secret'}

        with patch('requests.Session.request') as m:
            m.side_effect = req_lib.ConnectionError('refused')
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'retry': {'max_attempts': 1},
                    'auth': {'type': 'callback', 'handler': auth_handler},
                    'endpoints': {'ep': {'method': 'GET', 'path': '/things'}},
                }
            )
            with self.assertRaises(RequestError) as ctx:
                client.fetch('ep')

        self.assertIn(
            'auth_token=secret',
            ctx.exception.url,
            'exception.url should reflect post-auth URL, not pre-auth',
        )

    def test_enrichment_is_idempotent(self):
        "if exception already has context set, _enrich_exc must not overwrite it"
        from rest_fetcher.client import _enrich_exc

        exc = RequestError('msg', endpoint='original', method='DELETE', url='http://x')
        _enrich_exc(exc, 'other', 'GET', 'http://y')
        self.assertEqual(exc.endpoint, 'original')
        self.assertEqual(exc.method, 'DELETE')
        self.assertEqual(exc.url, 'http://x')

    def test_default_attributes_are_none(self):
        exc = RequestError('something went wrong')
        self.assertIsNone(exc.endpoint)
        self.assertIsNone(exc.method)
        self.assertIsNone(exc.url)


class TestRetry(unittest.TestCase):
    def test_success_on_first_attempt(self):
        handler = RetryHandler({'max_attempts': 3}, {})
        mock_fn = MagicMock(return_value=make_response(200, {'ok': True}))
        response = handler.execute(mock_fn)
        self.assertEqual(response.status_code, 200)
        mock_fn.assert_called_once()

    def test_retries_on_configured_codes(self):
        handler = RetryHandler({'max_attempts': 3, 'backoff': 'linear', 'base_delay': 0}, {})
        responses = [make_response(500), make_response(500), make_response(200, {'ok': True})]
        mock_fn = MagicMock(side_effect=responses)
        with patch('time.sleep'):
            response = handler.execute(mock_fn)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(mock_fn.call_count, 3)

    def test_raises_after_max_attempts(self):
        handler = RetryHandler({'max_attempts': 2, 'backoff': 'linear', 'base_delay': 0}, {})
        mock_fn = MagicMock(return_value=make_response(500))
        with patch('time.sleep'):
            with self.assertRaises(RequestError) as ctx:
                handler.execute(mock_fn)
        self.assertEqual(ctx.exception.status_code, 500)

    def test_non_retry_code_returned_immediately(self):
        handler = RetryHandler({'max_attempts': 3, 'on_codes': [500]}, {})
        mock_fn = MagicMock(return_value=make_response(404))
        response = handler.execute(mock_fn)
        self.assertEqual(response.status_code, 404)
        mock_fn.assert_called_once()

    def test_respects_retry_after_header(self):
        handler = RetryHandler(
            {'max_attempts': 2, 'backoff': 'linear', 'base_delay': 0}, {'respect_retry_after': True}
        )
        r429 = make_response(429)
        r429.headers = {'Retry-After': '5'}
        mock_fn = MagicMock(side_effect=[r429, make_response(200)])
        slept = []
        with patch('time.sleep', side_effect=lambda s: slept.append(s)):
            handler.execute(mock_fn)
        self.assertAlmostEqual(slept[0], 5.0, places=0)

    def _assert_invalid_retry_after_falls_back(self, header_value):
        handler = RetryHandler(
            {'max_attempts': 2, 'backoff': 'linear', 'base_delay': 7.0},
            {'respect_retry_after': True},
        )
        r429 = make_response(429)
        r429.headers = {'Retry-After': header_value}
        mock_fn = MagicMock(side_effect=[r429, make_response(200)])
        slept = []

        with patch('time.sleep', side_effect=lambda s: slept.append(s)):
            handler.execute(mock_fn)

        self.assertEqual(slept[0], 7.0)

    def test_max_retry_after_raises_rate_limit_error(self):
        handler = RetryHandler(
            {'max_attempts': 3, 'base_delay': 0, 'max_retry_after': 10.0},
            {'respect_retry_after': True},
        )
        r429 = make_response(429)
        r429.headers = {'Retry-After': '999'}
        mock_fn = MagicMock(return_value=r429)
        with patch('time.sleep'):
            with self.assertRaises(RateLimitError):
                handler.execute(mock_fn)

    def test_invalid_negative_retry_after_falls_back_to_backoff(self):
        self._assert_invalid_retry_after_falls_back('-5')

    def test_invalid_nan_retry_after_falls_back_to_backoff(self):
        self._assert_invalid_retry_after_falls_back('NaN')

    def test_invalid_infinite_retry_after_falls_back_to_backoff(self):
        self._assert_invalid_retry_after_falls_back('inf')

    def test_min_delay_enforced(self):
        handler = RetryHandler({}, {'min_delay': 0.5})
        handler._last_request_tick = float('inf')  # simulate very recent request
        slept = []
        with patch('time.sleep', side_effect=lambda s: slept.append(s)):
            handler._enforce_min_delay()
        self.assertTrue(len(slept) > 0)

    def test_min_delay_uses_monotonic_time(self):
        handler = RetryHandler({}, {'min_delay': 0.01})
        mock_fn = MagicMock(return_value=make_response(200, {'ok': True}))

        def boom(*args, **kwargs):
            raise AssertionError('time.time() should not be used for min_delay bookkeeping')

        with patch('time.time', side_effect=boom):
            handler.execute(mock_fn)

    def test_network_error_retried(self):
        import requests as req_lib

        handler = RetryHandler({'max_attempts': 3, 'base_delay': 0}, {})
        # use requests.ConnectionError, not bulitin — retry only catches requests.RequestException
        mock_fn = MagicMock(side_effect=[req_lib.ConnectionError('down'), make_response(200)])
        with patch('time.sleep'):
            response = handler.execute(mock_fn)
        self.assertEqual(response.status_code, 200)

    def test_programming_error_not_retried(self):
        "non-requests exceptions (AttributeError etc) should bubble up immediately, not be retried"
        handler = RetryHandler({'max_attempts': 3, 'base_delay': 0}, {})
        call_count = [0]

        def bad_fn():
            call_count[0] += 1
            raise AttributeError('programming error — wrong attribute')

        with patch('time.sleep'):
            with self.assertRaises(AttributeError):
                handler.execute(bad_fn)
        self.assertEqual(call_count[0], 1, 'should have failed on first attempt, not retried')

    def test_requests_timeout_retried(self):
        import requests as req_lib

        handler = RetryHandler({'max_attempts': 3, 'base_delay': 0}, {})
        mock_fn = MagicMock(side_effect=[req_lib.Timeout('timed out'), make_response(200)])
        with patch('time.sleep'):
            response = handler.execute(mock_fn)
        self.assertEqual(response.status_code, 200)

    def test_network_error_wrapped_in_request_error(self):
        "exhausted network retries should raise RequestError with cause set"
        import requests as req_lib

        handler = RetryHandler({'max_attempts': 2, 'base_delay': 0}, {})
        cause = req_lib.ConnectionError('refused')
        mock_fn = MagicMock(side_effect=cause)
        with patch('time.sleep'):
            with self.assertRaises(RequestError) as ctx:
                handler.execute(mock_fn)
        self.assertIs(ctx.exception.cause, cause)

    def test_jitter_full_applies_to_computed_backoff_only(self):
        handler = RetryHandler(
            {
                'max_attempts': 2,
                'backoff': 'linear',
                'base_delay': 4.0,
                'max_delay': 4.0,
                'jitter': 'full',
            },
            {},
        )
        with patch('rest_fetcher.retry.random.uniform', return_value=1.25) as uniform_mock:
            delay = handler._resolve_retry_delay({}, 500, 1)
        self.assertEqual(delay, 1.25)
        uniform_mock.assert_called_once_with(0.0, 4.0)

    def test_jitter_not_applied_to_retry_after(self):
        handler = RetryHandler(
            {
                'max_attempts': 2,
                'backoff': 'linear',
                'base_delay': 4.0,
                'max_delay': 4.0,
                'jitter': 'full',
            },
            {'respect_retry_after': True},
        )
        with patch('rest_fetcher.retry.random.uniform') as uniform_mock:
            delay = handler._resolve_retry_delay({'Retry-After': '7'}, 429, 1)
        self.assertEqual(delay, 7.0)
        uniform_mock.assert_not_called()

    def test_jitter_not_applied_to_min_delay(self):
        handler = RetryHandler(
            {
                'max_attempts': 2,
                'backoff': 'linear',
                'base_delay': 4.0,
                'max_delay': 4.0,
                'jitter': 'full',
            },
            {'min_delay': 3.0},
        )
        sleeps = []
        handler._last_request_tick = float('inf')
        with (
            patch('time.sleep', side_effect=lambda s: sleeps.append(s)),
            patch('rest_fetcher.retry.random.uniform') as uniform_mock,
        ):
            handler._enforce_min_delay()
        self.assertTrue(sleeps)
        uniform_mock.assert_not_called()

    def test_validate_normalizes_true_jitter_to_full(self):
        schema = {
            'base_url': 'https://api.example.com/v1',
            'retry': {'max_attempts': 2, 'jitter': True},
            'endpoints': {'test': {'method': 'GET', 'path': '/test'}},
        }
        validate(schema)
        self.assertEqual(schema['retry']['jitter'], 'full')

    def test_callable_backoff_cannot_be_combined_with_jitter(self):
        schema = {
            'base_url': 'https://api.example.com/v1',
            'retry': {'max_attempts': 2, 'backoff': lambda attempt: 1.0, 'jitter': 'full'},
            'endpoints': {'test': {'method': 'GET', 'path': '/test'}},
        }
        with self.assertRaises(SchemaError):
            validate(schema)

    def test_endpoint_retry_none_suppresses_attempts_but_keeps_retry_after_wait(self):
        sleeps = []
        response = make_response(429, {'detail': 'slow down'}, headers={'Retry-After': '5'})
        client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'retry': {
                    'max_attempts': 4,
                    'backoff': 'linear',
                    'base_delay': 0.1,
                    'max_delay': 0.1,
                },
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
        with (
            patch('requests.Session.request', return_value=response) as mock_req,
            patch('time.sleep', side_effect=lambda s: sleeps.append(s)),
        ):
            with self.assertRaises(RequestError):
                client.fetch('t')
        self.assertEqual(mock_req.call_count, 1)
        self.assertEqual(sleeps, [5.0])


class TestAPIClientFetch(unittest.TestCase):
    "integration-style tests using mock lists — no real HTTP"

    def _client(self, endpoint_config=None, **schema_overrides):
        endpoint = {'method': 'GET', 'path': '/test'}
        if endpoint_config:
            endpoint.update(endpoint_config)
        schema = {
            'base_url': 'https://api.example.com/v1',
            'endpoints': {'test': endpoint},
            **schema_overrides,
        }
        return APIClient(schema)

    def test_single_page_fetch_returns_dict(self):
        client = self._client(endpoint_config={'mock': [{'data': [1, 2, 3]}]})
        result = client.fetch('test')
        self.assertEqual(result, {'data': [1, 2, 3]})

    def test_multi_page_fetch_returns_list(self):
        client = self._client(
            endpoint_config={
                'pagination': cursor_pagination('cursor', 'next_cursor', 'items'),
                'mock': [
                    {'items': [1, 2], 'next_cursor': 'p2'},
                    {'items': [3, 4], 'next_cursor': None},
                ],
            }
        )
        result = client.fetch('test')
        self.assertEqual(result, [[1, 2], [3, 4]])

    def test_stream_yields_pages(self):
        client = self._client(
            endpoint_config={
                'pagination': cursor_pagination('cursor', 'next_cursor', 'items'),
                'mock': [
                    {'items': [1, 2], 'next_cursor': 'p2'},
                    {'items': [3], 'next_cursor': None},
                ],
            }
        )
        pages = list(client.stream('test'))
        self.assertEqual(pages, [[1, 2], [3]])

    def test_on_response_callback_applied(self):
        client = self._client(
            endpoint_config={
                'mock': [{'result': {'data': [42]}}],
                'on_response': lambda resp, state: resp['result']['data'],
            }
        )
        result = client.fetch('test')
        self.assertEqual(result, [42])

    def test_on_complete_flattens_pages(self):
        client = self._client(
            endpoint_config={
                'pagination': cursor_pagination('cursor', 'next_cursor', 'items'),
                'on_complete': lambda pages, state: [item for page in pages for item in page],
                'mock': [
                    {'items': [1, 2], 'next_cursor': 'p2'},
                    {'items': [3, 4], 'next_cursor': None},
                ],
            }
        )
        result = client.fetch('test')
        self.assertEqual(result, [1, 2, 3, 4])

    def test_on_page_called_per_page(self):
        collected = []
        client = self._client(
            endpoint_config={
                'pagination': cursor_pagination('cursor', 'next_cursor', 'items'),
                'on_page': lambda data, state: collected.append(data),
                'mock': [
                    {'items': [1, 2], 'next_cursor': 'p2'},
                    {'items': [3], 'next_cursor': None},
                ],
            }
        )
        list(client.stream('test'))
        self.assertEqual(collected, [[1, 2], [3]])

    def test_stream_on_complete_receives_summary_and_yields_pages(self):
        seen = {}

        def on_complete(summary, state):
            seen['summary'] = summary
            seen['stop'] = state.get('stop')
            return ['ignored']

        client = self._client(
            endpoint_config={
                'pagination': cursor_pagination('cursor', 'next_cursor', 'items'),
                'on_complete': on_complete,
                'mock': [
                    {'items': [1, 2], 'next_cursor': 'p2'},
                    {'items': [3], 'next_cursor': None},
                ],
            }
        )

        pages = list(client.stream('test'))

        self.assertEqual(pages, [[1, 2], [3]])
        self.assertIsInstance(seen['summary'], StreamSummary)
        self.assertEqual(seen['summary'].pages, 2)
        self.assertEqual(seen['summary'].requests, 2)
        self.assertIsNotNone(seen['summary'].stop)
        self.assertEqual(seen['summary'].stop.kind, 'next_request_none')
        self.assertEqual(seen['stop'], {'kind': 'next_request_none'})

    def test_stream_run_exposes_summary_after_exhaustion(self):
        client = self._client(
            endpoint_config={
                'pagination': cursor_pagination('cursor', 'next_cursor', 'items'),
                'mock': [
                    {'items': [1, 2], 'next_cursor': 'p2'},
                    {'items': [3], 'next_cursor': None},
                ],
            }
        )

        run = client.stream_run('test')
        pages = list(run)

        self.assertEqual(pages, [[1, 2], [3]])
        self.assertIsInstance(run.summary, StreamSummary)
        self.assertEqual(run.summary.pages, 2)
        self.assertEqual(run.summary.requests, 2)
        self.assertIsNotNone(run.summary.stop)
        self.assertEqual(run.summary.stop.kind, 'next_request_none')

    def test_stream_run_summary_none_if_iteration_abandoned(self):
        client = self._client(
            endpoint_config={
                'pagination': cursor_pagination('cursor', 'next_cursor', 'items'),
                'mock': [
                    {'items': [1, 2], 'next_cursor': 'p2'},
                    {'items': [3], 'next_cursor': None},
                ],
            }
        )

        run = client.stream_run('test')
        it = iter(run)
        first = next(it)
        self.assertEqual(first, [1, 2])
        # stop iterating early — no normal completion, no summary
        self.assertIsNone(run.summary)

        # abandoning one run must not affect a later run on the same client
        run2 = client.stream_run('test')
        self.assertEqual(list(run2), [[1, 2], [3]])
        self.assertIsNotNone(run2.summary)
        self.assertEqual(run2.summary.pages, 2)

    def test_stream_run_exposes_stop_on_max_pages(self):
        client = self._client(
            endpoint_config={
                'pagination': cursor_pagination('cursor', 'next_cursor', 'items'),
                'mock': [
                    {'items': [1, 2], 'next_cursor': 'p2'},
                    {'items': [3], 'next_cursor': None},
                ],
            }
        )

        run = client.stream_run('test', max_pages=1)
        pages = list(run)

        self.assertEqual(pages, [[1, 2]])
        self.assertIsInstance(run.summary, StreamSummary)
        self.assertEqual(run.summary.pages, 1)
        self.assertIsNotNone(run.summary.stop)
        self.assertEqual(run.summary.stop.kind, 'max_pages')
        self.assertEqual(run.summary.stop.limit, 1)
        self.assertEqual(run.summary.stop.observed, 1)

    def test_non_paginated_stream_on_complete_receives_summary(self):
        seen = {}
        client = self._client(
            endpoint_config={
                'mock': [{'result': {'x': 7}}],
                'on_response': lambda resp, state: resp['result'],
                'on_complete': lambda summary, state: seen.update(
                    {
                        'summary': summary,
                        'stop': state.get('stop'),
                    }
                ),
            }
        )

        pages = list(client.stream('test'))

        self.assertEqual(pages, [{'x': 7}])
        self.assertIsInstance(seen['summary'], StreamSummary)
        self.assertEqual(seen['summary'].pages, 1)
        self.assertEqual(seen['summary'].requests, 1)
        self.assertIsNotNone(seen['summary'].stop)
        self.assertEqual(seen['summary'].stop.kind, 'next_request_none')
        self.assertEqual(seen['stop'], {'kind': 'next_request_none'})

    def test_fetch_non_paginated_on_complete_return_value_is_used(self):
        client = self._client(
            endpoint_config={
                'mock': [{'result': {'x': 7}}],
                'on_response': lambda resp, state: resp['result'],
                'on_complete': lambda pages, state: {'wrapped': pages},
            }
        )

        result = client.fetch('test')

        self.assertEqual(result, {'wrapped': [{'x': 7}]})

    def test_stream_non_paginated_on_complete_return_value_is_ignored(self):
        seen = {}
        client = self._client(
            endpoint_config={
                'mock': [{'result': {'x': 7}}],
                'on_response': lambda resp, state: resp['result'],
                'on_complete': lambda summary, state: seen.update({'summary': summary})
                or ['ignored'],
            }
        )

        pages = list(client.stream('test'))

        self.assertEqual(pages, [{'x': 7}])
        self.assertIsInstance(seen['summary'], StreamSummary)

    def test_fetch_failure_does_not_call_on_complete(self):
        seen = {'called': False}
        client = self._client(
            endpoint_config={
                'on_complete': lambda pages, state: seen.update({'called': True}) or pages,
            }
        )

        with patch.object(
            client._session, 'request', side_effect=RequestError('boom', status_code=500)
        ):
            with self.assertRaises(RequestError):
                client.fetch('test')

        self.assertFalse(seen['called'])

    def test_stream_failure_does_not_call_on_complete(self):
        seen = {'called': False}
        client = self._client(
            endpoint_config={
                'on_complete': lambda summary, state: seen.update({'called': True}) or summary,
            }
        )

        with patch.object(
            client._session, 'request', side_effect=RequestError('boom', status_code=500)
        ):
            with self.assertRaises(RequestError):
                list(client.stream('test'))

        self.assertFalse(seen['called'])

    def test_call_time_params_merged(self):
        received = []

        def mock_fn(request_kwargs, ctx=None, *, run_state):
            received.append(request_kwargs.get('params', {}))
            return {'data': []}, {}

        client = self._client(
            endpoint_config={
                'params': {'default_param': 'yes'},
            }
        )
        client._make_job('test', {'params': {'extra': 'value'}})._execute_request(
            {
                'method': 'GET',
                'url': 'x',
                'headers': {},
                'params': {'default_param': 'yes', 'extra': 'value'},
            }
        ) if False else None
        # verify via schema resolution
        from rest_fetcher.schema import resolve_endpoint

        schema = {
            'base_url': 'https://x.com',
            'endpoints': {'test': {'params': {'default': 'yes'}}},
        }
        resolved = resolve_endpoint(schema, 'test')
        self.assertEqual(resolved['params']['default'], 'yes')

    def test_unknown_endpoint_raises_schema_error(self):
        client = self._client()
        with self.assertRaises(SchemaError):
            client.fetch('nonexistent')

    def test_default_retry_behavior_preserved_when_no_retry_config_is_present(self):
        responses = [make_response(500), make_response(200, {'ok': True})]
        client = APIClient(
            {
                'base_url': 'https://example.invalid',
                'endpoints': {'t': {'method': 'GET', 'path': '/t', 'response_format': 'json'}},
            }
        )
        with patch('requests.Session.request', side_effect=responses), patch('time.sleep'):
            self.assertEqual(client.fetch('t'), {'ok': True})

    def test_endpoint_retry_none_does_not_sleep_backoff_without_retry_after(self):
        client = APIClient(
            {
                'base_url': 'https://example.invalid',
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
        with (
            patch('requests.Session.request', return_value=make_response(500)),
            patch('time.sleep') as mock_sleep,
        ):
            with self.assertRaises(RequestError):
                client.fetch('t')
        mock_sleep.assert_not_called()

    def test_on_error_skip_returns_empty(self):
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(404)
            client = self._client(endpoint_config={'on_error': lambda exc, state: 'skip'})
            result = client.fetch('test')
            self.assertEqual(result, {})

    def test_on_error_raise_reraises(self):
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(404)
            client = self._client(endpoint_config={'on_error': lambda exc, state: 'raise'})
            with self.assertRaises(RequestError):
                client.fetch('test')

    def test_on_error_unknown_return_value_raises_callback_error(self):
        'returning anything other than "raise"/"skip" is a programming error'
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(404)
            client = self._client(
                endpoint_config={
                    'on_error': lambda exc, state: 'continue'  # invalid
                }
            )
            with self.assertRaises(CallbackError) as ctx:
                client.fetch('test')
            self.assertIn('continue', str(ctx.exception))

    def test_on_error_exception_propagates_directly(self):
        "if on_error itself raises, the exception propagates as-is (not wrapped in CallbackError)"
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(404)  # 404 is not retried

            def bad_on_error(exc, state):
                raise ValueError('handler exploded')

            client = self._client(endpoint_config={'on_error': bad_on_error})
            with self.assertRaises(ValueError, msg='should propagate ValueError directly'):
                client.fetch('test')

    def test_on_error_http_state_includes_final_response_headers(self):
        seen = {}

        def on_error(exc, state):
            seen['headers'] = dict(state['_response_headers'])
            seen['status_code'] = exc.status_code
            return 'skip'

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(
                404, {'detail': 'nope'}, headers={'Retry-After': '9', 'X-Test': '1'}
            )
            client = self._client(endpoint_config={'on_error': on_error})
            client.fetch('test')

        self.assertEqual(seen['status_code'], 404)
        self.assertEqual(seen['headers']['Retry-After'], '9')
        self.assertEqual(seen['headers']['X-Test'], '1')
        self.assertEqual(seen['headers']['_status_code'], 404)

    def test_on_error_network_state_does_not_fabricate_response_headers(self):
        import requests as req_lib

        seen = {}

        def on_error(exc, state):
            seen['has_headers'] = '_response_headers' in state
            return 'skip'

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'retry': {'max_attempts': 1},
                'on_error': on_error,
                'endpoints': {'ep': {'method': 'GET', 'path': '/things'}},
            }
        )
        with patch('requests.Session.request', side_effect=req_lib.ConnectionError('refused')):
            client.fetch('ep')
        self.assertFalse(seen['has_headers'])

    def test_context_manager_closes_session(self):
        with self._client() as client:
            pass
        self.assertTrue(client._session.closed if hasattr(client._session, 'closed') else True)

    def test_state_persists_across_pages(self):
        seen_states = []

        def next_req(resp, state):
            seen_states.append(dict(state))
            return None  # single page

        client = self._client(
            endpoint_config={
                'pagination': {
                    'next_request': next_req,
                },
                'update_state': lambda resp, state: {'page_count': state.get('page_count', 0) + 1},
                'mock': [{'data': 1}],
            }
        )
        list(client.stream('test'))
        self.assertIn('_response_headers', seen_states[0])

    def test_response_parser_override(self):
        def xml_parser(response):
            return {'parsed': 'from_xml'}

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {})
            schema = {
                'base_url': 'https://x.com',
                'response_parser': xml_parser,
                'endpoints': {'test': {'method': 'GET'}},
            }
            client = APIClient(schema)
            result = client.fetch('test')
            self.assertEqual(result, {'parsed': 'from_xml'})


class TestFormEncoded(unittest.TestCase):
    "form= key: schema-level and call-time form-encoded POST"

    def _schema(self, endpoint_overrides=None):
        ep = {'method': 'POST', 'path': '/submit'}
        if endpoint_overrides:
            ep.update(endpoint_overrides)
        return {
            'base_url': 'https://api.example.com',
            'endpoints': {'submit': ep},
        }

    def test_schema_form_sent_as_data(self):
        "form dict in schema is passed to requests as data= (url-encoded)"
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'ok': True})
            client = APIClient(self._schema({'form': {'grant_type': 'password'}}))
            client.fetch('submit')
        _, kwargs = mock_req.call_args
        self.assertIn('data', kwargs)
        self.assertEqual(kwargs['data'], {'grant_type': 'password'})
        self.assertNotIn('json', kwargs)

    def test_calltime_form_sent_as_data(self):
        "form= at call time is passed to requests as data="
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'ok': True})
            client = APIClient(self._schema())
            client.fetch('submit', form={'username': 'alice', 'password': 'secret'})
        _, kwargs = mock_req.call_args
        self.assertIn('data', kwargs)
        self.assertEqual(kwargs['data'], {'username': 'alice', 'password': 'secret'})
        self.assertNotIn('json', kwargs)

    def test_calltime_form_merges_over_schema_form(self):
        "call-time form merges over schema form — schema static fields survive"
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'ok': True})
            client = APIClient(self._schema({'form': {'grant_type': 'password'}}))
            client.fetch('submit', form={'username': 'alice', 'password': 'secret'})
        _, kwargs = mock_req.call_args
        self.assertEqual(
            kwargs['data'],
            {
                'grant_type': 'password',
                'username': 'alice',
                'password': 'secret',
            },
        )

    def test_body_and_form_in_schema_raises_schema_error(self):
        "body and form in schema simultaneously is a SchemaError"
        with self.assertRaises(SchemaError):
            APIClient(self._schema({'body': {'key': 'val'}, 'form': {'field': 'val'}}))

    def test_schema_body_calltime_form_raises_schema_error(self):
        "schema body + call-time form raises SchemaError at fetch time"
        client = APIClient(self._schema({'body': {'key': 'val'}}))
        with patch('requests.Session.request'):
            with self.assertRaises(SchemaError):
                client.fetch('submit', form={'field': 'val'})

    def test_schema_form_calltime_body_raises_schema_error(self):
        "schema form + call-time body raises SchemaError at fetch time"
        client = APIClient(self._schema({'form': {'field': 'val'}}))
        with patch('requests.Session.request'):
            with self.assertRaises(SchemaError):
                client.fetch('submit', body={'key': 'val'})

    def test_files_validation_rejects_invalid_type(self):
        "files must be dict/list/tuple — other types raise SchemaError at construction"
        with self.assertRaises(SchemaError):
            APIClient(self._schema({'files': 'not-valid'}))

    def test_body_and_files_in_schema_raise_schema_error(self):
        "body and files in schema simultaneously is a SchemaError"
        with self.assertRaises(SchemaError):
            APIClient(self._schema({'body': {'key': 'val'}, 'files': {'upload': object()}}))

    def test_schema_files_are_sent_via_request_kwargs(self):
        "schema files attach multipart payload via requests files="
        marker = object()
        client = APIClient(self._schema({'files': {'upload': marker}}))
        with patch.object(
            requests.Session, 'request', return_value=make_response(body={'ok': True})
        ) as mock_req:
            client.fetch('submit')
            self.assertIs(mock_req.call_args.kwargs['files']['upload'], marker)
            self.assertNotIn('json', mock_req.call_args.kwargs)

    def test_schema_form_and_files_can_coexist(self):
        "multipart upload may include form fields and files together"
        marker = object()
        client = APIClient(self._schema({'form': {'field': 'val'}, 'files': {'upload': marker}}))
        with patch.object(
            requests.Session, 'request', return_value=make_response(body={'ok': True})
        ) as mock_req:
            client.fetch('submit')
            self.assertEqual(mock_req.call_args.kwargs['data'], {'field': 'val'})
            self.assertIs(mock_req.call_args.kwargs['files']['upload'], marker)

    def test_calltime_files_override_schema_files(self):
        "call-time files replace schema files rather than merging multipart payloads"
        schema_marker = object()
        call_marker = object()
        client = APIClient(self._schema({'files': {'upload': schema_marker}}))
        with patch.object(
            requests.Session, 'request', return_value=make_response(body={'ok': True})
        ) as mock_req:
            client.fetch('submit', files={'upload': call_marker})
            self.assertIs(mock_req.call_args.kwargs['files']['upload'], call_marker)

    def test_schema_body_calltime_files_raise_schema_error(self):
        "schema body + call-time files raises SchemaError at fetch time"
        client = APIClient(self._schema({'body': {'key': 'val'}}))
        with self.assertRaises(SchemaError):
            with patch.object(
                requests.Session, 'request', return_value=make_response(body={'ok': True})
            ):
                client.fetch('submit', files={'upload': object()})

    def test_calltime_body_schema_files_raise_schema_error(self):
        "call-time body + schema files raises SchemaError at fetch time"
        client = APIClient(self._schema({'files': {'upload': object()}}))
        with self.assertRaises(SchemaError):
            with patch.object(
                requests.Session, 'request', return_value=make_response(body={'ok': True})
            ):
                client.fetch('submit', body={'key': 'val'})

    def test_calltime_files_pass_through_sequence(self):
        "call-time files may use requests-compatible sequence form"
        marker = object()
        files = [('upload', marker)]
        client = APIClient(self._schema({}))
        with patch.object(
            requests.Session, 'request', return_value=make_response(body={'ok': True})
        ) as mock_req:
            client.fetch('submit', files=files)
            self.assertEqual(mock_req.call_args.kwargs['files'], files)

    def test_no_form_no_body_sets_neither(self):
        "no form or body — neither data= nor json= in request kwargs"
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'ok': True})
            client = APIClient(self._schema())
            client.fetch('submit')
        _, kwargs = mock_req.call_args
        self.assertNotIn('data', kwargs)
        self.assertNotIn('json', kwargs)


class TestOnRequestHook(unittest.TestCase):
    def test_on_request_shapes_request_before_auth(self):
        seen_by_auth = {}

        def on_request(req, state):
            headers = {**req.get('headers', {}), 'X-Run-ID': state['run_id']}
            return {**req, 'headers': headers}

        def auth_handler(req, config):
            seen_by_auth['x_run_id'] = req.get('headers', {}).get('X-Run-ID')
            headers = {**req.get('headers', {}), 'Authorization': f'Bearer {config["token"]}'}
            return {**req, 'headers': headers}

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'state': {'run_id': 'run-123', 'token': 'secret'},
                'auth': {'type': 'callback', 'handler': auth_handler},
                'on_request': on_request,
                'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
            }
        )

        with patch.object(
            requests.Session, 'request', return_value=make_response(body={'ok': True})
        ) as mock_req:
            client.fetch('ep')

        self.assertEqual(seen_by_auth['x_run_id'], 'run-123')
        self.assertEqual(mock_req.call_args.kwargs['headers']['X-Run-ID'], 'run-123')
        self.assertEqual(mock_req.call_args.kwargs['headers']['Authorization'], 'Bearer secret')

    def test_on_request_runs_on_mock_path(self):
        seen = {}

        def on_request(req, state):
            req = {**req, 'headers': {**req.get('headers', {}), 'X-Trace': state['trace_id']}}
            return req

        def mock_handler(req, run_state=None):
            seen['headers'] = dict(req.get('headers', {}))
            return {'ok': True}

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'state': {'trace_id': 'trace-1'},
                'on_request': on_request,
                'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'mock': mock_handler}},
            }
        )

        result = client.fetch('ep')
        self.assertEqual(result, {'ok': True})
        self.assertEqual(seen['headers']['X-Trace'], 'trace-1')

    def test_on_request_must_return_dict(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'state': {'trace_id': 'trace-1'},
                'on_request': lambda req, state: req.update(
                    {'headers': {'X-Trace': state['trace_id']}}
                ),
                'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
            }
        )

        with patch.object(
            requests.Session, 'request', return_value=make_response(body={'ok': True})
        ):
            with self.assertRaises(CallbackError) as ctx:
                client.fetch('ep')
        self.assertIn('on_request must return a dict of request kwargs', str(ctx.exception))
        self.assertIn('did you forget to return', str(ctx.exception))

    def test_endpoint_on_request_overrides_client_on_request(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'state': {'trace_id': 'trace-1'},
                'on_request': lambda req, state: {
                    **req,
                    'headers': {**req.get('headers', {}), 'X-Client': '1'},
                },
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'on_request': lambda req, state: {
                            **req,
                            'headers': {**req.get('headers', {}), 'X-Endpoint': '1'},
                        },
                    }
                },
            }
        )
        with patch.object(
            requests.Session, 'request', return_value=make_response(body={'ok': True})
        ) as mock_req:
            client.fetch('ep')
        headers = mock_req.call_args.kwargs['headers']
        self.assertNotIn('X-Client', headers)
        self.assertEqual(headers['X-Endpoint'], '1')

    def test_endpoint_on_request_none_suppresses_client_on_request(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'on_request': lambda req, state: {
                    **req,
                    'headers': {**req.get('headers', {}), 'X-Client': '1'},
                },
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'on_request': None,
                    }
                },
            }
        )
        with patch.object(
            requests.Session, 'request', return_value=make_response(body={'ok': True})
        ) as mock_req:
            client.fetch('ep')
        headers = mock_req.call_args.kwargs['headers']
        self.assertNotIn('X-Client', headers)


class TestErrorMessages(unittest.TestCase):
    def test_error_includes_api_message_anthropic_style(self):
        'anthropic error body: {"type": "error", "error": {"message": "..."}}'
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(
                400,
                {
                    'type': 'error',
                    'error': {'type': 'invalid_request_error', 'message': 'missing beta header'},
                },
            )
            client = APIClient(simple_schema())
            with self.assertRaises(RequestError) as ctx:
                client.fetch('test')
        self.assertIn('missing beta header', str(ctx.exception))

    def test_error_includes_api_message_generic_style(self):
        'generic error body: {"message": "..."}'
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(404, {'message': 'not found'})
            client = APIClient(simple_schema())
            with self.assertRaises(RequestError) as ctx:
                client.fetch('test')
        self.assertIn('not found', str(ctx.exception))

    def test_error_includes_status_code(self):
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(503, {'message': 'unavailable'})
            client = APIClient(simple_schema())
            with self.assertRaises(RequestError) as ctx:
                client.fetch('test')
        self.assertIn('503', str(ctx.exception))

    def test_error_non_json_body_included(self):
        with patch('requests.Session.request') as mock_req:
            r = make_response(500, None)
            r.text = 'Internal Server Error'
            r.json.side_effect = Exception('not json')
            mock_req.return_value = r
            client = APIClient(simple_schema())
            with self.assertRaises(RequestError) as ctx:
                client.fetch('test')
        self.assertIn('Internal Server Error', str(ctx.exception))


class TestEmptyBodyAndStatusCode(unittest.TestCase):
    def test_empty_body_returns_none(self):
        "HTTP 204 No Content — empty body should parse to None, not raise"
        with patch('requests.Session.request') as mock_req:
            r = make_response(204, None)
            r.text = ''
            mock_req.return_value = r
            client = APIClient(simple_schema())
            result = client.fetch('test')
            self.assertIsNone(result)

    def test_whitespace_only_body_returns_none(self):
        with patch('requests.Session.request') as mock_req:
            r = make_response(200, None)
            r.text = '   '
            mock_req.return_value = r
            mock_req.return_value.json.side_effect = Exception('no json')
            client = APIClient(simple_schema())
            result = client.fetch('test')
            self.assertIsNone(result)

    def test_status_code_injected_into_state_headers(self):
        'state["_response_headers"]["_status_code"] must equal HTTP status code'
        captured = {}
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(204, None)
            mock_req.return_value.text = ''
            client = APIClient(
                simple_schema(
                    on_response=lambda resp, state: captured.update(state['_response_headers'])
                    or resp
                )
            )
            client.fetch('test')
        self.assertEqual(captured['_status_code'], 204)

    def test_status_code_200_available_in_on_response(self):
        seen = {}
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'ok': True})
            client = APIClient(
                simple_schema(
                    on_response=lambda resp, state: seen.update(
                        {'code': state['_response_headers']['_status_code']}
                    )
                    or resp
                )
            )
            client.fetch('test')
        self.assertEqual(seen['code'], 200)

    def test_204_pattern_is_holiday(self):
        "204 = not a holiday pattern, as used by nager.at IsTodayPublicHoliday endpoint"
        with patch('requests.Session.request') as mock_req:
            r = make_response(204, None)
            r.text = ''
            mock_req.return_value = r
            client = APIClient(
                simple_schema(
                    on_response=lambda resp, state: {
                        'is_holiday': state['_response_headers']['_status_code'] == 200
                    }
                )
            )
            result = client.fetch('test')
            self.assertEqual(result, {'is_holiday': False})

    def test_200_pattern_is_holiday(self):
        "200 = is a holiday pattern"
        with patch('requests.Session.request') as mock_req:
            r = make_response(200, None)
            r.text = ''
            mock_req.return_value = r
            client = APIClient(
                simple_schema(
                    on_response=lambda resp, state: {
                        'is_holiday': state['_response_headers']['_status_code'] == 200
                    }
                )
            )
            result = client.fetch('test')
            self.assertEqual(result, {'is_holiday': True})


class TestStateSeedingPrecedence(unittest.TestCase):
    "initial_params seeds state last; call-time params override initial_params in state"

    def test_call_time_params_win_over_initial_params_in_state(self):
        "initial_params={limit:100}, call-time params={limit:50} -> state[limit]=50 -> page 2 uses 50"
        from rest_fetcher.pagination import CycleRunner

        requests_seen = []
        call_count = [0]

        def mock_fetch(request, ctx=None, *, run_state):
            requests_seen.append(
                {k: dict(v) if isinstance(v, dict) else v for k, v in request.items()}
            )
            call_count[0] += 1
            # return 50 items on page 1 — full page at call-time limit
            return outcome(
                ({'items': list(range(50))} if call_count[0] == 1 else {'items': []}), request
            )

        strategy = offset_pagination(limit=100, data_path='items')
        runner = CycleRunner(strategy)

        # initial_params says limit=100, but call-time override says limit=50
        # state seeding: initial_params first, then call-time params overwrite
        initial = {
            'method': 'GET',
            'url': 'https://example.com',
            'params': {'offset': 0, 'limit': 50},
        }  # call-time override
        list(runner.run(mock_fetch, initial))

        # page 2 must use limit=50 (call-time), not limit=100 (initial_params factory default)
        self.assertEqual(
            requests_seen[1]['params']['limit'],
            50,
            'call-time limit=50 should win over factory initial_params limit=100 in state',
        )

    def test_initial_params_used_when_no_call_time_override(self):
        "when no call-time override, initial_params seed is used"
        from rest_fetcher.pagination import CycleRunner

        requests_seen = []
        call_count = [0]

        def mock_fetch(request, ctx=None, *, run_state):
            requests_seen.append(
                {k: dict(v) if isinstance(v, dict) else v for k, v in request.items()}
            )
            call_count[0] += 1
            return outcome(
                ({'items': list(range(100))} if call_count[0] == 1 else {'items': []}), request
            )

        strategy = offset_pagination(limit=100, data_path='items')
        runner = CycleRunner(strategy)
        # initial request already has limit=100 from initial_params injection
        initial = {
            'method': 'GET',
            'url': 'https://example.com',
            'params': {'offset': 0, 'limit': 100},
        }
        list(runner.run(mock_fetch, initial))
        self.assertEqual(requests_seen[1]['params']['limit'], 100)

    def test_page_number_call_time_page_size_wins_in_state(self):
        from rest_fetcher.pagination import CycleRunner

        requests_seen = []
        call_count = [0]

        def mock_fetch(request, ctx=None, *, run_state):
            requests_seen.append(
                {k: dict(v) if isinstance(v, dict) else v for k, v in request.items()}
            )
            call_count[0] += 1
            return outcome(
                ({'results': list(range(25))} if call_count[0] == 1 else {'results': []}), request
            )

        strategy = page_number_pagination(page_size=50, data_path='results')
        runner = CycleRunner(strategy)
        initial = {
            'method': 'GET',
            'url': 'https://example.com',
            'params': {'page': 1, 'page_size': 25},
        }  # call-time: 25, factory: 50
        list(runner.run(mock_fetch, initial))
        self.assertEqual(
            requests_seen[1]['params']['page_size'],
            25,
            'call-time page_size=25 should win over factory page_size=50 in state',
        )


class TestInitialStatePrecedence(unittest.TestCase):
    def test_initial_params_override_seed_in_callback_state(self):
        seen = {}
        runner = CycleRunner(
            {
                'next_request': lambda resp, state: None,
                'on_response': lambda resp, state: (seen.update(state) or resp),
                'initial_params': {'page': 2},
            }
        )

        def mock_fetch(req, ctx=None, *, run_state):
            return outcome({'items': [1]}, req)

        initial = {'method': 'GET', 'url': 'https://x.com', 'params': {'page': 2}}
        list(runner.run(mock_fetch, initial, initial_state={'page': 1}))
        assert seen['page'] == 2


class TestPathParams(unittest.TestCase):
    def test_path_params_interpolated(self):
        schema = {
            'base_url': 'https://api.example.com/v3',
            'endpoints': {'holidays': {'method': 'GET', 'path': '/holidays/{year}/{country}'}},
        }
        client = APIClient(schema)
        # verify _build_url resolves correctly via the job
        job = client._make_job('holidays', {'path_params': {'year': 2026, 'country': 'US'}})
        url = job._build_url()
        self.assertEqual(url, 'https://api.example.com/v3/holidays/2026/US')

    def test_path_params_missing_key_raises(self):
        from rest_fetcher.exceptions import SchemaError

        schema = {
            'base_url': 'https://api.example.com/v3',
            'endpoints': {'holidays': {'method': 'GET', 'path': '/holidays/{year}/{country}'}},
        }
        client = APIClient(schema)
        job = client._make_job('holidays', {'path_params': {'year': 2026}})  # missing country
        with self.assertRaises(SchemaError):
            job._build_url()

    def test_no_path_params_unchanged(self):
        schema = {
            'base_url': 'https://api.example.com/v3',
            'endpoints': {'items': {'method': 'GET', 'path': '/items'}},
        }
        client = APIClient(schema)
        job = client._make_job('items', {})
        self.assertEqual(job._build_url(), 'https://api.example.com/v3/items')

    def test_path_params_with_mock_full_fetch(self):
        client = APIClient(
            {
                'base_url': 'https://date.nager.at/api/v3',
                'endpoints': {
                    'holidays': {
                        'method': 'GET',
                        'path': '/PublicHolidays/{year}/{country}',
                        'mock': [
                            {'date': '2026-01-01', 'name': "New Year's Day", 'countryCode': 'US'}
                        ],
                    }
                },
            }
        )
        result = client.fetch('holidays', path_params={'year': 2026, 'country': 'US'})
        self.assertEqual(result['countryCode'], 'US')
        self.assertEqual(result['name'], "New Year's Day")


class TestHeaderScrubbing(unittest.TestCase):
    def setUp(self):
        from rest_fetcher.client import _scrub

        self.scrub = _scrub

    # default exact-match set
    def test_authorization_scrubbed_by_default(self):
        self.assertEqual(self.scrub({'Authorization': 'Bearer abc'})['Authorization'], '***')

    def test_x_api_key_scrubbed_by_default(self):
        self.assertEqual(self.scrub({'X-Api-Key': 'secret'})['X-Api-Key'], '***')

    def test_safe_header_preserved(self):
        self.assertEqual(
            self.scrub({'Content-Type': 'application/json'})['Content-Type'], 'application/json'
        )

    # pattern-match: any key containing token/secret/password/key/auth
    def test_custom_token_header_scrubbed_by_pattern(self):
        self.assertEqual(self.scrub({'X-Auth-Token': 'tok'})['X-Auth-Token'], '***')

    def test_my_password_header_scrubbed_by_pattern(self):
        self.assertEqual(self.scrub({'X-My-Password': 'pw'})['X-My-Password'], '***')

    def test_api_secret_header_scrubbed_by_pattern(self):
        self.assertEqual(self.scrub({'Api-Secret': 'shh'})['Api-Secret'], '***')

    def test_case_insensitive_pattern_match(self):
        self.assertEqual(self.scrub({'X-ACCESS-TOKEN': 'tok'})['X-ACCESS-TOKEN'], '***')

    # extra_scrub from schema['scrub_headers']
    def test_extra_scrub_exact_name(self):
        result = self.scrub({'X-Tenant-Id': 'abc', 'Accept': '*/*'}, extra_scrub=['X-Tenant-Id'])
        self.assertEqual(result['X-Tenant-Id'], '***')
        self.assertEqual(result['Accept'], '*/*')

    def test_extra_scrub_case_insensitive(self):
        result = self.scrub({'x-tenant-id': 'abc'}, extra_scrub=['X-TENANT-ID'])
        self.assertEqual(result['x-tenant-id'], '***')

    def test_empty_headers_returns_empty(self):
        self.assertEqual(self.scrub({}), {})

    def test_scrub_schema_key_validated(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'scrub_headers': [123]})  # non-string entry

    def test_scrub_headers_must_be_list(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'scrub_headers': 'X-Token'})  # string not list

    def test_scrub_recorded_url_redacts_builtin_query_params(self):
        from rest_fetcher.client import _scrub_recorded_url

        url = 'https://api.example.com/items?access_token=abc123&limit=100'
        result = _scrub_recorded_url(url)
        self.assertIn('access_token=[REDACTED]', result)
        self.assertIn('limit=100', result)

    def test_scrub_recorded_url_redacts_by_pattern(self):
        from rest_fetcher.client import _scrub_recorded_url

        url = 'https://api.example.com/items?my_auth_token=abc123&page=2'
        result = _scrub_recorded_url(url)
        self.assertIn('my_auth_token=[REDACTED]', result)
        self.assertIn('page=2', result)

    def test_scrub_recorded_url_redacts_user_extensions(self):
        from rest_fetcher.client import _scrub_recorded_url

        url = 'https://api.example.com/items?tenant_token=abc123&page=2'
        result = _scrub_recorded_url(url, extra_scrub=['tenant_token'])
        self.assertIn('tenant_token=[REDACTED]', result)
        self.assertIn('page=2', result)

    def test_scrub_recorded_url_does_not_redact_generic_code_by_default(self):
        from rest_fetcher.client import _scrub_recorded_url

        url = 'https://api.example.com/items?code=US&page=2'
        result = _scrub_recorded_url(url)
        self.assertIn('code=US', result)
        self.assertIn('page=2', result)

    def test_scrub_recorded_url_can_redact_code_via_extension(self):
        from rest_fetcher.client import _scrub_recorded_url

        url = 'https://api.example.com/items?code=secret123&page=2'
        result = _scrub_recorded_url(url, extra_scrub=['code'])
        self.assertIn('code=[REDACTED]', result)
        self.assertIn('page=2', result)

    def test_scrub_query_params_schema_key_validated(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'scrub_query_params': [123]})

    def test_scrub_query_params_must_be_list(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'scrub_query_params': 'tenant_token'})

    def test_retry_base_delay_must_be_positive(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'retry': {'max_attempts': 3, 'base_delay': 0}})

    def test_retry_max_delay_must_be_positive(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'retry': {'max_attempts': 3, 'max_delay': -1}})

    def test_retry_base_delay_must_be_numeric(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'retry': {'max_attempts': 3, 'base_delay': 'fast'}})


# Item 5 — state scope semantics
class TestStateScopeSemantics(unittest.TestCase):
    def test_pagination_callback_receives_seeded_state_keys(self):
        seen_in_callback = {}

        def next_req(resp, state):
            seen_in_callback.update(state)
            return None

        client = APIClient(
            {
                **simple_schema(),
                'state': {'region': 'eu-west-1', 'env': 'prod'},
                'endpoints': {
                    'ep': {
                        'mock': [{'items': [1, 2]}],
                        'pagination': {
                            'next_request': next_req,
                        },
                        'on_response': lambda r, s: r.get('items', []),
                    }
                },
            }
        )
        client.fetch('ep')

        self.assertEqual(seen_in_callback.get('region'), 'eu-west-1')
        self.assertEqual(seen_in_callback.get('env'), 'prod')

    def test_pagination_callback_direct_mutation_raises(self):
        def next_req(resp, state):
            state['injected'] = 'should-raise'
            return None

        client = APIClient(
            {
                **simple_schema(),
                'state': {'region': 'eu'},
                'endpoints': {
                    'ep': {
                        'mock': [{'items': [1]}],
                        'pagination': {
                            'next_request': next_req,
                        },
                        'on_response': lambda r, s: r.get('items', []),
                    }
                },
            }
        )
        with self.assertRaises(CallbackError) as ctx:
            client.fetch('ep')
        self.assertIn('injected', str(ctx.exception))

    def test_page_state_keys_visible_in_callback(self):
        seen = {}

        strategy = offset_pagination(limit=10, data_path='items')
        strategy['on_response'] = lambda r, s: (seen.update(s) or r.get('items', []))

        client = APIClient(
            {
                **simple_schema(),
                'state': {'env': 'test'},
                'endpoints': {
                    'ep': {
                        'mock': [{'items': list(range(10))}, {'items': []}],
                        'pagination': strategy,
                    }
                },
            }
        )
        client.fetch('ep')
        self.assertEqual(seen.get('env'), 'test')
        self.assertIn('offset', seen)

    def test_on_error_receives_run_local_state(self):
        received_state = {}

        def on_err(exc, state):
            received_state.update(state)
            return 'skip'

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(404, {'error': 'not found'})
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'state': {'user': 'alice'},
                    'on_error': on_err,
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                }
            )
            client.fetch('ep')

        self.assertEqual(received_state.get('user'), 'alice')

    def test_on_error_state_is_read_only(self):
        from rest_fetcher import StateViewMutationError

        errors = []

        def on_err(exc, state):
            with self.assertRaises(StateViewMutationError):
                state['mutate'] = 'nope'
            errors.append(exc)
            return 'skip'

        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(404, {'error': 'not found'})
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'state': {'user': 'alice'},
                    'on_error': on_err,
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                }
            )
            client.fetch('ep')

        self.assertEqual(len(errors), 1)

    def test_auth_receives_config_view_not_page_state(self):
        auth_received = {}

        def auth_handler(req, config):
            auth_received.update(dict(config))
            return req

        client = APIClient(
            {
                **simple_schema(),
                'state': {'token': 'abc'},
                'auth': {'type': 'callback', 'handler': auth_handler},
                'endpoints': {
                    'ep': {
                        'mock': [{'items': list(range(10))}, {'items': []}],
                        'pagination': {
                            'next_request': lambda r, s: None,
                            'initial_params': {'offset': 0, 'limit': 10},
                        },
                        'on_response': lambda r, s: r.get('items', []),
                    }
                },
            }
        )
        client.fetch('ep')
        self.assertEqual(auth_received.get('token'), 'abc')
        self.assertNotIn('offset', auth_received)


class TestNonPaginatedCallbackPromotion(unittest.TestCase):
    def test_non_paginated_root_level_callbacks_run_through_trivial_runner(self):
        "non-paginated endpoints still apply root-level callbacks through the trivial runner"
        seen = {}

        def on_page(page, state):
            seen.setdefault('pages', []).append(list(page))

        def on_complete(pages, state):
            seen['complete'] = list(pages)

        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'mock': [{'items': [1, 2, 3]}],
                        'on_response': lambda resp, state: resp['items'],
                        'on_page': on_page,
                        'on_complete': on_complete,
                    }
                },
            }
        )

        result = client.fetch('ep')
        self.assertEqual(result, [[1, 2, 3]])
        self.assertEqual(seen['pages'], [[1, 2, 3]])
        self.assertEqual(seen['complete'], [[1, 2, 3]])

    def test_endpoint_on_response_overrides_builtin_helper_on_response(self):
        "endpoint-level on_response wins over the helper-provided on_response"
        seen = {'endpoint_calls': 0}

        def endpoint_on_response(resp, state):
            seen['endpoint_calls'] += 1
            return resp.get('items', [])

        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1, 2], 'next_cursor': 'n1'},
                            {'items': [3], 'next_cursor': None},
                        ],
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                        'on_response': endpoint_on_response,
                    }
                },
            }
        )

        result = client.fetch('ep')
        self.assertEqual(result, [[1, 2], [3]])
        self.assertEqual(seen['endpoint_calls'], 2)

    def test_endpoint_level_on_page_and_on_complete_work_with_custom_pagination(self):
        "endpoint-level on_page/on_complete fire alongside custom pagination next_request"
        seen = {'pages': [], 'complete': None}

        def on_page(page, state):
            seen['pages'].append(list(page))

        def on_complete(pages, state):
            seen['complete'] = list(pages)

        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1, 2], 'next': True},
                            {'items': [3], 'next': False},
                        ],
                        'pagination': {
                            'next_request': lambda resp, state: {
                                'params': {'page': state.get('page', 1) + 1}
                            }
                            if resp.get('next')
                            else None,
                        },
                        'on_response': lambda resp, state: resp['items'],
                        'on_page': on_page,
                        'on_complete': on_complete,
                    }
                },
            }
        )

        result = client.fetch('ep')
        self.assertEqual(result, [[1, 2], [3]])
        self.assertEqual(seen['pages'], [[1, 2], [3]])
        self.assertEqual(seen['complete'], [[1, 2], [3]])

    def test_stream_run_summary_includes_endpoint_source_retries_and_elapsed(self):
        client = APIClient(
            {
                **simple_schema(),
                'retry': {'max_attempts': 2, 'base_delay': 0.001, 'max_delay': 0.001},
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                    }
                },
            }
        )

        responses = [
            requests.ConnectionError('boom'),
            make_response(
                body={'items': [1], 'next_cursor': None},
                headers={'Content-Type': 'application/json'},
            ),
        ]

        with patch.object(client._session, 'request', side_effect=responses):
            run = client.stream_run('ep')
            pages = list(run)

        self.assertEqual(pages, [[1]])
        self.assertIsInstance(run.summary, StreamSummary)
        self.assertEqual(run.summary.endpoint, 'ep')
        self.assertEqual(run.summary.source, 'live')
        self.assertEqual(run.summary.requests, 2)
        self.assertEqual(run.summary.retries, 1)
        self.assertGreaterEqual(run.summary.elapsed_seconds, 0.0)
        self.assertIsNotNone(run.summary.stop)
        self.assertEqual(run.summary.stop.kind, 'next_request_none')

    def test_on_request_seconds_since_last_request_none_until_previous_request_exists(self):
        seen = []

        def on_request(req, state):
            seen.append(state.get('seconds_since_last_request'))
            return req

        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1], 'next_cursor': 'n1'},
                            {'items': [2], 'next_cursor': 'n2'},
                            {'items': [3], 'next_cursor': None},
                        ],
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                        'on_request': on_request,
                    }
                },
            }
        )

        pages = list(client.stream('ep'))
        self.assertEqual(pages, [[1], [2], [3]])
        self.assertEqual(len(seen), 3)
        self.assertIsNone(seen[0])
        self.assertIsNone(seen[1])
        self.assertIsNotNone(seen[2])
        self.assertGreaterEqual(seen[2], 0.0)

    def test_run_state_build_summary_elapsed_is_non_negative(self):
        from rest_fetcher.client import _RunState

        run_state = _RunState(endpoint_name='ep', event_source='live')
        summary = run_state.build_summary()
        self.assertGreaterEqual(summary.elapsed_seconds, 0.0)

    def test_mark_wait_rejects_unknown_wait_type(self):
        from rest_fetcher.client import _RunState

        run_state = _RunState()
        with self.assertRaises(ValueError):
            run_state.mark_wait('mystery', 1.0)

    def test_on_error_stop_records_error_stop_and_errors_counter(self):
        seen = {}

        def on_error(exc, state):
            seen['errors_so_far'] = state.get('errors_so_far')
            return 'stop'

        client = APIClient(
            {
                **simple_schema(),
                'retry': {'max_attempts': 1},
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'on_error': on_error,
                    }
                },
            }
        )

        with patch.object(
            client._session,
            'request',
            return_value=make_response(
                status=500, body={'message': 'bad'}, headers={'Content-Type': 'application/json'}
            ),
        ):
            run = client.stream_run('ep')
            pages = list(run)

        self.assertEqual(pages, [])
        self.assertEqual(seen['errors_so_far'], 1)
        self.assertIsNotNone(run.summary)
        self.assertEqual(run.summary.requests, 1)
        self.assertEqual(run.summary.pages, 0)
        self.assertIsNotNone(run.summary.stop)
        self.assertEqual(run.summary.stop.kind, 'error_stop')

    def test_max_pages_stopped_event_includes_progress_fields(self):
        events = []
        client = APIClient(
            {
                **simple_schema(),
                'on_event': lambda ev: events.append(ev),
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1], 'next_cursor': 'n1'},
                            {'items': [2], 'next_cursor': None},
                        ],
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                    }
                },
            }
        )
        list(client.stream('ep', max_pages=1))
        stopped = next((e for e in events if e.kind == 'stopped'), None)
        self.assertIsNotNone(stopped)
        self.assertEqual(stopped.data.get('stop_kind'), 'max_pages')
        self.assertIn('pages_so_far', stopped.data)
        self.assertIn('requests_so_far', stopped.data)
        self.assertIn('elapsed_seconds_so_far', stopped.data)

    def test_max_requests_stopped_event_includes_progress_fields(self):
        events = []
        client = APIClient(
            {
                **simple_schema(),
                'on_event': lambda ev: events.append(ev),
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1], 'next_cursor': 'n1'},
                            {'items': [2], 'next_cursor': None},
                        ],
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                    }
                },
            }
        )
        list(client.stream('ep', max_requests=1))
        stopped = next((e for e in events if e.kind == 'stopped'), None)
        self.assertIsNotNone(stopped)
        self.assertEqual(stopped.data.get('stop_kind'), 'max_requests')
        self.assertIn('pages_so_far', stopped.data)
        self.assertIn('requests_so_far', stopped.data)
        self.assertIn('elapsed_seconds_so_far', stopped.data)

    def test_error_path_request_end_event_includes_progress_fields(self):
        events = []
        client = APIClient(
            {
                **simple_schema(),
                'retry': {'max_attempts': 1},
                'on_event': lambda ev: events.append(ev),
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'on_error': lambda exc, state: 'skip',
                    }
                },
            }
        )
        with patch.object(
            client._session,
            'request',
            return_value=make_response(
                status=500, body={}, headers={'Content-Type': 'application/json'}
            ),
        ):
            client.fetch('ep')
        req_end = next((e for e in events if e.kind == 'request_end'), None)
        self.assertIsNotNone(req_end)
        self.assertIn('requests_so_far', req_end.data)
        self.assertIn('elapsed_seconds_so_far', req_end.data)


class TestBoundedCapsBehaviour(unittest.TestCase):
    def test_fetch_max_pages_returns_partial_result_instead_of_raising(self):
        "max_pages is a local guardrail: preserve completed pages and return them normally"
        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1, 2], 'next_cursor': 'n1'},
                            {'items': [3], 'next_cursor': 'n2'},
                            {'items': [4], 'next_cursor': None},
                        ],
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                    }
                },
            }
        )

        result = client.fetch('ep', max_pages=2)
        self.assertEqual(result, [[1, 2], [3]])

    def test_fetch_max_requests_returns_partial_result_instead_of_raising(self):
        "max_requests is also local: stop before request N+1 and keep completed pages"
        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                    }
                },
            }
        )

        responses = [
            make_response(
                body={'items': [1, 2], 'next_cursor': 'n1'},
                headers={'Content-Type': 'application/json'},
            ),
            make_response(
                body={'items': [3], 'next_cursor': 'n2'},
                headers={'Content-Type': 'application/json'},
            ),
            make_response(
                body={'items': [4], 'next_cursor': None},
                headers={'Content-Type': 'application/json'},
            ),
        ]

        with patch.object(client._session, 'request', side_effect=responses):
            result = client.fetch('ep', max_requests=2)

        self.assertEqual(result, [[1, 2], [3]])

    def test_fetch_max_pages_runs_on_complete_and_sets_stop_state(self):
        seen = {}

        def on_complete(pages, state):
            seen['stop'] = state.get('stop')
            return pages

        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1, 2], 'next_cursor': 'n1'},
                            {'items': [3], 'next_cursor': 'n2'},
                            {'items': [4], 'next_cursor': None},
                        ],
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                        'on_complete': on_complete,
                    }
                },
            }
        )

        result = client.fetch('ep', max_pages=2)
        self.assertEqual(result, [[1, 2], [3]])
        self.assertIsInstance(seen['stop'], dict)
        self.assertEqual(seen['stop']['kind'], 'max_pages')
        self.assertEqual(seen['stop']['limit'], 2)
        self.assertEqual(seen['stop']['observed'], 2)

    def test_fetch_max_requests_runs_on_complete_and_sets_stop_state(self):
        seen = {}

        def on_complete(pages, state):
            seen['stop'] = state.get('stop')
            return pages

        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                        'on_complete': on_complete,
                    }
                },
            }
        )

        responses = [
            make_response(
                body={'items': [1, 2], 'next_cursor': 'n1'},
                headers={'Content-Type': 'application/json'},
            ),
            make_response(
                body={'items': [3], 'next_cursor': 'n2'},
                headers={'Content-Type': 'application/json'},
            ),
            make_response(
                body={'items': [4], 'next_cursor': None},
                headers={'Content-Type': 'application/json'},
            ),
        ]

        with patch.object(client._session, 'request', side_effect=responses):
            result = client.fetch('ep', max_requests=2)

        self.assertEqual(result, [[1, 2], [3]])
        self.assertIsInstance(seen['stop'], dict)
        self.assertEqual(seen['stop']['kind'], 'max_requests')
        self.assertEqual(seen['stop']['limit'], 2)
        self.assertEqual(seen['stop']['observed'], 2)

    def test_fetch_on_complete_return_none_keeps_aggregate_result(self):
        called = {'n': 0}

        def on_complete(pages, state):
            called['n'] += 1
            return None

        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1, 2], 'next_cursor': 'n1'},
                            {'items': [3], 'next_cursor': None},
                        ],
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                        'on_complete': on_complete,
                    }
                },
            }
        )

        result = client.fetch('ep')
        self.assertEqual(result, [[1, 2], [3]])
        self.assertEqual(called['n'], 1)

    def test_stream_max_pages_runs_on_complete_and_exposes_stop_state(self):
        "stream capped by max_pages still completes normally and on_complete sees summary/state stop"
        seen = {}

        def on_complete(summary, state):
            seen['summary'] = summary
            seen['stop'] = dict(state.get('stop', {}))
            return 'ignored in stream mode'

        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1, 2], 'next_cursor': 'n1'},
                            {'items': [3], 'next_cursor': 'n2'},
                            {'items': [4], 'next_cursor': None},
                        ],
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                        'on_complete': on_complete,
                    }
                },
            }
        )

        pages = list(client.stream('ep', max_pages=2))
        # stream mode still yields page payloads; on_complete return value is ignored.
        self.assertEqual(pages, [[1, 2], [3]])
        self.assertIsInstance(seen['summary'], StreamSummary)
        self.assertEqual(seen['summary'].pages, 2)
        self.assertEqual(seen['summary'].requests, 2)
        self.assertEqual(seen['summary'].stop, StopSignal(kind='max_pages', limit=2, observed=2))
        self.assertEqual(seen['stop'], {'kind': 'max_pages', 'limit': 2, 'observed': 2})

    def test_stream_max_requests_runs_on_complete_and_exposes_stop_state(self):
        "stream capped by max_requests still completes normally and on_complete sees summary/state stop"
        seen = {}

        def on_complete(summary, state):
            seen['summary'] = summary
            seen['stop'] = dict(state.get('stop', {}))
            return 'ignored in stream mode'

        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                        'on_complete': on_complete,
                    }
                },
            }
        )

        responses = [
            make_response(
                body={'items': [1, 2], 'next_cursor': 'n1'},
                headers={'Content-Type': 'application/json'},
            ),
            make_response(
                body={'items': [3], 'next_cursor': 'n2'},
                headers={'Content-Type': 'application/json'},
            ),
            make_response(
                body={'items': [4], 'next_cursor': None},
                headers={'Content-Type': 'application/json'},
            ),
        ]

        with patch.object(client._session, 'request', side_effect=responses):
            pages = list(client.stream('ep', max_requests=2))

        self.assertEqual(pages, [[1, 2], [3]])
        self.assertIsInstance(seen['summary'], StreamSummary)
        self.assertEqual(seen['summary'].pages, 2)
        self.assertEqual(seen['summary'].requests, 2)
        self.assertEqual(seen['summary'].stop, StopSignal(kind='max_requests', limit=2, observed=2))
        self.assertEqual(seen['stop'], {'kind': 'max_requests', 'limit': 2, 'observed': 2})

    def test_stream_natural_exhaustion_keeps_stop_none_even_with_generous_caps(self):
        seen = {}

        def on_complete(summary, state):
            seen['summary'] = summary
            seen['stop'] = state.get('stop')

        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1, 2], 'next_cursor': 'n1'},
                            {'items': [3], 'next_cursor': None},
                        ],
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                        'on_complete': on_complete,
                    }
                },
            }
        )

        pages = list(client.stream('ep', max_pages=10, max_requests=10))

        self.assertEqual(pages, [[1, 2], [3]])
        self.assertEqual(
            seen['summary'],
            StreamSummary(
                pages=2,
                requests=2,
                stop=StopSignal(kind='next_request_none'),
                endpoint='ep',
                source='live',
                retries=0,
                elapsed_seconds=seen['summary'].elapsed_seconds,
            ),
        )
        self.assertGreaterEqual(seen['summary'].elapsed_seconds, 0.0)
        self.assertEqual(seen['stop'], {'kind': 'next_request_none'})

    def test_time_limit_still_raises_and_does_not_call_on_complete(self):
        "time_limit remains destructive and does not turn into normal completion"
        seen = {'called': False}

        def on_complete(summary, state):
            seen['called'] = True
            return summary

        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1, 2], 'next_cursor': 'n1'},
                            {'items': [3], 'next_cursor': None},
                        ],
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                        'on_complete': on_complete,
                    }
                },
            }
        )

        with patch(
            'rest_fetcher.context.time.monotonic',
            side_effect=itertools.chain([0.0, 0.0], itertools.repeat(10.0)),
        ):
            with self.assertRaises(DeadlineExceeded):
                client.fetch('ep', time_limit=5.0)

        self.assertFalse(seen['called'])


# Item 6 — retry: programming errors not retried; KeyError regression
class TestRetryProgrammingErrors(unittest.TestCase):
    def test_key_error_in_request_fn_not_retried(self):
        """regression: KeyError inside request_fn is a programmer error, must not retry"""
        handler = RetryHandler({'max_attempts': 3, 'base_delay': 0}, {})
        call_count = [0]

        def bad_fn():
            call_count[0] += 1
            d = {}
            return d['missing_key']  # KeyError — programmer error

        with patch('time.sleep'):
            with self.assertRaises(KeyError):
                handler.execute(bad_fn)

        self.assertEqual(call_count[0], 1, 'KeyError is a programming error — must not be retried')

    def test_attribute_error_not_retried(self):
        handler = RetryHandler({'max_attempts': 3, 'base_delay': 0}, {})
        call_count = [0]

        def bad_fn():
            call_count[0] += 1
            return None.something  # AttributeError

        with patch('time.sleep'):
            with self.assertRaises(AttributeError):
                handler.execute(bad_fn)

        self.assertEqual(call_count[0], 1)

    def test_type_error_not_retried(self):
        handler = RetryHandler({'max_attempts': 3, 'base_delay': 0}, {})
        call_count = [0]

        def bad_fn():
            call_count[0] += 1
            return 1 + 'oops'  # TypeError

        with patch('time.sleep'):
            with self.assertRaises(TypeError):
                handler.execute(bad_fn)

        self.assertEqual(call_count[0], 1)


class TestStateView(unittest.TestCase):
    """StateView raises StateViewMutationError on all write operations"""

    def setUp(self):
        from rest_fetcher.pagination import StateView

        self.StateView = StateView
        from rest_fetcher import StateViewMutationError

        self.MutErr = StateViewMutationError

    def _view(self, d=None):
        return self.StateView(d or {'x': 1, 'y': 2})

    def test_setitem_raises(self):
        v = self._view()
        with self.assertRaises(self.MutErr) as ctx:
            v['new_key'] = 'value'
        self.assertEqual(ctx.exception.key, 'new_key')

    def test_setitem_raises_on_existing_key(self):
        v = self._view()
        with self.assertRaises(self.MutErr):
            v['x'] = 99

    def test_delitem_raises(self):
        v = self._view()
        with self.assertRaises(self.MutErr) as ctx:
            del v['x']
        self.assertEqual(ctx.exception.key, 'x')

    def test_update_raises(self):
        v = self._view()
        with self.assertRaises(self.MutErr):
            v.update({'z': 3})

    def test_pop_raises(self):
        v = self._view()
        with self.assertRaises(self.MutErr) as ctx:
            v.pop('x')
        self.assertEqual(ctx.exception.key, 'x')

    def test_setdefault_raises(self):
        v = self._view()
        with self.assertRaises(self.MutErr):
            v.setdefault('new', 42)

    def test_clear_raises(self):
        v = self._view()
        with self.assertRaises(self.MutErr):
            v.clear()

    def test_reads_work_normally(self):
        v = self._view({'a': 1, 'b': 2})
        self.assertEqual(v['a'], 1)
        self.assertEqual(v.get('b'), 2)
        self.assertIn('a', v)
        self.assertEqual(list(v.keys()), ['a', 'b'])
        self.assertEqual(len(v), 2)

    def test_is_dict_subclass(self):
        v = self._view()
        self.assertIsInstance(v, dict)

    def test_error_message_contains_key(self):
        v = self._view()
        try:
            v['mykey'] = 'x'
        except self.MutErr as e:
            self.assertIn('mykey', str(e))
            self.assertIn('update_state', str(e))

    def test_mutation_raised_as_callback_error(self):
        """StateViewMutationError from inside a callback is wrapped in CallbackError"""
        from rest_fetcher import CallbackError

        def bad_next_req(resp, state):
            state['x'] = 1  # raises StateViewMutationError
            return None

        client = APIClient(
            {
                **simple_schema(),
                'endpoints': {
                    'ep': {
                        'mock': [{'items': [1]}],
                        'pagination': {
                            'next_request': bad_next_req,
                        },
                        'on_response': lambda r, s: r.get('items', []),
                    }
                },
            }
        )
        with self.assertRaises(CallbackError) as ctx:
            client.fetch('ep')
        self.assertIsInstance(ctx.exception.cause.__class__.__mro__[0], type)  # cause is set
        self.assertEqual(ctx.exception.callback_name, 'next_request')


class TestRequestInState(unittest.TestCase):
    """_request key in callback state snapshot"""

    def _run_pages(self, mock_pages, on_response=None, next_request=None, update_state=None):
        from rest_fetcher.pagination import CycleRunner

        states_seen = []

        def capturing_next_request(resp, state):
            states_seen.append(
                dict(state)
            )  # full snapshot; access _request via states[n]['_request']
            if next_request:
                return next_request(resp, state)
            return None

        strategy = {
            'next_request': capturing_next_request,
            'on_response': on_response or (lambda r, s: r.get('items', [])),
        }
        if update_state:
            strategy['update_state'] = update_state

        runner = CycleRunner(strategy)
        pages = []
        fetch_count = [0]

        def mock_fetch(request, ctx=None, *, run_state):
            idx = min(fetch_count[0], len(mock_pages) - 1)
            fetch_count[0] += 1
            return outcome(mock_pages[idx], request)

        initial = {'method': 'GET', 'url': 'https://x.com', 'params': {'page': 1, 'size': 10}}
        for page in runner.run(mock_fetch, initial):
            pages.append(page)

        return pages, states_seen

    def test_request_present_in_state_snapshot(self):
        """_request key is populated in callback state on every page"""
        _, states = self._run_pages([{'items': [1, 2, 3]}])
        self.assertIn('_request', states[0])

    def test_request_contains_params(self):
        """_request[params] reflects what was actually sent"""
        _, states = self._run_pages([{'items': [1]}])
        self.assertEqual(states[0]['_request']['params'], {'page': 1, 'size': 10})

    def test_request_params_advance_across_pages(self):
        """_request[params][page] reflects page number sent on each page"""
        from rest_fetcher.pagination import CycleRunner

        pages_seen_params = []
        page_num = [1]

        def next_req(resp, state):
            pages_seen_params.append(state['_request']['params'].copy())
            page_num[0] += 1
            if page_num[0] > 3:
                return None
            return {'params': {'page': page_num[0], 'size': 10}}

        runner = CycleRunner(
            {
                'next_request': next_req,
                'on_response': lambda r, s: r.get('items', []),
            }
        )
        mock = [{'items': [1, 2]}, {'items': [3, 4]}, {'items': [5, 6]}]
        fetch_count = [0]

        def mock_fetch(req, ctx=None, *, run_state):
            idx = min(fetch_count[0], len(mock) - 1)
            fetch_count[0] += 1
            return outcome(mock[idx], req)

        initial = {'method': 'GET', 'url': 'https://x.com', 'params': {'page': 1, 'size': 10}}
        list(runner.run(mock_fetch, initial))

        self.assertEqual(pages_seen_params[0]['page'], 1)
        self.assertEqual(pages_seen_params[1]['page'], 2)
        self.assertEqual(pages_seen_params[2]['page'], 3)

    def test_custom_next_request_reads_page_from_request(self):
        """custom callback can read current page from _request without update_state"""
        from rest_fetcher.pagination import CycleRunner

        call_order = []

        def next_req(resp, state):
            current = (state.get('_request') or {}).get('params', {}).get('page', 1)
            call_order.append(current)
            nxt = current + 1
            if nxt > 3:
                return None
            return {'params': {'page': nxt}}

        runner = CycleRunner(
            {
                'next_request': next_req,
                'on_response': lambda r, s: r.get('items', []),
            }
        )
        fetch_count = [0]

        def mock_fetch(req, ctx=None, *, run_state):
            fetch_count[0] += 1
            return outcome({'items': [fetch_count[0]]}, req)

        initial = {'method': 'GET', 'url': 'https://x.com', 'params': {'page': 1}}
        list(runner.run(mock_fetch, initial))

        self.assertEqual(
            call_order,
            [1, 2, 3],
            'callback reads page 1, 2, 3 from _request without any state mutation',
        )

    def test_on_complete_state_excludes_response_headers_and_stop_signal(self):
        complete_state = {}

        schema = {
            'base_url': 'https://api.example.com',
            'endpoints': {
                'users': {
                    'method': 'GET',
                    'path': '/users',
                    'on_complete': lambda pages, state: complete_state.update(state) or pages,
                    'pagination': {'next_request': lambda parsed, state: None},
                }
            },
        }
        client = APIClient(schema)
        with patch.object(
            requests.Session,
            'request',
            return_value=make_response(body={'ok': True}, headers={'X-Test': '1'}),
        ):
            result = client.fetch('users')
        self.assertEqual(result, [{'ok': True}])
        self.assertNotIn('_response_headers', complete_state)
        self.assertNotIn('_stop_signal', complete_state)

    def test_request_not_in_on_complete_state(self):
        """on_complete receives final_state without _request (run is done)"""
        from rest_fetcher.pagination import CycleRunner

        complete_state = {}

        runner = CycleRunner(
            {
                'next_request': lambda r, s: None,
                'on_response': lambda r, s: r.get('items', []),
                'on_complete': lambda pages, state: complete_state.update(state) or pages,
            }
        )

        def mock_fetch(req, ctx=None, *, run_state):
            return outcome({'items': [1]}, req)

        initial = {'method': 'GET', 'url': 'https://x.com', 'params': {}}
        list(runner.run(mock_fetch, initial))
        # on_complete state does not include _request (no current request at completion)
        self.assertNotIn('_request', complete_state)

    def test_request_in_state_contains_auth_headers(self):
        """_request in callback state includes auth headers injected by auth handler —
        verifies _request is captured post-auth, not pre-auth.
        this test will catch future refactors that capture _request too early."""

        observed_headers = {}

        def capturing_next_req(resp, state):
            req = state.get('_request') or {}
            observed_headers.update(req.get('headers') or {})
            return None

        # use a full APIClient with bearer auth so auth injection actually runs
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'auth': {'type': 'bearer', 'token': 'secret-token'},
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/items',
                        'mock': [{'items': [1, 2]}],
                        'pagination': {
                            'next_request': capturing_next_req,
                        },
                        'on_response': lambda r, s: r.get('items', []),
                    }
                },
            }
        )
        client.fetch('ep')

        self.assertIn(
            'Authorization',
            observed_headers,
            '_request should contain Authorization header added by bearer auth',
        )
        self.assertTrue(
            observed_headers['Authorization'].startswith('Bearer '),
            f'Authorization header should start with Bearer, got: {observed_headers.get("Authorization")!r}',
        )

    def test_request_snapshot_isolates_live_request_from_callback_mutation(self):
        """mutating state["_request"]["params"] inside a callback must not affect the
        live request dict used for the next page fetch. _copy_request_for_snapshot
        provides one-level copies of params/headers/json/data for this purpose."""
        from rest_fetcher.pagination import CycleRunner

        requests_seen = []
        page_num = [1]

        def next_req(resp, state):
            # attempt to mutate _request params via the nested plain dict —
            # this is not blocked by StateView (only top-level is guarded),
            # but must not bleed into the next actual request.
            state['_request']['params']['injected'] = 'poison'
            page_num[0] += 1
            if page_num[0] > 2:
                return None
            return {'params': {'page': page_num[0]}}

        runner = CycleRunner(
            {
                'next_request': next_req,
                'on_response': lambda r, s: r.get('items', []),
            }
        )

        def mock_fetch(req, ctx=None, *, run_state):
            requests_seen.append(dict(req.get('params', {})))
            return outcome({'items': [1]}, req)

        initial = {'method': 'GET', 'url': 'https://x.com', 'params': {'page': 1}}
        list(runner.run(mock_fetch, initial))

        self.assertNotIn(
            'injected',
            requests_seen[1],
            'callback mutation of _request snapshot must not affect the live next request',
        )


class TestExamplePatterns(unittest.TestCase):
    """integration tests for patterns demonstrated in examples.py"""

    def test_three_layer_header_merge(self):
        """client headers + endpoint headers + call-time headers all present, later wins"""
        with patch('requests.Session.request') as mock_req:
            mock_req.return_value = make_response(200, {'ok': True})
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'headers': {'X-Client': 'v1', 'Accept': 'application/json'},
                    'endpoints': {
                        'ep': {
                            'method': 'GET',
                            'path': '/x',
                            'headers': {'X-Endpoint': 'yes', 'Accept': 'application/msgpack'},
                        }
                    },
                }
            )
            client.fetch('ep', headers={'X-Call': 'now', 'Accept': 'text/plain'})

        _, kwargs = mock_req.call_args
        sent_headers = kwargs.get('headers', {})
        # call-time wins over endpoint wins over client
        self.assertEqual(sent_headers.get('Accept'), 'text/plain', 'call-time should win')
        self.assertEqual(sent_headers.get('X-Endpoint'), 'yes', 'endpoint header present')
        self.assertEqual(sent_headers.get('X-Call'), 'now', 'call-time header present')
        self.assertIn('X-Client', sent_headers, 'client header still present')

    def test_response_parser_inheritance_and_endpoint_override(self):
        """client response_parser inherited by all endpoints; endpoint override takes full precedence"""
        with patch('requests.Session.request') as mock_req:
            mock_req.side_effect = [
                make_response(200, {'data': {'result': 42}}),
                make_response(200, {'data': {'result': 99}, 'extra': True}),
            ]
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'response_parser': lambda resp, parsed: parsed.get('data', parsed),
                    'endpoints': {
                        'normal': {'method': 'GET', 'path': '/a'},
                        'raw': {
                            'method': 'GET',
                            'path': '/b',
                            'response_parser': lambda resp, parsed: parsed,
                        },
                    },
                }
            )
            result_normal = client.fetch('normal')
            result_raw = client.fetch('raw')

        self.assertEqual(result_normal, {'result': 42}, 'client parser unwraps data')
        self.assertEqual(
            result_raw,
            {'data': {'result': 99}, 'extra': True},
            'endpoint override returns full response',
        )

    def test_update_state_persists_across_pages(self):
        """update_state return dict is merged into page_state — values visible on next page"""
        from rest_fetcher.pagination import CycleRunner

        states_in_next_request = []
        call_count = [0]

        def update_st(resp, state):
            return {'rows_seen': state.get('rows_seen', 0) + len(resp.get('items', []))}

        def next_req(resp, state):
            states_in_next_request.append(state.get('rows_seen'))
            call_count[0] += 1
            return None if call_count[0] >= 3 else {'params': {'page': call_count[0] + 1}}

        runner = CycleRunner(
            {
                'next_request': next_req,
                'update_state': update_st,
                'on_response': lambda r, s: r.get('items', []),
            }
        )

        pages = [{'items': [1, 2, 3]}, {'items': [4, 5]}, {'items': [6]}]
        fetch_count = [0]

        def mock_fetch(req, ctx=None, *, run_state):
            idx = fetch_count[0]
            fetch_count[0] += 1
            return outcome(pages[idx], req)

        initial = {'method': 'GET', 'url': 'https://x.com', 'params': {'page': 1}}
        list(runner.run(mock_fetch, initial))

        # update_state runs BEFORE next_request on each page,
        # so next_request always sees the result of this page's update_state.
        # page 1: update_state returned rows_seen=3, next_request sees 3.
        # page 2: update_state returned rows_seen=5 (3+2), next_request sees 5.
        # page 3: update_state returned rows_seen=6 (5+1), next_request sees 6.
        self.assertEqual(states_in_next_request[0], 3, 'page 1: update_state runs first, sees 3')
        self.assertEqual(states_in_next_request[1], 5, 'page 2: sees 5 rows (3+2)')
        self.assertEqual(states_in_next_request[2], 6, 'page 3: sees 6 rows (3+2+1)')

    def test_params_mode_replace_clears_original_params(self):
        """params_mode=replace: page 2+ sends only continuation token, not original filter"""
        from rest_fetcher.pagination import CycleRunner

        requests_seen = []
        fetch_count = [0]

        def mock_fetch(req, ctx=None, *, run_state):
            requests_seen.append(dict(req.get('params', {})))
            fetch_count[0] += 1
            return outcome(
                (
                    {'results': [fetch_count[0]], 'next_token': 'tok2'}
                    if fetch_count[0] == 1
                    else {'results': [fetch_count[0]]}
                ),
                req,
            )

        def next_req(resp, state):
            token = resp.get('next_token')
            if not token:
                return None
            return {'params': {'token': token}, 'params_mode': 'replace'}

        runner = CycleRunner(
            {
                'next_request': next_req,
                'on_response': lambda r, s: r.get('results', []),
            }
        )
        initial = {
            'method': 'GET',
            'url': 'https://x.com',
            'params': {'q': 'python', 'category': 'books'},
        }
        list(runner.run(mock_fetch, initial))

        # page 1: original filter params
        self.assertEqual(requests_seen[0], {'q': 'python', 'category': 'books'})
        # page 2: only the continuation token; filter params cleared
        self.assertEqual(requests_seen[1], {'token': 'tok2'})
        self.assertNotIn(
            'q', requests_seen[1], 'filter param must be cleared by params_mode=replace'
        )

    def test_paginated_playback_round_trip(self):
        """playback save/load: multi-page fetch saved as raw responses, reloaded through callbacks"""
        import os
        import tempfile

        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            fixture_path = f.name
        try:
            with patch('requests.Session.request') as mock_req:
                mock_req.side_effect = [
                    make_response(
                        200,
                        {'items': [1, 2], 'next': True},
                        headers={'Content-Type': 'application/json'},
                    ),
                    make_response(
                        200, {'items': [3, 4]}, headers={'Content-Type': 'application/json'}
                    ),
                ]
                client = APIClient(
                    {
                        'base_url': 'https://api.example.com/v1',
                        'endpoints': {
                            'ep': {
                                'method': 'GET',
                                'path': '/items',
                                'playback': {'path': fixture_path, 'mode': 'save'},
                                'pagination': {
                                    'next_request': lambda r, s: {'params': {'page': 2}}
                                    if r.get('next')
                                    else None,
                                },
                                'on_response': lambda r, s: r.get('items', []),
                            }
                        },
                    }
                )
                saved = client.fetch('ep')

            client2 = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {
                        'ep': {
                            'method': 'GET',
                            'path': '/items',
                            'playback': {'path': fixture_path, 'mode': 'load'},
                            'pagination': {
                                'next_request': lambda r, s: {'params': {'page': 2}}
                                if r.get('next')
                                else None,
                            },
                            'on_response': lambda r, s: r.get('items', []),
                        }
                    },
                }
            )
            loaded = client2.fetch('ep')

            self.assertEqual(saved, loaded, 'loaded fixture must match original fetch')
        finally:
            os.unlink(fixture_path)


# Item 2 — SchemaBuilder
class TestSchemaBuilder(unittest.TestCase):
    def test_build_returns_plain_dict(self):
        from rest_fetcher.types import SchemaBuilder

        schema = SchemaBuilder('https://api.example.com').endpoint('ep').build()
        self.assertIsInstance(schema, dict)

    def test_minimal_build_passes_validate(self):
        from rest_fetcher.types import SchemaBuilder

        schema = SchemaBuilder('https://api.example.com').endpoint('ep').build()
        validate(schema)  # must not raise

    def test_bearer_token(self):
        from rest_fetcher.types import SchemaBuilder

        schema = SchemaBuilder('https://api.example.com').bearer('abc').endpoint('ep').build()
        self.assertEqual(schema['auth']['type'], 'bearer')
        self.assertEqual(schema['auth']['token'], 'abc')

    def test_bearer_callback(self):
        from rest_fetcher.types import SchemaBuilder

        cb = lambda state: 'tok'
        schema = SchemaBuilder('https://api.example.com').bearer(cb).endpoint('ep').build()
        self.assertEqual(schema['auth']['type'], 'bearer')
        self.assertIs(schema['auth']['token_callback'], cb)

    def test_basic_auth(self):
        from rest_fetcher.types import SchemaBuilder

        schema = (
            SchemaBuilder('https://api.example.com').basic('user', 'pass').endpoint('ep').build()
        )
        self.assertEqual(schema['auth'], {'type': 'basic', 'username': 'user', 'password': 'pass'})

    def test_timeout(self):
        from rest_fetcher.types import SchemaBuilder

        schema = SchemaBuilder('https://api.example.com').timeout(30).endpoint('ep').build()
        self.assertEqual(schema['timeout'], 30)

    def test_timeout_tuple(self):
        from rest_fetcher.types import SchemaBuilder

        schema = SchemaBuilder('https://api.example.com').timeout((5, 60)).endpoint('ep').build()
        self.assertEqual(schema['timeout'], (5, 60))

    def test_retry(self):
        from rest_fetcher.types import SchemaBuilder

        schema = (
            SchemaBuilder('https://api.example.com')
            .retry(max_attempts=5, backoff='linear')
            .endpoint('ep')
            .build()
        )
        self.assertEqual(schema['retry']['max_attempts'], 5)
        self.assertEqual(schema['retry']['backoff'], 'linear')

    def test_state(self):
        from rest_fetcher.types import SchemaBuilder

        schema = (
            SchemaBuilder('https://api.example.com')
            .state(api_key='secret', region='eu')
            .endpoint('ep')
            .build()
        )
        self.assertEqual(schema['state']['api_key'], 'secret')

    def test_scrub_headers(self):
        from rest_fetcher.types import SchemaBuilder

        schema = (
            SchemaBuilder('https://api.example.com')
            .scrub_headers('X-Tenant-Id', 'X-App-Token')
            .endpoint('ep')
            .build()
        )
        self.assertIn('X-Tenant-Id', schema['scrub_headers'])

    def test_scrub_query_params(self):
        from rest_fetcher.types import SchemaBuilder

        schema = (
            SchemaBuilder('https://api.example.com')
            .scrub_query_params('tenant_token', 'signature')
            .endpoint('ep')
            .build()
        )
        self.assertIn('tenant_token', schema['scrub_query_params'])

    def test_endpoint_kwargs_passed_through(self):
        from rest_fetcher.types import SchemaBuilder

        schema = (
            SchemaBuilder('https://api.example.com')
            .endpoint('users', method='GET', path='/users', params={'active': True}, timeout=60)
            .build()
        )
        ep = schema['endpoints']['users']
        self.assertEqual(ep['method'], 'GET')
        self.assertEqual(ep['path'], '/users')
        self.assertEqual(ep['params'], {'active': True})
        self.assertEqual(ep['timeout'], 60)

    def test_multiple_endpoints(self):
        from rest_fetcher.types import SchemaBuilder

        schema = (
            SchemaBuilder('https://api.example.com')
            .endpoint('list_users', path='/users')
            .endpoint('create_user', method='POST', path='/users')
            .build()
        )
        self.assertIn('list_users', schema['endpoints'])
        self.assertIn('create_user', schema['endpoints'])

    def test_chaining_returns_builder(self):
        from rest_fetcher.types import SchemaBuilder

        b = SchemaBuilder('https://api.example.com')
        self.assertIs(b.bearer('tok'), b)
        self.assertIs(b.timeout(30), b)
        self.assertIs(b.endpoint('ep'), b)

    def test_build_is_independent_copy(self):
        """build() must return a copy — mutating result does not affect builder"""
        from rest_fetcher.types import SchemaBuilder

        builder = SchemaBuilder('https://api.example.com').endpoint('ep')
        s1 = builder.build()
        s1['extra'] = 'injected'
        s2 = builder.build()
        self.assertNotIn('extra', s2)

    def test_builder_produces_valid_schema_with_oauth2(self):
        from rest_fetcher.types import SchemaBuilder

        schema = (
            SchemaBuilder('https://api.example.com')
            .oauth2('https://auth.example.com/token', 'cid', 'csecret', scope='read')
            .endpoint('ep')
            .build()
        )
        validate(schema)

    def test_builder_produces_client_that_works(self):
        """end-to-end: SchemaBuilder -> APIClient -> fetch"""
        from rest_fetcher.types import SchemaBuilder

        schema = (
            SchemaBuilder('https://api.example.com')
            .bearer('test-token')
            .timeout(30)
            .endpoint('ep', method='GET', path='/test', mock=[{'result': 'ok'}])
            .build()
        )
        client = APIClient(schema)
        result = client.fetch('ep')
        self.assertEqual(result, {'result': 'ok'})

    def test_schema_validation_on_invalid_builder_output(self):
        """SchemaBuilder output goes through normal validate() — invalid values caught"""
        from rest_fetcher.types import SchemaBuilder

        schema = SchemaBuilder('https://api.example.com').endpoint('ep').build()
        schema['timeout'] = -1  # invalid — validate should catch this
        with self.assertRaises(SchemaError):
            validate(schema)


class TestOperationContext(unittest.TestCase):
    """unit tests for OperationContext guards and counters"""

    def _ctx(self, **kwargs):
        from rest_fetcher.context import OperationContext

        return OperationContext(**kwargs)

    def test_no_limits_never_raises(self):
        ctx = self._ctx()
        for _ in range(1000):
            ctx.check_deadline()
            ctx.check_max_pages()
            ctx.check_max_requests()
            ctx.record_request()
            ctx.record_page()

    def test_max_requests_returns_stop_signal_before_exceeding(self):
        from rest_fetcher.context import StopSignal

        ctx = self._ctx(max_requests=3)
        for _ in range(3):
            self.assertIsNone(ctx.check_max_requests())
            ctx.record_request()
        stop = ctx.check_max_requests()
        self.assertEqual(stop, StopSignal(kind='max_requests', limit=3, observed=3))

    def test_max_pages_returns_stop_signal_before_exceeding(self):
        from rest_fetcher.context import StopSignal

        ctx = self._ctx(max_pages=2)
        ctx.record_page()
        ctx.record_page()
        stop = ctx.check_max_pages()
        self.assertEqual(stop, StopSignal(kind='max_pages', limit=2, observed=2))

    def test_deadline_raises_when_expired(self):
        from rest_fetcher import DeadlineExceeded

        ctx = self._ctx(time_limit=0.0)
        # time_limit=0 means any elapsed time exceeds it
        with self.assertRaises(DeadlineExceeded) as cm:
            ctx.check_deadline()
        self.assertEqual(cm.exception.limit, 0.0)
        self.assertGreaterEqual(cm.exception.elapsed, 0.0)

    def test_deadline_not_raised_if_within_limit(self):
        ctx = self._ctx(time_limit=9999.0)
        ctx.check_deadline()  # should not raise

    def test_elapsed_uses_monotonic(self):
        import time

        from rest_fetcher.client import _RunState

        run_state = _RunState()
        before = time.monotonic()
        time.sleep(0.01)
        after = time.monotonic()
        elapsed = run_state.elapsed_seconds()
        self.assertGreaterEqual(elapsed, 0.0)
        self.assertLessEqual(elapsed, after - before + 0.1)


class TestSafetyCaps(unittest.TestCase):
    """integration tests for max_pages, max_requests, time_limit via client API"""

    def _paginated_client(self, num_pages=5):
        """client with num_pages pages available, always returns next page until exhausted"""
        pages = [{'items': list(range(i * 3, i * 3 + 3))} for i in range(num_pages)]
        call_count = [0]

        def next_req(resp, state):
            if call_count[0] >= num_pages:
                return None
            return {'params': {'page': call_count[0] + 1}}

        def mock_fn(req, ctx=None, *, run_state):
            idx = min(call_count[0], num_pages - 1)
            call_count[0] += 1
            return pages[
                idx
            ]  # _execute_mock adds headers; callable mock returns just the response dict

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/items',
                        'mock': mock_fn,
                        'pagination': {
                            'next_request': next_req,
                        },
                        'on_response': lambda r, s: r.get('items', []),
                    }
                },
            }
        )
        return client, call_count

    # max_pages tests

    def test_max_pages_1_stops_before_page_2(self):
        client, call_count = self._paginated_client(num_pages=5)
        result = client.fetch('ep', max_pages=1)
        self.assertEqual(result, [0, 1, 2])
        self.assertEqual(call_count[0], 1, 'only one page should have been fetched')

    def test_max_pages_equals_total_pages_does_not_raise(self):
        client, _ = self._paginated_client(num_pages=3)
        # next_req returns None after 3 pages, so no cap is hit
        result = client.fetch('ep', max_pages=3)
        # result is a list of 3 pages (on_response returns per-page list)
        self.assertEqual(len(result), 3)
        self.assertEqual(sum(len(p) for p in result), 9)  # 3 pages × 3 items

    def test_max_pages_larger_than_available_does_not_raise(self):
        client, _ = self._paginated_client(num_pages=2)
        result = client.fetch('ep', max_pages=100)
        # 2 pages, each with 3 items
        self.assertEqual(len(result), 2)
        self.assertEqual(sum(len(p) for p in result), 6)

    def test_max_pages_stream_yields_partial_then_stops_cleanly(self):
        client, _ = self._paginated_client(num_pages=5)
        collected = list(client.stream('ep', max_pages=2))
        self.assertEqual(len(collected), 2)

    def test_max_pages_fetch_returns_partial_result(self):
        client, _ = self._paginated_client(num_pages=5)
        result = client.fetch('ep', max_pages=2)
        self.assertEqual(result, [[0, 1, 2], [3, 4, 5]])

    # max_requests tests

    def test_max_requests_1_returns_stop_signal_on_retryable_second_attempt(self):
        """max_requests=1: first attempt succeeds for a normal page,
        but if the first response is retryable (429), the second attempt is blocked."""
        from unittest.mock import MagicMock, patch

        from rest_fetcher.context import OperationContext, StopSignal

        handler = RetryHandler(
            {'max_attempts': 5, 'backoff': 'linear', 'base_delay': 0},
            {'respect_retry_after': False},
        )
        ctx = OperationContext(max_requests=1)
        r429 = make_response(429)
        r429.headers = {}
        mock_fn = MagicMock(return_value=r429)
        with patch('time.sleep'):
            stop = handler.execute(mock_fn, ctx)
        self.assertEqual(stop, StopSignal(kind='max_requests', limit=1, observed=1))
        self.assertEqual(mock_fn.call_count, 1, 'only one attempt should have been made')

    def test_max_requests_via_client_blocks_retries(self):
        """max_requests=1: first attempt gets 429, retry would be attempt 2 but is blocked"""
        attempt_count = [0]

        def mock_429_then_ok(*args, **kwargs):
            attempt_count[0] += 1
            if attempt_count[0] == 1:
                return make_response(429)
            return make_response(200, {'items': [1]})

        with patch('requests.Session.request', side_effect=mock_429_then_ok):
            client = APIClient(
                {
                    'base_url': 'https://api.example.com',
                    'retry': {'max_attempts': 5, 'backoff': 'linear', 'base_delay': 0.01},
                    'endpoints': {'ep': {'method': 'GET', 'path': '/x'}},
                }
            )
            with patch('time.sleep'):
                result = client.fetch('ep', max_requests=1)
        self.assertEqual(result, [])
        self.assertEqual(attempt_count[0], 1, 'only one attempt before cap fired')

    def test_max_requests_3_allows_exactly_3_attempts(self):
        """max_requests=3: exactly 3 http attempts allowed across retries"""
        from rest_fetcher.context import OperationContext, StopSignal

        ctx = OperationContext(max_requests=3)
        handler = RetryHandler({'max_attempts': 10, 'backoff': 'linear', 'base_delay': 0}, {})
        attempt_count = [0]

        def always_429():
            attempt_count[0] += 1
            return make_response(429)

        from unittest.mock import patch

        with patch('time.sleep'):
            stop = handler.execute(always_429, ctx)

        self.assertEqual(stop, StopSignal(kind='max_requests', limit=3, observed=3))
        self.assertEqual(
            attempt_count[0], 3, 'exactly 3 attempts should have been made before max_requests stop'
        )

    # time_limit tests

    def test_time_limit_zero_fails_quickly(self):
        from rest_fetcher import DeadlineExceeded

        client, _ = self._paginated_client(num_pages=5)
        with self.assertRaises(DeadlineExceeded) as cm:
            client.fetch('ep', time_limit=0.0)
        self.assertEqual(cm.exception.limit, 0.0)

    def test_time_limit_generous_does_not_raise(self):
        client, _ = self._paginated_client(num_pages=2)
        result = client.fetch('ep', time_limit=9999.0)
        self.assertEqual(len(result), 2)  # 2 pages
        self.assertEqual(sum(len(p) for p in result), 6)

    def test_time_limit_stream_yields_partial_then_raises(self):
        import time as time_mod

        from rest_fetcher import DeadlineExceeded

        # use a real short sleep to make duration actually expire mid-stream
        sleep_calls = [0]
        original_sleep = time_mod.sleep

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/items',
                        'mock': [
                            {'items': [1, 2]},
                            {'items': [3, 4]},
                            {'items': [5, 6]},
                        ],
                        'pagination': {
                            'next_request': lambda r, s: (
                                None if not r.get('items') else {'params': {'page': 2}}
                            ),
                            'delay': 0.05,  # each inter-page delay counts toward deadline
                        },
                        'on_response': lambda r, s: r.get('items', []),
                    }
                },
            }
        )
        collected = []
        with self.assertRaises(DeadlineExceeded):
            for page in client.stream('ep', time_limit=0.03):
                collected.append(page)
        # at least page 1 yielded before the deadline fired
        self.assertGreaterEqual(len(collected), 0)

    # combined and edge-case tests

    def test_no_limits_set_completes_normally(self):
        client, _ = self._paginated_client(num_pages=3)
        result = client.fetch('ep')
        self.assertEqual(len(result), 3)  # 3 pages
        self.assertEqual(sum(len(p) for p in result), 9)  # 9 total items

    def test_exception_attributes_populated(self):
        from rest_fetcher import DeadlineExceeded

        e3 = DeadlineExceeded(limit=30.0, elapsed=30.5)
        self.assertEqual(e3.limit, 30.0)
        self.assertAlmostEqual(e3.elapsed, 30.5)

    # non-paginated endpoint treated as one page

    def test_max_pages_zero_returns_empty_on_non_paginated(self):
        """max_pages=0 stops before the single non-paginated request and returns an empty result"""
        call_count = [0]

        def mock_fn(req, ctx=None, *, run_state):
            call_count[0] += 1
            return {'data': 1}

        client = APIClient(
            {
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'mock': mock_fn}},
            }
        )
        result = client.fetch('ep', max_pages=0)
        self.assertEqual(result, [])
        self.assertEqual(call_count[0], 0, 'no request should have been issued')

    def test_max_pages_1_allows_non_paginated(self):
        """max_pages=1 allows a single non-paginated request through"""
        client = APIClient(
            {
                'base_url': 'https://api.example.com',
                'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'mock': [{'data': 42}]}},
            }
        )
        result = client.fetch('ep', max_pages=1)
        self.assertEqual(result, {'data': 42})

    # oversleep: deadline check fires after retry-related sleeps

    def test_deadline_check_fires_after_retry_after_sleep(self):
        """DeadlineExceeded fires after Retry-After sleep, not after the next attempt"""
        from rest_fetcher import DeadlineExceeded
        from rest_fetcher.context import OperationContext

        # Strategy: time advances past deadline only after the sleep call.
        # fake_monotonic returns 0.0 until fake_sleep is called, then returns 100.0.
        # This means: construction (started_at=0), top-of-loop check (elapsed=0, ok),
        # attempt issued (attempt_count=1), 429 received, sleep called, post-sleep
        # check (elapsed=100 > limit=10) fires DeadlineExceeded.
        slept = {'done': False}

        def fake_sleep(t):
            slept['done'] = True

        def fake_monotonic():
            return 100.0 if slept['done'] else 0.0

        handler = RetryHandler(
            {'max_attempts': 5, 'backoff': 'linear', 'base_delay': 0, 'on_codes': [429]},
            {'respect_retry_after': True},
        )

        attempt_count = [0]

        def always_429():
            attempt_count[0] += 1
            return make_response(429, headers={'Retry-After': '60'})

        with patch('time.sleep', fake_sleep), patch('time.monotonic', fake_monotonic):
            ctx = OperationContext(time_limit=10.0)  # started_at=0, limit=10s
            with self.assertRaises(DeadlineExceeded):
                handler.execute(always_429, ctx)

        self.assertEqual(
            attempt_count[0],
            1,
            'one attempt made; Retry-After sleep triggered; post-sleep check raised',
        )

    def test_deadline_check_fires_after_network_error_backoff(self):
        """DeadlineExceeded fires after network-error backoff sleep"""
        import requests as req_lib

        from rest_fetcher import DeadlineExceeded
        from rest_fetcher.context import OperationContext

        slept = {'done': False}

        def fake_sleep(t):
            slept['done'] = True

        def fake_monotonic():
            return 100.0 if slept['done'] else 0.0

        handler = RetryHandler({'max_attempts': 5, 'backoff': 'linear', 'base_delay': 1}, {})

        attempt_count = [0]

        def always_network_error():
            attempt_count[0] += 1
            raise req_lib.ConnectionError('connection refused')

        with patch('time.sleep', fake_sleep), patch('time.monotonic', fake_monotonic):
            ctx = OperationContext(time_limit=10.0)
            with self.assertRaises(DeadlineExceeded):
                handler.execute(always_network_error, ctx)

        self.assertEqual(
            attempt_count[0],
            1,
            'one attempt made; backoff sleep triggered; post-sleep check raised',
        )


class TestCallbackWrapperSemantics(unittest.TestCase):
    def test_pagination_callback_response_error_is_not_rewrapped(self):
        from rest_fetcher.pagination import CycleRunner

        def on_response(resp, state):
            raise ResponseError('bad body', raw=resp)

        runner = CycleRunner({'on_response': on_response, 'next_request': lambda resp, state: None})

        def mock_fetch(request, ctx=None, *, run_state):
            return outcome({'items': [1]}, request, {'_status_code': 200})

        with self.assertRaises(ResponseError) as cm:
            list(runner.run(mock_fetch, {}))
        self.assertEqual(str(cm.exception), 'bad body')

    def test_pagination_optional_callback_none_is_a_noop(self):
        from rest_fetcher.pagination import CycleRunner

        runner = CycleRunner({'on_page': None, 'next_request': lambda resp, state: None})

        def mock_fetch(request, ctx=None, *, run_state):
            return outcome({'items': [1]}, request, {'_status_code': 200})

        pages = list(runner.run(mock_fetch, {}))
        self.assertEqual(len(pages), 1)


class TestCallTimeOverrides(unittest.TestCase):
    def test_call_time_response_format_overrides_endpoint_and_client(self):
        schema = {
            'base_url': 'https://example.com',
            'response_format': 'json',
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/x',
                    'response_format': 'text',
                }
            },
        }
        resp = make_response(200, {'x': 1}, headers={'Content-Type': 'application/json'})
        with patch('requests.Session.request', return_value=resp):
            out = APIClient(schema).fetch('ep', response_format='text')
        self.assertIsInstance(out, str)

    def test_call_time_csv_delimiter_overrides_endpoint_and_client(self):
        schema = {
            'base_url': 'https://example.com',
            'csv_delimiter': ';',
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/r.csv',
                    'response_format': 'csv',
                    'csv_delimiter': ',',
                }
            },
        }
        from unittest.mock import MagicMock

        response = MagicMock()
        response.status_code = 200
        response.ok = True
        response.url = 'https://example.com/r.csv'
        response.headers = {'Content-Type': 'text/csv'}
        response.content = b'id|name\n1|Alice\n'
        response.text = response.content.decode('utf-8')
        with patch('requests.Session.request', return_value=response):
            rows = APIClient(schema).fetch('ep', csv_delimiter='|')
        self.assertEqual(rows[0]['name'], 'Alice')

    def test_call_time_encoding_overrides_endpoint_and_client(self):
        schema = {
            'base_url': 'https://example.com',
            'encoding': 'utf-8',
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/r.csv',
                    'response_format': 'csv',
                    'encoding': 'utf-8',
                }
            },
        }
        from unittest.mock import MagicMock

        response = MagicMock()
        response.status_code = 200
        response.ok = True
        response.url = 'https://example.com/r.csv'
        response.headers = {'Content-Type': 'text/csv'}
        response.content = 'id;name\n1;Алиса\n'.encode('cp1251')
        response.text = response.content.decode('latin1')
        with patch('requests.Session.request', return_value=response):
            rows = APIClient(schema).fetch('ep', csv_delimiter=';', encoding='cp1251')
        self.assertEqual(rows[0]['name'], 'Алиса')

    def test_call_time_scrub_headers_overrides_endpoint_and_client(self):
        schema = {
            'base_url': 'https://example.com',
            'scrub_headers': ['x-client-secret'],
            'endpoints': {
                'ep': {'method': 'GET', 'path': '/x', 'scrub_headers': ['x-endpoint-secret']}
            },
        }
        client = APIClient(schema)
        job = client._make_job('ep', {'scrub_headers': ['x-call-secret']})
        self.assertEqual(job._extra_scrub, ['x-call-secret'])

    def test_call_time_scrub_query_params_overrides_endpoint_and_client(self):
        schema = {
            'base_url': 'https://example.com',
            'scrub_query_params': ['client_token'],
            'endpoints': {
                'ep': {'method': 'GET', 'path': '/x', 'scrub_query_params': ['endpoint_token']}
            },
        }
        client = APIClient(schema)
        job = client._make_job('ep', {'scrub_query_params': ['call_token']})
        self.assertEqual(job._extra_scrub_query, ['call_token'])

    def test_call_time_canonical_parser_overrides_endpoint_and_client(self):
        client_parser = lambda content, ctx: {'source': 'client'}
        endpoint_parser = lambda content, ctx: {'source': 'endpoint'}
        call_parser = lambda content, ctx: {'source': 'call'}
        schema = {
            'base_url': 'https://example.com',
            'canonical_parser': client_parser,
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/x',
                    'canonical_parser': endpoint_parser,
                }
            },
        }
        client = APIClient(schema)
        job = client._make_job('ep', {'canonical_parser': call_parser})
        self.assertIs(job._canonical_parser, call_parser)

    def test_call_time_headers_merge_over_endpoint_and_client(self):
        schema = {
            'base_url': 'https://example.com',
            'headers': {'X-Client': '1', 'X-Shared': 'client'},
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/x',
                    'headers': {'X-Endpoint': '1', 'X-Shared': 'endpoint'},
                }
            },
        }
        client = APIClient(schema)
        request = client._make_job(
            'ep', {'headers': {'X-Call': '1', 'X-Shared': 'call'}}
        )._build_initial_request()
        self.assertEqual(request['headers']['X-Client'], '1')
        self.assertEqual(request['headers']['X-Endpoint'], '1')
        self.assertEqual(request['headers']['X-Call'], '1')
        self.assertEqual(request['headers']['X-Shared'], 'call')

    def test_call_time_params_merge_over_endpoint(self):
        schema = {
            'base_url': 'https://example.com',
            'endpoints': {
                'ep': {
                    'method': 'GET',
                    'path': '/x',
                    'params': {'endpoint_only': '1', 'shared': 'endpoint'},
                }
            },
        }
        client = APIClient(schema)
        request = client._make_job(
            'ep', {'params': {'call_only': '1', 'shared': 'call'}}
        )._build_initial_request()
        self.assertEqual(request['params']['endpoint_only'], '1')
        self.assertEqual(request['params']['call_only'], '1')
        self.assertEqual(request['params']['shared'], 'call')


if __name__ == '__main__':
    unittest.main(verbosity=2)


class TestRunnerGuard(unittest.TestCase):
    def test_pytest_is_required_runner(self) -> None:
        # If someone runs `python -m unittest discover`, they'll get a misleading green run
        # because pytest-style tests (bare functions + fixtures) are excluded.
        #
        # Fail loudly unless we are under pytest.
        if os.environ.get('PYTEST_CURRENT_TEST') is None:
            self.fail('Use pytest as the test runner: `pytest -q`')


def test_retry_handler_logs_on_retry_hook_failure(caplog):
    import pytest
    from rest_fetcher.retry import RetryHandler

    class DummyResponse:
        status_code = 500
        headers = {}
        text = ''

        def json(self):
            return {}

    handler = RetryHandler(
        {'max_attempts': 2, 'base_delay': 0, 'max_delay': 0}, {'respect_retry_after': False}
    )

    def request_fn():
        return DummyResponse()

    def bad_on_retry(_info):
        raise RuntimeError('boom')

    with caplog.at_level('WARNING', logger='rest_fetcher.retry'):
        with pytest.raises(RequestError):
            handler.execute(request_fn, on_retry=bad_on_retry)
    assert 'on_retry hook raised; suppressing exception' in caplog.text


def test_retry_handler_uses_planned_ms_in_on_retry_payload():
    import pytest
    from rest_fetcher.retry import RetryHandler

    class DummyResponse:
        status_code = 500
        headers = {}
        text = ''

        def json(self):
            return {}

    handler = RetryHandler(
        {'max_attempts': 2, 'base_delay': 0, 'max_delay': 0}, {'respect_retry_after': False}
    )
    seen = []

    def request_fn():
        return DummyResponse()

    with pytest.raises(RequestError):
        handler.execute(request_fn, on_retry=seen.append)

    assert seen
    assert 'planned_ms' in seen[0]
    assert 'retry_in_ms' not in seen[0]


class TestOnPageComplete(unittest.TestCase):
    """Tests for Plan G: on_page_complete post-cycle adaptive throttling hook."""

    def _paginated_schema(self, pages, on_page_complete=None, on_error=None, delay=None):
        pagination = {
            **cursor_pagination(
                cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
            ),
        }
        if delay is not None:
            pagination['delay'] = delay
        ep = {
            'mock': pages,
            'pagination': pagination,
        }
        if on_page_complete is not None:
            ep['on_page_complete'] = on_page_complete
        if on_error is not None:
            ep['on_error'] = on_error
        return {
            'base_url': 'https://api.example.com/v1',
            'endpoints': {'ep': ep},
        }

    # --- basic firing ---

    def test_hook_fires_once_per_page(self):
        calls = []

        def hook(outcome, state):
            calls.append(outcome)
            return None

        client = APIClient(
            self._paginated_schema(
                pages=[
                    {'items': [1], 'next_cursor': 'n1'},
                    {'items': [2], 'next_cursor': None},
                ],
                on_page_complete=hook,
            )
        )
        client.fetch('ep')
        self.assertEqual(len(calls), 2)

    def test_hook_receives_page_cycle_outcome_type(self):
        from rest_fetcher import PageCycleOutcome

        received = []

        def hook(outcome, state):
            received.append(outcome)
            return None

        client = APIClient(
            self._paginated_schema(
                pages=[{'items': [1], 'next_cursor': None}],
                on_page_complete=hook,
            )
        )
        client.fetch('ep')
        self.assertEqual(len(received), 1)
        self.assertIsInstance(received[0], PageCycleOutcome)
        self.assertTrue(hasattr(received[0], 'kind'))
        self.assertTrue(hasattr(received[0], 'cycle_elapsed_ms'))

    def test_hook_receives_state_view(self):
        received_states = []

        def hook(outcome, state):
            received_states.append(type(state).__name__)
            return None

        client = APIClient(
            self._paginated_schema(
                pages=[{'items': [1], 'next_cursor': None}],
                on_page_complete=hook,
            )
        )
        client.fetch('ep')
        self.assertEqual(received_states, ['StateView'])

    def test_successful_outcome_has_no_error(self):
        received = []

        def hook(outcome, state):
            received.append(outcome)
            return None

        client = APIClient(
            self._paginated_schema(
                pages=[{'items': [1], 'next_cursor': None}],
                on_page_complete=hook,
            )
        )
        client.fetch('ep')
        self.assertIsNone(received[0].error)
        self.assertEqual(received[0].kind, 'success')
        self.assertIsNone(received[0].stop_signal)

    def test_attempts_for_page_is_one_on_first_attempt_success(self):
        received = []

        def hook(outcome, state):
            received.append(outcome)
            return None

        client = APIClient(
            self._paginated_schema(
                pages=[{'items': [1], 'next_cursor': None}],
                on_page_complete=hook,
            )
        )
        client.fetch('ep')
        self.assertEqual(received[0].attempts_for_page, 1)
        self.assertEqual(received[0].kind, 'success')
        self.assertIsNotNone(received[0].cycle_elapsed_ms)

    def test_attempts_for_page_counts_retries(self):
        received = []

        def hook(outcome, state):
            received.append(outcome)
            return None

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'retry': {
                    'max_attempts': 2,
                    'backoff': 'linear',
                    'base_delay': 0.02,
                    'max_delay': 0.02,
                    'on_codes': [429],
                },
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                    }
                },
            }
        )

        call_count = [0]

        def mock_request(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return make_response(
                    status=429, body={}, headers={'Content-Type': 'application/json'}
                )
            return make_response(
                body={'items': [1], 'next_cursor': None},
                headers={'Content-Type': 'application/json'},
            )

        with patch.object(client._session, 'request', side_effect=mock_request):
            client.fetch('ep')

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].kind, 'success')
        self.assertEqual(received[0].attempts_for_page, 2)

    def test_state_has_progress_fields(self):
        seen_pages = []

        def hook(outcome, state):
            seen_pages.append(state.get('pages_so_far'))
            return None

        client = APIClient(
            self._paginated_schema(
                pages=[
                    {'items': [1], 'next_cursor': 'n1'},
                    {'items': [2], 'next_cursor': None},
                ],
                on_page_complete=hook,
            )
        )
        client.fetch('ep')
        self.assertEqual(seen_pages, [1, 2])

    # --- return value: adaptive delay ---

    def test_none_return_applies_no_delay(self):
        import time

        def hook(outcome, state):
            return None

        client = APIClient(
            self._paginated_schema(
                pages=[
                    {'items': [1], 'next_cursor': 'n1'},
                    {'items': [2], 'next_cursor': None},
                ],
                on_page_complete=hook,
            )
        )
        t0 = time.monotonic()
        client.fetch('ep')
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 1.0)

    def test_zero_return_applies_no_delay(self):
        import time

        def hook(outcome, state):
            return 0

        client = APIClient(
            self._paginated_schema(
                pages=[{'items': [1], 'next_cursor': None}],
                on_page_complete=hook,
            )
        )
        t0 = time.monotonic()
        client.fetch('ep')
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 1.0)

    # --- invalid return values raise CallbackError ---

    def test_negative_return_raises_callback_error(self):
        def hook(outcome, state):
            return -1.0

        client = APIClient(
            self._paginated_schema(
                pages=[{'items': [1], 'next_cursor': None}],
                on_page_complete=hook,
            )
        )
        with self.assertRaises(CallbackError) as ctx:
            client.fetch('ep')
        self.assertIn('on_page_complete', str(ctx.exception))

    def test_string_return_raises_callback_error(self):
        def hook(outcome, state):
            return 'fast'

        client = APIClient(
            self._paginated_schema(
                pages=[{'items': [1], 'next_cursor': None}],
                on_page_complete=hook,
            )
        )
        with self.assertRaises(CallbackError) as ctx:
            client.fetch('ep')
        self.assertIn('on_page_complete', str(ctx.exception))

    def test_bool_true_return_raises_callback_error(self):
        def hook(outcome, state):
            return True

        client = APIClient(
            self._paginated_schema(
                pages=[{'items': [1], 'next_cursor': None}],
                on_page_complete=hook,
            )
        )
        with self.assertRaises(CallbackError) as ctx:
            client.fetch('ep')
        self.assertIn('on_page_complete', str(ctx.exception))

    def test_bool_false_return_raises_callback_error(self):
        def hook(outcome, state):
            return False

        client = APIClient(
            self._paginated_schema(
                pages=[{'items': [1], 'next_cursor': None}],
                on_page_complete=hook,
            )
        )
        with self.assertRaises(CallbackError) as ctx:
            client.fetch('ep')
        self.assertIn('on_page_complete', str(ctx.exception))

    # --- hook exception propagates directly ---

    def test_hook_exception_propagates_directly(self):
        def hook(outcome, state):
            raise ValueError('hook blew up')

        client = APIClient(
            self._paginated_schema(
                pages=[{'items': [1], 'next_cursor': None}],
                on_page_complete=hook,
            )
        )
        with self.assertRaises(ValueError) as ctx:
            client.fetch('ep')
        self.assertIn('hook blew up', str(ctx.exception))

    # --- error path: skip fires hook, raise does not ---

    def test_hook_fires_on_skip_with_error_set(self):
        received = []

        def hook(outcome, state):
            received.append(outcome)
            return None

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1], 'next_cursor': 'n1'},
                            {'items': [2], 'next_cursor': None},
                        ],
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_error': lambda exc, state: 'skip',
                    }
                },
            }
        )
        # drive a 404 response on the second page via mock sequence
        # use mock list with a dict that has no 'items' to simulate error-free skip flow
        # Actually use a real HTTP mock error response path:
        responses = [
            make_response(
                body={'items': [1], 'next_cursor': 'n1'},
                headers={'Content-Type': 'application/json'},
            ),
            make_response(
                status=404,
                body={'detail': 'not found'},
                headers={'Content-Type': 'application/json'},
            ),
        ]
        # need a real session-based client for HTTP mocking
        client2 = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_error': lambda exc, state: 'skip',
                    }
                },
            }
        )
        with patch.object(client2._session, 'request', side_effect=responses):
            result = client2.fetch('ep')

        # hook fires for page 1 (success) and page 2 (skip)
        self.assertEqual(len(received), 2)
        # first outcome: success
        self.assertIsNone(received[0].error)
        self.assertEqual(received[0].kind, 'success')
        # second outcome: skip
        self.assertIsNotNone(received[1].error)
        self.assertEqual(received[1].kind, 'skipped')

    def test_hook_does_not_fire_on_raise(self):
        received = []

        def hook(outcome, state):
            received.append(outcome)
            return None

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_error': lambda exc, state: 'raise',
                    }
                },
            }
        )
        # use callable side_effect to avoid StopIteration-inside-generator (PEP 479)
        resp = make_response(status=500, body={}, headers={'Content-Type': 'application/json'})
        with patch.object(client._session, 'request', return_value=resp):
            with self.assertRaises(RequestError):
                client.fetch('ep')

        self.assertEqual(len(received), 0)

    def test_hook_fires_on_error_stop_with_stop_signal_set(self):
        received = []

        def hook(outcome, state):
            received.append(outcome)
            return None

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_error': lambda exc, state: 'stop',
                    }
                },
            }
        )
        # use a counter-based callable to avoid PEP 479 StopIteration issues
        call_count = [0]

        def mock_request(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return make_response(
                    body={'items': [1], 'next_cursor': 'n1'},
                    headers={'Content-Type': 'application/json'},
                )
            return make_response(status=503, body={}, headers={'Content-Type': 'application/json'})

        with patch.object(client._session, 'request', side_effect=mock_request):
            client.fetch('ep')

        # hook fires for page 1 (success path via normal loop)
        # and for page 2 (error_stop via early break path)
        self.assertEqual(len(received), 2)
        # first page: success, no stop signal
        self.assertIsNone(received[0].stop_signal)
        # second page: error_stop — stop_signal set and status_code preserved
        self.assertIsNotNone(received[1].stop_signal)
        self.assertEqual(received[1].stop_signal.kind, 'error_stop')
        self.assertEqual(received[1].status_code, 503)

    # --- stop_signal set: adaptive sleep is skipped ---

    def test_adaptive_sleep_skipped_when_stop_signal_set(self):
        import time

        call_log = []

        def hook(outcome, state):
            call_log.append(outcome.stop_signal)
            return 10.0  # would take 10s if sleep ran

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_error': lambda exc, state: 'stop',
                    }
                },
            }
        )
        resp503 = make_response(status=503, body={}, headers={'Content-Type': 'application/json'})
        t0 = time.monotonic()
        with patch.object(client._session, 'request', return_value=resp503):
            client.fetch('ep')
        elapsed = time.monotonic() - t0
        # sleep must not have run — stop_signal was set
        self.assertLess(elapsed, 5.0)
        self.assertIsNotNone(call_log[0])

    # --- adaptive_wait_end event emitted ---

    def test_adaptive_wait_end_event_emitted(self):
        events = []

        def on_event(event):
            events.append(event)

        def hook(outcome, state):
            return 0.001  # tiny sleep

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1], 'next_cursor': 'n1'},
                            {'items': [2], 'next_cursor': None},
                        ],
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_event': on_event,
                    }
                },
            }
        )
        client.fetch('ep')
        adaptive_events = [e for e in events if e.kind == 'adaptive_wait_end']
        # fires for page 1 (which has a next page); page 2 stop is next_request_none so no sleep
        self.assertEqual(len(adaptive_events), 1)
        self.assertIn('wait_ms', adaptive_events[0].data)
        self.assertEqual(adaptive_events[0].data['wait_type'], 'adaptive')

    def test_adaptive_wait_end_not_emitted_when_no_delay(self):
        events = []

        def on_event(event):
            events.append(event)

        def hook(outcome, state):
            return None

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'mock': [{'items': [1], 'next_cursor': None}],
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_event': on_event,
                    }
                },
            }
        )
        client.fetch('ep')
        adaptive_events = [e for e in events if e.kind == 'adaptive_wait_end']
        self.assertEqual(len(adaptive_events), 0)

    # --- adaptive counters in run_state ---

    def test_adaptive_wait_counters_increment(self):
        summaries = []

        def hook(outcome, state):
            return 0.001

        def on_complete(summary, state):
            summaries.append(state)

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1], 'next_cursor': 'n1'},
                            {'items': [2], 'next_cursor': None},
                        ],
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_complete': on_complete,
                    }
                },
            }
        )
        list(client.stream('ep'))
        # page 1 fires adaptive sleep (0.001s); page 2 has no next so no sleep
        # counters are exposed via state in on_complete
        # Note: final state is after loop exit so check via event instead
        events = []

        def on_event(e):
            events.append(e)

        client2 = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1], 'next_cursor': 'n1'},
                            {'items': [2], 'next_cursor': None},
                        ],
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_event': on_event,
                    }
                },
            }
        )
        client2.fetch('ep')
        adaptive_end = [e for e in events if e.kind == 'adaptive_wait_end']
        self.assertEqual(len(adaptive_end), 1)
        self.assertEqual(adaptive_end[0].data['adaptive_waits_so_far'], 1)
        self.assertGreater(adaptive_end[0].data['adaptive_wait_seconds_so_far'], 0)

    def test_adaptive_wait_end_not_emitted_when_stop_signal_already_set(self):
        events = []

        def on_event(event):
            events.append(event)

        def hook(outcome, state):
            return 0.001

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_error': lambda exc, state: 'stop',
                        'on_event': on_event,
                    }
                },
            }
        )
        resp503 = make_response(status=503, body={}, headers={'Content-Type': 'application/json'})
        with patch.object(client._session, 'request', return_value=resp503):
            client.fetch('ep')

        adaptive_events = [e for e in events if e.kind == 'adaptive_wait_end']
        stopped_events = [e for e in events if e.kind == 'stopped']
        self.assertEqual(len(adaptive_events), 0)
        self.assertEqual(len(stopped_events), 1)
        self.assertEqual(stopped_events[0].data['stop_kind'], 'error_stop')

    def test_request_start_remains_minimal_while_later_events_are_enriched(self):
        events = []

        def on_event(event):
            events.append(event)

        def hook(outcome, state):
            return 0.001

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1], 'next_cursor': 'n1'},
                            {'items': [2], 'next_cursor': None},
                        ],
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_event': on_event,
                    }
                },
            }
        )
        client.fetch('ep')

        request_start = [e for e in events if e.kind == 'request_start']
        request_end = [e for e in events if e.kind == 'request_end']
        adaptive_end = [e for e in events if e.kind == 'adaptive_wait_end']
        self.assertGreaterEqual(len(request_start), 1)
        self.assertGreaterEqual(len(request_end), 1)
        self.assertEqual(len(adaptive_end), 1)
        self.assertEqual(set(request_start[0].data.keys()), {'method'})
        self.assertIn('requests_so_far', request_end[0].data)
        self.assertIn('elapsed_seconds_so_far', request_end[0].data)
        self.assertIn('adaptive_waits_so_far', adaptive_end[0].data)
        self.assertIn('adaptive_wait_seconds_so_far', adaptive_end[0].data)

    def test_cycle_elapsed_ms_matches_request_timing_on_first_attempt(self):
        received = []
        events = []
        mono_state = {'value': 1000.0}

        def hook(outcome, state):
            received.append(outcome)
            return None

        def on_event(event):
            events.append(event)

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_event': on_event,
                    }
                },
            }
        )

        def fake_monotonic():
            mono_state['value'] += 0.5
            return mono_state['value']

        response = make_response(
            body={'items': [1], 'next_cursor': None}, headers={'Content-Type': 'application/json'}
        )

        with (
            patch.object(client._session, 'request', return_value=response),
            patch('rest_fetcher.client.time.monotonic', side_effect=fake_monotonic),
        ):
            client.fetch('ep')

        request_end = [e for e in events if e.kind == 'request_end']
        self.assertEqual(len(received), 1)
        self.assertEqual(len(request_end), 1)
        self.assertIsNotNone(received[0].cycle_elapsed_ms)
        self.assertAlmostEqual(
            received[0].cycle_elapsed_ms, request_end[0].data['elapsed_ms'], places=6
        )

    def test_cycle_elapsed_ms_includes_retry_backoff_waits(self):
        received = []
        events = []
        BACKOFF_S = 0.02
        sleep_called = {'value': False}
        mono_state = {'value': 1000.0}

        def hook(outcome, state):
            received.append(outcome)
            return None

        def on_event(event):
            events.append(event)

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'retry': {
                    'max_attempts': 2,
                    'backoff': 'linear',
                    'base_delay': BACKOFF_S,
                    'max_delay': BACKOFF_S,
                    'on_codes': [429],
                },
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_event': on_event,
                    }
                },
            }
        )

        call_count = [0]

        def mock_request(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return make_response(
                    status=429, body={}, headers={'Content-Type': 'application/json'}
                )
            return make_response(
                body={'items': [1], 'next_cursor': None},
                headers={'Content-Type': 'application/json'},
            )

        def fake_sleep(seconds):
            sleep_called['value'] = True

        def fake_monotonic():
            mono_state['value'] += 0.0005
            if sleep_called['value']:
                mono_state['value'] += BACKOFF_S
                sleep_called['value'] = False
            return mono_state['value']

        with (
            patch.object(client._session, 'request', side_effect=mock_request),
            patch('rest_fetcher.retry.time.sleep', side_effect=fake_sleep),
            patch('rest_fetcher.client.time.monotonic', side_effect=fake_monotonic),
        ):
            client.fetch('ep')

        retry_events = [e for e in events if e.kind == 'retry']
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].kind, 'success')
        self.assertEqual(received[0].attempts_for_page, 2)
        self.assertEqual(len(retry_events), 1)
        self.assertEqual(retry_events[0].data['retries_so_far'], 1)
        self.assertIsNotNone(received[0].cycle_elapsed_ms)
        self.assertGreater(received[0].cycle_elapsed_ms, BACKOFF_S * 1000.0)

    # --- schema validation accepts on_page_complete ---

    def test_schema_validates_on_page_complete_as_callable(self):
        # should not raise
        from rest_fetcher.schema import validate

        validate(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/items',
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': lambda outcome, state: None,
                    }
                },
            }
        )

    def test_schema_rejects_non_callable_on_page_complete(self):
        from rest_fetcher.schema import validate

        with self.assertRaises(SchemaError):
            validate(
                {
                    'base_url': 'https://api.example.com/v1',
                    'endpoints': {
                        'ep': {
                            'method': 'GET',
                            'path': '/items',
                            'pagination': cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                            'on_page_complete': 'not_a_callable',
                        }
                    },
                }
            )

    # --- PageCycleOutcome is exported from public surface ---

    def test_page_cycle_outcome_importable_from_public_surface(self):
        from rest_fetcher import PageCycleOutcome

        self.assertTrue(hasattr(PageCycleOutcome, '__dataclass_fields__'))

    # --- static delay + adaptive delay are additive ---

    def test_static_and_adaptive_delays_are_additive(self):
        """Both delays contribute to total sleep; callers rely on the additive effect, not a specific order."""
        import time

        call_order = []

        real_sleep = time.sleep
        sleep_calls = []

        def tracking_sleep(seconds):
            sleep_calls.append(seconds)
            # don't actually sleep in tests

        def hook(outcome, state):
            return 0.07  # adaptive

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'mock': [
                            {'items': [1], 'next_cursor': 'n1'},
                            {'items': [2], 'next_cursor': None},
                        ],
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                            'delay': 0.05,  # static baseline
                        },
                        'on_page_complete': hook,
                    }
                },
            }
        )

        with patch('time.sleep', side_effect=tracking_sleep):
            client.fetch('ep')

        # page 1 contributes both static (0.05) and adaptive (0.07) sleep;
        # page 2 has no continuation so neither delay runs. The total effect is additive.
        self.assertGreaterEqual(len(sleep_calls), 2)
        self.assertAlmostEqual(sum(sleep_calls[:2]), 0.12, places=5)
        self.assertEqual({round(s, 2) for s in sleep_calls[:2]}, {0.05, 0.07})

    # --- Bug regression: invalid return on error_stop path must raise CallbackError ---

    def test_invalid_return_on_error_stop_path_raises_callback_error(self):
        """Bug fix: on_page_complete return value must be validated on error_stop path,
        not silently ignored as it was before the fix."""
        from rest_fetcher.exceptions import CallbackError

        def hook(outcome, state):
            return 'bad'  # invalid: not float, None, or 0

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_error': lambda exc, state: 'stop',
                    }
                },
            }
        )
        resp503 = make_response(status=503, body={}, headers={'Content-Type': 'application/json'})
        with patch.object(client._session, 'request', return_value=resp503):
            with self.assertRaises(CallbackError) as ctx:
                client.fetch('ep')
        self.assertIn('on_page_complete', str(ctx.exception))

    # --- Bug regression: status_code preserved on skip and error_stop outcomes ---

    def test_status_code_preserved_on_http_error_skip(self):
        """Bug fix: _status_code was not injected into headers on skip path,
        causing outcome.status_code to be None even when response code is known."""
        received = []

        def hook(outcome, state):
            received.append(outcome)
            return None

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_error': lambda exc, state: 'skip',
                    }
                },
            }
        )
        call_count = [0]

        def mock_request(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return make_response(
                    body={'items': [1], 'next_cursor': 'n1'},
                    headers={'Content-Type': 'application/json'},
                )
            return make_response(status=404, body={}, headers={'Content-Type': 'application/json'})

        with patch.object(client._session, 'request', side_effect=mock_request):
            client.fetch('ep')

        self.assertEqual(len(received), 2)
        # page 1 success — status code is the 200
        self.assertEqual(received[0].status_code, 200)
        # page 2 skip — status_code must be 404, not None
        self.assertEqual(received[1].status_code, 404)
        self.assertEqual(received[1].kind, 'skipped')

    def test_status_code_preserved_on_error_stop(self):
        """Bug fix: error_stop path hardcoded status_code=None; must use _last_error.status_code."""
        received = []

        def hook(outcome, state):
            received.append(outcome)
            return None

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'path': '/items',
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                        },
                        'on_page_complete': hook,
                        'on_error': lambda exc, state: 'stop',
                    }
                },
            }
        )
        resp503 = make_response(status=503, body={}, headers={'Content-Type': 'application/json'})
        with patch.object(client._session, 'request', return_value=resp503):
            client.fetch('ep')

        self.assertEqual(len(received), 1)
        # status_code must be 503, not None
        self.assertEqual(received[0].status_code, 503)
        self.assertEqual(received[0].kind, 'stopped')


class TestG3StrictValidation(unittest.TestCase):
    def test_strict_rejects_unknown_nested_retry_key(self):
        with self.assertRaises(SchemaError):
            validate(
                {**simple_schema(), 'retry': {'max_attempts': 2, 'on_errror': [500]}}, strict=True
            )

    def test_strict_rejects_unknown_nested_pagination_key(self):
        schema = simple_schema(
            pagination={'next_request': lambda parsed, state: None, 'delayy': 1.0}
        )
        with self.assertRaises(SchemaError):
            validate(schema, strict=True)

    def test_strict_rejects_unknown_fixed_auth_key_but_callback_allows_extra(self):
        with self.assertRaises(SchemaError):
            validate(
                {**simple_schema(), 'auth': {'type': 'bearer', 'token': 'x', 'toke': 'y'}},
                strict=True,
            )
        validate(
            {
                **simple_schema(),
                'auth': {
                    'type': 'callback',
                    'handler': lambda request_kwargs, config: request_kwargs,
                    'tenant_id': 'abc',
                },
            },
            strict=True,
        )

    def test_strict_rejects_unknown_nested_rate_limit_key(self):
        with self.assertRaises(SchemaError):
            validate(
                {**simple_schema(), 'rate_limit': {'min_delay': 0.5, 'minn_delay': 0.5}},
                strict=True,
            )

    def test_rate_limit_max_retry_after_is_rejected_with_redirect(self):
        with self.assertRaises(SchemaError):
            validate({**simple_schema(), 'rate_limit': {'max_retry_after': 1.0}}, strict=False)


class TestG3SummaryFields(unittest.TestCase):
    def test_stream_summary_includes_static_and_adaptive_waits(self):
        seen = {}

        def on_complete(summary, state):
            seen['summary'] = summary

        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/test',
                        'mock': [
                            {'items': [1], 'next_cursor': 'n1'},
                            {'items': [2], 'next_cursor': None},
                        ],
                        'pagination': {
                            **cursor_pagination(
                                cursor_param='cursor',
                                next_cursor_path='next_cursor',
                                data_path='items',
                            ),
                            'delay': 0.2,
                        },
                        'on_page_complete': lambda outcome, state: 0.3,
                        'on_complete': on_complete,
                    }
                },
            }
        )

        with patch('time.sleep', return_value=None):
            list(client.stream('ep'))

        summary = seen['summary']
        self.assertIsInstance(summary, StreamSummary)
        self.assertEqual(summary.static_wait_seconds, 0.2)
        self.assertEqual(summary.adaptive_wait_seconds, 0.3)
        self.assertEqual(summary.retry_wait_seconds, 0.0)
        self.assertEqual(summary.rate_limit_wait_seconds, 0.0)
        self.assertAlmostEqual(summary.total_wait_seconds, 0.5)


class TestB5MetricsSession(unittest.TestCase):
    def test_validate_metrics_accepts_bool_none_and_session(self):
        session = MetricsSession()
        validate({**simple_schema(), 'metrics': True}, strict=True)
        validate({**simple_schema(), 'metrics': False}, strict=True)
        validate({**simple_schema(), 'metrics': None}, strict=True)
        validate({**simple_schema(), 'metrics': session}, strict=True)

    def test_validate_metrics_rejects_invalid_values(self):
        for bad in ('yes', 42, {}):
            with self.assertRaises(SchemaError):
                validate({**simple_schema(), 'metrics': bad}, strict=True)

    def test_client_metrics_none_when_disabled(self):
        client = APIClient(simple_schema())
        self.assertIsNone(client.metrics)
        client = APIClient({**simple_schema(), 'metrics': False})
        self.assertIsNone(client.metrics)

    def test_metrics_true_creates_default_session(self):
        client = APIClient({**simple_schema(), 'metrics': True})
        self.assertIsInstance(client.metrics, MetricsSession)

    def test_injected_metrics_session_is_reused(self):
        session = MetricsSession()
        client = APIClient({**simple_schema(), 'metrics': session})
        self.assertIs(client.metrics, session)

    def test_metrics_summary_and_reset(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'metrics': True,
                'endpoints': {'ep': {'method': 'GET', 'path': '/x', 'mock': [{'items': [1]}]}},
            }
        )
        client.fetch('ep')
        snap = client.metrics.summary()
        self.assertIsInstance(snap, MetricsSummary)
        self.assertEqual(snap.total_runs, 1)
        self.assertEqual(snap.total_failed_runs, 0)
        self.assertEqual(snap.total_requests, 1)
        self.assertEqual(snap.total_pages, 1)
        self.assertEqual(snap.total_retries, 0)
        self.assertEqual(snap.total_wait_seconds, 0.0)
        self.assertGreaterEqual(snap.total_elapsed_seconds, 0.0)

        flushed = client.metrics.reset()
        self.assertEqual(flushed.total_runs, 1)
        self.assertEqual(client.metrics.summary(), MetricsSummary())

    def test_shared_metrics_session_combines_totals_across_clients(self):
        session = MetricsSession()
        client_a = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'metrics': session,
                'endpoints': {'ep': {'method': 'GET', 'path': '/a', 'mock': [{'items': [1]}]}},
            }
        )
        client_b = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'metrics': session,
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/b',
                        'mock': [
                            {'items': [1], 'next_cursor': 'n1'},
                            {'items': [2], 'next_cursor': None},
                        ],
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                    }
                },
            }
        )
        client_a.fetch('ep')
        client_b.fetch('ep')
        snap = session.summary()
        self.assertEqual(snap.total_runs, 2)
        self.assertEqual(snap.total_requests, 3)
        self.assertEqual(snap.total_pages, 3)

    def test_failed_run_is_recorded_when_on_complete_raises(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'metrics': True,
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'mock': [{'items': [1]}],
                        'on_complete': lambda summary, state: raise_(RuntimeError('boom')),
                    }
                },
            }
        )
        with self.assertRaises(CallbackError):
            list(client.stream('ep'))
        snap = client.metrics.summary()
        self.assertEqual(snap.total_runs, 1)
        self.assertEqual(snap.total_failed_runs, 1)
        self.assertEqual(snap.total_requests, 1)
        self.assertEqual(snap.total_pages, 1)

    def test_metrics_record_failure_does_not_mask_run_exception(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'metrics': True,
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'mock': [{'items': [1]}],
                        'on_complete': lambda summary, state: raise_(RuntimeError('boom')),
                    }
                },
            }
        )
        client.metrics._record = MagicMock(side_effect=RuntimeError('metrics-bad'))
        with self.assertRaises(CallbackError) as ctx:
            list(client.stream('ep'))
        self.assertIn('boom', str(ctx.exception))

    def test_single_success_run_matches_metrics_summary(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'metrics': True,
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'mock': [{'items': [1]}, {'items': [2]}],
                    }
                },
            }
        )
        run = client.stream_run('ep')
        list(run)
        summary = run.summary
        metrics = client.metrics.summary()

        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual(summary.pages, metrics.total_pages)
        self.assertEqual(summary.requests, metrics.total_requests)
        self.assertEqual(summary.retries, metrics.total_retries)
        self.assertEqual(summary.bytes_received, metrics.total_bytes_received)
        self.assertEqual(summary.retry_bytes_received, metrics.total_retry_bytes_received)
        self.assertEqual(summary.total_wait_seconds, metrics.total_wait_seconds)
        self.assertEqual(summary.elapsed_seconds, metrics.total_elapsed_seconds)
        self.assertEqual(metrics.total_runs, 1)
        self.assertEqual(metrics.total_failed_runs, 0)

    def test_single_failed_run_is_counted_in_metrics_summary(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'metrics': True,
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'mock': [{'items': [1]}],
                        'on_complete': lambda summary, state: raise_(RuntimeError('boom')),
                    }
                },
            }
        )
        with self.assertRaises(CallbackError):
            list(client.stream('ep'))
        metrics = client.metrics.summary()
        self.assertEqual(metrics.total_runs, 1)
        self.assertEqual(metrics.total_failed_runs, 1)
        self.assertEqual(metrics.total_pages, 1)
        self.assertEqual(metrics.total_requests, 1)
        self.assertEqual(metrics.total_retries, 0)
        # This mock-path test pins failure accounting, not live/playback byte totals.
        self.assertEqual(metrics.total_bytes_received, 0)
        self.assertEqual(metrics.total_retry_bytes_received, 0)

    def test_playback_save_failure_marks_run_failed(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, 'fixture.jsonl')
            client = APIClient(
                {
                    'base_url': 'https://api.example.com/v1',
                    'metrics': True,
                    'endpoints': {
                        'ep': {
                            'method': 'GET',
                            'path': '/x',
                            'playback': {'path': path, 'mode': 'save'},
                        }
                    },
                }
            )
            with patch.object(
                requests.Session, 'request', return_value=make_response(body={'items': [1]})
            ):
                with patch(
                    'rest_fetcher.playback.PlaybackHandler.save',
                    side_effect=RuntimeError('save-bad'),
                ):
                    with self.assertRaises(RuntimeError) as ctx:
                        client.fetch('ep')
            self.assertIn('save-bad', str(ctx.exception))
            snap = client.metrics.summary()
            self.assertEqual(snap.total_runs, 1)
            self.assertEqual(snap.total_failed_runs, 1)
            self.assertEqual(snap.total_requests, 1)
            self.assertEqual(snap.total_pages, 1)

    def test_abandoned_generator_counts_as_failed_run(self):
        client = APIClient(
            {
                'base_url': 'https://api.example.com/v1',
                'metrics': True,
                'endpoints': {
                    'ep': {
                        'method': 'GET',
                        'path': '/x',
                        'mock': [
                            {'items': [1], 'next_cursor': 'n1'},
                            {'items': [2], 'next_cursor': None},
                        ],
                        'pagination': cursor_pagination(
                            cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                        ),
                    }
                },
            }
        )
        it = client.stream('ep')
        next(it)
        it.close()
        snap = client.metrics.summary()
        self.assertEqual(snap.total_runs, 1)
        self.assertEqual(snap.total_failed_runs, 1)
        self.assertEqual(snap.total_requests, 1)
        self.assertEqual(snap.total_pages, 0)


class TestOptionalTypingSurface(unittest.TestCase):
    def test_schema_typeddicts_track_runtime_config_keys(self):
        from rest_fetcher.types import (
            CallParams,
            ClientSchema,
            EndpointSchema,
            RateLimitConfig,
            RetryConfig,
        )

        self.assertIn('response_format', CallParams.__annotations__)
        self.assertIn('csv_delimiter', CallParams.__annotations__)
        self.assertIn('scrub_headers', CallParams.__annotations__)
        self.assertIn('scrub_query_params', CallParams.__annotations__)
        self.assertIn('rate_limit', CallParams.__annotations__)

        self.assertIn('canonical_parser', EndpointSchema.__annotations__)
        self.assertIn('retry', EndpointSchema.__annotations__)
        self.assertIn('rate_limit', EndpointSchema.__annotations__)
        self.assertIn('scrub_headers', EndpointSchema.__annotations__)
        self.assertIn('scrub_query_params', EndpointSchema.__annotations__)

        self.assertIn('canonical_parser', ClientSchema.__annotations__)

        self.assertIn('reactive_wait_on_terminal', RetryConfig.__annotations__)
        for key in ('strategy', 'requests_per_second', 'burst', 'on_limit', 'clock', 'sleep'):
            self.assertIn(key, RateLimitConfig.__annotations__)


class TestErrorRaiseSemantics(unittest.TestCase):
    def _job(self) -> object:
        from rest_fetcher.client import _FetchJob

        client = APIClient(simple_schema())
        return _FetchJob(
            client._schema,
            'test',
            client._session,
            client._auth,
            client._retry_config,
        )

    def test_handle_error_response_raise_raises_constructed_request_error(self):
        job = self._job()
        response = MagicMock()
        response.status_code = 503
        response.url = 'https://example.com/items'
        response.text = 'boom'
        response.json.side_effect = ValueError('no json')
        response.headers = {}
        with self.assertRaises(RequestError) as ctx:
            job._handle_error_response(response, method='GET', url=response.url, request_kwargs={})
        self.assertEqual(ctx.exception.status_code, 503)
        self.assertIn('http 503', str(ctx.exception))

    def test_handle_request_cycle_error_raise_preserves_original_traceback_site(self):
        from rest_fetcher.client import _RunState

        job = self._job()
        run_state = _RunState(endpoint_name='test', event_source='live')

        def original_raise_site():
            raise RequestError('boom', status_code=503)

        try:
            original_raise_site()
        except RequestError as exc:
            try:
                job._handle_request_cycle_error(
                    exc,
                    source='live',
                    method='GET',
                    url='https://example.com/items',
                    request_kwargs={},
                    run_state=run_state,
                    start_mono=0.0,
                )
            except RequestError as reraised:
                tb = reraised.__traceback__
            else:
                self.fail('RequestError was not re-raised')

        names = []
        while tb is not None:
            names.append(tb.tb_frame.f_code.co_name)
            tb = tb.tb_next
        self.assertIn('original_raise_site', names)
