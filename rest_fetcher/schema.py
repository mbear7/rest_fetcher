from typing import Any

from .events import _KNOWN_EVENT_KINDS
from .exceptions import SchemaError
from .metrics import MetricsSession
from .parsing import VALID_RESPONSE_FORMATS

# valid values for fields with a fixed set of options
_VALID_AUTH_TYPES = {'bearer', 'basic', 'oauth2', 'oauth2_password', 'callback'}
_VALID_METHODS = {'GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'}
_VALID_BACKOFF = {'exponential', 'linear'}
_VALID_JITTER = {'full', 'equal'}
_VALID_LOG_LEVELS = {'none', 'error', 'medium', 'verbose'}


def _err(path, msg):
    raise SchemaError(f'schema[{path}]: {msg}')


def _check_type(value, expected, path):
    if not isinstance(value, expected):
        type_name = expected.__name__ if hasattr(expected, '__name__') else str(expected)
        _err(path, f'expected {type_name}, got {type(value).__name__}')


def _check_callable(value, path):
    if not callable(value):
        _err(path, f'expected a callable (function or object with __call__), got {type(value).__name__!r}')


def _check_one_of(value, valid_set, path):
    if value not in valid_set:
        _err(path, f'must be one of {sorted(valid_set)}, got {value!r}')


def _validate_on_event_kinds(value, path):
    '''validates and normalizes on_event_kinds in place.

    returns the normalized value:
        None              → all events pass through
        frozenset[str]    → only those kinds pass through

    'all' normalizes to None before kind validation.
    single string normalizes to a one-element frozenset.
    iterables normalize to frozenset.
    empty collections normalize to empty frozenset (silences all events).
    all kind strings are validated against _KNOWN_EVENT_KINDS.
    '''
    if value is None:
        return None

    if isinstance(value, str):
        if value == 'all':
            return None
        if value not in _KNOWN_EVENT_KINDS:
            _err(path, f'unknown event kind {value!r} — known kinds: {sorted(_KNOWN_EVENT_KINDS)}')
        return frozenset({value})

    if isinstance(value, (set, list, tuple, frozenset)):
        for item in value:
            if not isinstance(item, str):
                _err(path, f'expected string event kind, got {type(item).__name__!r}')
            if item not in _KNOWN_EVENT_KINDS:
                _err(path, f'unknown event kind {item!r} — known kinds: {sorted(_KNOWN_EVENT_KINDS)}')
        return frozenset(value)

    _err(path, f'expected None, str, or collection of strings, got {type(value).__name__!r}')


def _validate_auth(auth, path='auth', strict=False):
    _check_type(auth, dict, path)

    auth_type = auth.get('type')
    if auth_type is None:
        _err(path, 'missing required key: type')
    _check_one_of(auth_type, _VALID_AUTH_TYPES, f'{path}.type')

    if auth_type == 'bearer':
        if 'token' not in auth and 'token_callback' not in auth:
            _err(path, 'bearer auth requires either "token" (str) or "token_callback" (callable)')
        if 'token_callback' in auth:
            _check_callable(auth['token_callback'], f'{path}.token_callback')

    elif auth_type == 'basic':
        for key in ('username', 'password'):
            if key not in auth:
                _err(path, f'basic auth requires "{key}"')

    elif auth_type == 'oauth2':
        for key in ('token_url', 'client_id', 'client_secret'):
            if key not in auth:
                _err(path, f'oauth2 auth requires "{key}"')

    elif auth_type == 'oauth2_password':
        for key in ('token_url', 'client_id', 'client_secret', 'username', 'password'):
            if key not in auth:
                _err(path, f'oauth2_password auth requires "{key}"')

    elif auth_type == 'callback':
        if 'handler' not in auth:
            _err(path, 'callback auth requires "handler" (callable)')
        _check_callable(auth['handler'], f'{path}.handler')

    if strict and auth_type in _KNOWN_AUTH_KEYS and auth_type != 'callback':
        unknown = set(auth) - _KNOWN_AUTH_KEYS[auth_type]
        if unknown:
            bad = sorted(unknown)[0]
            _err(path, f'unknown {auth_type} auth key: {bad}')


def _validate_retry(retry, path='retry', strict=False):
    _check_type(retry, dict, path)
    if strict:
        unknown = set(retry) - _KNOWN_RETRY_KEYS
        if unknown:
            bad = sorted(unknown)[0]
            _err(path, f'unknown {path} key: {bad}')

    if 'max_attempts' in retry:
        _check_type(retry['max_attempts'], int, f'{path}.max_attempts')
        if retry['max_attempts'] < 1:
            _err(f'{path}.max_attempts', 'must be >= 1')

    backoff = retry.get('backoff')
    if 'backoff' in retry:
        if not callable(backoff):
            _check_one_of(backoff, _VALID_BACKOFF, f'{path}.backoff')

    if 'on_codes' in retry:
        _check_type(retry['on_codes'], list, f'{path}.on_codes')
        bad = [c for c in retry['on_codes'] if not isinstance(c, int)]
        if bad:
            _err(f'{path}.on_codes', f'all status codes must be integers — got non-int value(s): {bad}')

    if 'base_delay' in retry:
        _check_type(retry['base_delay'], (int, float), f'{path}.base_delay')
        if retry['base_delay'] <= 0:
            _err(f'{path}.base_delay', 'must be > 0')

    if 'max_delay' in retry:
        _check_type(retry['max_delay'], (int, float), f'{path}.max_delay')
        if retry['max_delay'] <= 0:
            _err(f'{path}.max_delay', 'must be > 0')

    if 'max_retry_after' in retry:
        _check_type(retry['max_retry_after'], (int, float), f'{path}.max_retry_after')
        if retry['max_retry_after'] <= 0:
            _err(f'{path}.max_retry_after', 'must be > 0')

    if 'jitter' in retry:
        jitter = retry['jitter']
        if isinstance(jitter, bool):
            if jitter is True:
                retry['jitter'] = 'full'
        else:
            _check_one_of(jitter, _VALID_JITTER, f'{path}.jitter')

    if callable(backoff):
        jitter = retry.get('jitter', False)
        if jitter:
            _err(path, 'callable backoff cannot be combined with jitter — include randomness in the callable itself')


def _validate_rate_limit(rl, path='rate_limit', strict=False):
    _check_type(rl, dict, path)

    if 'max_retry_after' in rl:
        _err(path, 'max_retry_after is not supported; use retry.max_retry_after instead')

    if strict:
        unknown = set(rl) - _KNOWN_RATE_LIMIT_KEYS
        if unknown:
            bad = sorted(unknown)[0]
            _err(path, f'unknown {path} key: {bad}')

    if 'respect_retry_after' in rl:
        _check_type(rl['respect_retry_after'], bool, f'{path}.respect_retry_after')

    if 'min_delay' in rl:
        _check_type(rl['min_delay'], (int, float), f'{path}.min_delay')
        if rl['min_delay'] < 0:
            _err(f'{path}.min_delay', 'must be >= 0')

    # Proactive token bucket rate limiting (Plan D)
    # These keys are optional; if present, validate strictly.
    if 'strategy' in rl or 'requests_per_second' in rl or 'burst' in rl or 'on_limit' in rl:
        strategy = rl.get('strategy', 'token_bucket')
        if strategy != 'token_bucket':
            _err(f'{path}.strategy', "must be 'token_bucket'")

        if 'requests_per_second' not in rl:
            _err(path, 'token_bucket requires requests_per_second')
        _check_type(rl['requests_per_second'], (int, float), f'{path}.requests_per_second')
        if rl['requests_per_second'] <= 0:
            _err(f'{path}.requests_per_second', 'must be > 0')

        if 'burst' not in rl:
            _err(path, 'token_bucket requires burst')
        _check_type(rl['burst'], int, f'{path}.burst')
        if rl['burst'] <= 0:
            _err(f'{path}.burst', 'must be > 0')

        if 'on_limit' in rl:
            _check_one_of(rl['on_limit'], {'wait', 'raise'}, f'{path}.on_limit')

        if 'clock' in rl and rl['clock'] is not None:
            _check_callable(rl['clock'], f'{path}.clock')

        if 'sleep' in rl and rl['sleep'] is not None:
            _check_callable(rl['sleep'], f'{path}.sleep')

def _validate_timeout(value, path):
    if isinstance(value, tuple):
        if len(value) != 2 or not all(isinstance(v, (int, float)) for v in value):
            _err(path, 'tuple form must be (connect_timeout, read_timeout)')
    elif isinstance(value, (int, float)):
        if value <= 0:
            _err(path, 'must be > 0')
    else:
        _err(path, 'must be a number or (connect_timeout, read_timeout) tuple')


def _validate_metrics(value: Any, path: str) -> None:
    if value is None or isinstance(value, bool) or isinstance(value, MetricsSession):
        return
    _err(path, 'must be True, False, None, or a MetricsSession instance')


def _validate_string_list(value, path):
    _check_type(value, list, path)
    if not all(isinstance(h, str) for h in value):
        _err(path, 'all entries must be strings')


def _validate_csv_delimiter(value, path):
    _check_type(value, str, path)
    if len(value) != 1:
        _err(path, 'must be a single-character string')


def _validate_encoding(value, path):
    _check_type(value, str, path)
    if not value:
        _err(path, 'must be a non-empty string')


def _validate_pagination(pagination, path='pagination', strict=False):
    # pagination can be explicitly set to None to disable it
    if pagination is None:
        return

    _check_type(pagination, dict, path)

    # built-in pagination helpers tag their output with _rf_pagination_helper.
    # helper dicts may carry lifecycle hooks (on_response etc.) that will be
    # extracted to endpoint level during config resolution. skip the targeted
    # rejection for these — they are not raw user dicts with misplaced hooks.
    is_helper = pagination.get('_rf_pagination_helper', False)

    if not is_helper:
        # targeted rejection for lifecycle hooks that moved to endpoint level.
        # runs before generic strict unknown-key sweep so the redirecting message
        # is not masked. fires in both strict and non-strict modes to prevent
        # silent migration failures where hooks silently stop firing.
        for moved_key in _MOVED_PAGINATION_KEYS:
            if moved_key in pagination:
                _err(path, f'{path}.{moved_key} is not supported; use {moved_key} at endpoint level instead')

    if strict and not is_helper:
        unknown = set(pagination) - _KNOWN_PAGINATION_KEYS
        if unknown:
            bad = sorted(unknown)[0]
            _err(path, f'unknown {path} key: {bad}')

    # next_request is required — it is the only stop signal; omitting it means
    # pagination would loop forever with no way to terminate
    if 'next_request' not in pagination:
        _err(f'{path}.next_request', 'required — must be a callable(parsed_body, state) -> dict | None')
    _check_callable(pagination['next_request'], f'{path}.next_request')

    if 'delay' in pagination:
        _check_type(pagination['delay'], (int, float), f'{path}.delay')
        if pagination['delay'] < 0:
            _err(f'{path}.delay', 'must be >= 0')

    if 'initial_params' in pagination and pagination['initial_params'] is not None:
        _check_type(pagination['initial_params'], dict, f'{path}.initial_params')


def _validate_endpoint(name, endpoint, path=None, strict=False):
    path = path or f'endpoints.{name}'
    _check_type(endpoint, dict, path)

    # method defaults to GET if omitted
    if 'method' in endpoint:
        _check_one_of(endpoint['method'].upper(), _VALID_METHODS, f'{path}.method')

    # path is optional — some apis use base_url directly
    if 'path' in endpoint and endpoint['path'] is not None:
        _check_type(endpoint['path'], str, f'{path}.path')

    if 'headers' in endpoint:
        _check_type(endpoint['headers'], dict, f'{path}.headers')

    if 'params' in endpoint:
        _check_type(endpoint['params'], dict, f'{path}.params')

    if 'body' in endpoint:
        _check_type(endpoint['body'], dict, f'{path}.body')

    if 'form' in endpoint:
        _check_type(endpoint['form'], dict, f'{path}.form')

    if 'files' in endpoint:
        if not isinstance(endpoint['files'], (dict, list, tuple)):
            _err(f'{path}.files', 'expected dict, list, or tuple')

    if 'body' in endpoint and 'form' in endpoint:
        _err(path, 'body and form are mutually exclusive — use one or the other, not both')

    if 'body' in endpoint and 'files' in endpoint:
        _err(path, 'body and files are mutually exclusive — use body for JSON or files for multipart uploads, not both')

    # pagination at endpoint level — merges over client-level defaults
    if 'pagination' in endpoint:
        _validate_pagination(endpoint['pagination'], f'{path}.pagination', strict=strict)

    optional_callables = ('on_response', 'on_page', 'on_complete', 'on_page_complete', 'update_state', 'on_error', 'on_event', 'on_request')
    for key in optional_callables:
        if key in endpoint and endpoint[key] is not None:
            _check_callable(endpoint[key], f'{path}.{key}')

    if 'on_event_kinds' in endpoint:
        endpoint['on_event_kinds'] = _validate_on_event_kinds(endpoint['on_event_kinds'], f'{path}.on_event_kinds')

    if 'retry' in endpoint:
        if endpoint['retry'] is not None:
            _validate_retry(endpoint['retry'], f'{path}.retry', strict=strict)

    if 'rate_limit' in endpoint:
        if endpoint['rate_limit'] is not None:
            _validate_rate_limit(endpoint['rate_limit'], f'{path}.rate_limit', strict=strict)
    if 'log_level' in endpoint:
        _check_one_of(endpoint['log_level'], _VALID_LOG_LEVELS, f'{path}.log_level')

    if 'timeout' in endpoint:
        _validate_timeout(endpoint['timeout'], f'{path}.timeout')

    if 'debug' in endpoint:
        _check_type(endpoint['debug'], bool, f'{path}.debug')

    if 'response_format' in endpoint:
        _check_type(endpoint['response_format'], str, f'{path}.response_format')
        _check_one_of(endpoint['response_format'], VALID_RESPONSE_FORMATS, f'{path}.response_format')


    if 'response_parser' in endpoint and endpoint['response_parser'] is not None:
        _check_callable(endpoint['response_parser'], f'{path}.response_parser')

    if 'canonical_parser' in endpoint and endpoint['canonical_parser'] is not None:
        _check_callable(endpoint['canonical_parser'], f'{path}.canonical_parser')

    if 'csv_delimiter' in endpoint:
        _validate_csv_delimiter(endpoint['csv_delimiter'], f'{path}.csv_delimiter')

    if 'encoding' in endpoint:
        _validate_encoding(endpoint['encoding'], f'{path}.encoding')

    if 'scrub_headers' in endpoint:
        _validate_string_list(endpoint['scrub_headers'], f'{path}.scrub_headers')

    if 'scrub_query_params' in endpoint:
        _validate_string_list(endpoint['scrub_query_params'], f'{path}.scrub_query_params')

    if 'playback' in endpoint:
        pb = endpoint['playback']
        if not isinstance(pb, dict):
            _err(f'{path}.playback', 'must be a dict with path and optional mode')
        mode = pb.get('mode', 'auto')
        if mode not in ('auto', 'save', 'load', 'none'):
            _err(f'{path}.playback.mode', 'must be one of auto|save|load|none')
        if mode != 'none' and 'path' not in pb:
            _err(f'{path}.playback', 'requires a path key unless mode="none"')
        if 'record_as_bytes' in pb:
            _check_type(pb['record_as_bytes'], bool, f'{path}.playback.record_as_bytes')

    if 'mock' in endpoint:
        # mock can be a callable or a list of dicts (static pages)
        mock = endpoint['mock']
        if not callable(mock) and not isinstance(mock, list):
            _err(f'{path}.mock', 'must be a callable or a list of dicts')


_KNOWN_SCHEMA_KEYS = {
    'base_url', 'auth', 'retry', 'rate_limit', 'pagination', 'log_level',
    'headers', 'timeout', 'on_error', 'on_event', 'on_event_kinds', 'on_request',
    'response_parser', 'canonical_parser', 'response_format',
    'state', 'session_config', 'files', 'csv_delimiter', 'encoding',
    'scrub_headers', 'scrub_query_params', 'endpoints', 'debug', 'metrics',
}


_KNOWN_RETRY_KEYS = {'max_attempts', 'backoff', 'on_codes', 'base_delay', 'max_delay', 'jitter', 'reactive_wait_on_terminal', 'max_retry_after'}
_KNOWN_RATE_LIMIT_KEYS = {'respect_retry_after', 'min_delay', 'strategy', 'requests_per_second', 'burst', 'on_limit', 'clock', 'sleep'}
_KNOWN_PAGINATION_KEYS = {'next_request', 'delay', 'initial_params', '_rf_pagination_helper'}

# lifecycle hooks that were moved from pagination to endpoint level.
# rejected with targeted redirecting messages in both strict and non-strict modes.
_MOVED_PAGINATION_KEYS = {
    'on_response': 'on_response',
    'on_page': 'on_page',
    'on_complete': 'on_complete',
    'on_page_complete': 'on_page_complete',
    'update_state': 'update_state',
}
_KNOWN_AUTH_KEYS = {
    'bearer': {'type', 'token', 'token_callback'},
    'basic': {'type', 'username', 'password'},
    'oauth2': {'type', 'token_url', 'client_id', 'client_secret', 'scope', 'expiry_margin'},
    'oauth2_password': {'type', 'token_url', 'client_id', 'client_secret', 'username', 'password', 'scope', 'expiry_margin'},
    'callback': {'type', 'handler'},
}

_KNOWN_ENDPOINT_KEYS = {
    'method', 'path', 'headers', 'params', 'body', 'form', 'files',
    'timeout', 'log_level', 'debug', 'response_format', 'response_parser',
    'canonical_parser', 'csv_delimiter', 'encoding', 'pagination',
    'on_response', 'on_page', 'on_complete', 'on_page_complete', 'update_state',
    'on_error', 'on_event', 'on_event_kinds', 'on_request',
    'scrub_headers', 'scrub_query_params', 'mock', 'playback', 'rate_limit', 'retry',
}


def validate(schema, strict=False):
    '''
    validates the top-level api client schema dict.
    raises SchemaError with a descriptive message on the first problem found.
    returns the schema unchanged so it can be used inline:
        client = APIClient(validate(my_schema))

    strict=True raises SchemaError on any unrecognised top-level or endpoint keys,
    which catches typos like 'on_requets' that would otherwise be silently ignored.
    '''
    _check_type(schema, dict, 'root')

    if 'base_url' not in schema:
        _err('base_url', 'missing required key')
    _check_type(schema['base_url'], str, 'base_url')

    if 'auth' in schema:
        _validate_auth(schema['auth'], strict=strict)

    if 'retry' in schema:
        _validate_retry(schema['retry'], strict=strict)

    if 'rate_limit' in schema:
        _validate_rate_limit(schema['rate_limit'], strict=strict)

    # client-level pagination defaults (inherited by endpoints)
    if 'pagination' in schema:
        _validate_pagination(schema['pagination'], strict=strict)

    if 'log_level' in schema:
        _check_one_of(schema['log_level'], _VALID_LOG_LEVELS, 'log_level')

    if 'headers' in schema:
        _check_type(schema['headers'], dict, 'headers')

    if 'timeout' in schema:
        _validate_timeout(schema['timeout'], 'timeout')

    if 'on_error' in schema and schema['on_error'] is not None:
        _check_callable(schema['on_error'], 'on_error')

    if 'on_event' in schema and schema['on_event'] is not None:
        _check_callable(schema['on_event'], 'on_event')
    if 'on_event_kinds' in schema:
        schema['on_event_kinds'] = _validate_on_event_kinds(schema['on_event_kinds'], 'on_event_kinds')
    if 'on_request' in schema and schema['on_request'] is not None:
        _check_callable(schema['on_request'], 'on_request')

    if 'response_parser' in schema and schema['response_parser'] is not None:
        _check_callable(schema['response_parser'], 'response_parser')

    if 'canonical_parser' in schema and schema['canonical_parser'] is not None:
        _check_callable(schema['canonical_parser'], 'canonical_parser')

    if 'response_format' in schema:
        _check_type(schema['response_format'], str, 'response_format')
        _check_one_of(schema['response_format'], VALID_RESPONSE_FORMATS, 'response_format')

    if 'state' in schema:
        _check_type(schema['state'], dict, 'state')

    if 'metrics' in schema:
        _validate_metrics(schema['metrics'], 'metrics')

    if 'session_config' in schema:
        sc = schema['session_config']
        _check_type(sc, dict, 'session_config')
        _VALID_SESSION_KEYS = {'verify', 'cert', 'proxies', 'max_redirects'}
        unknown = set(sc) - _VALID_SESSION_KEYS
        if unknown:
            _err('session_config', f'unknown key(s): {sorted(unknown)} — supported: {sorted(_VALID_SESSION_KEYS)}')
        if 'verify' in sc and not isinstance(sc['verify'], (bool, str)):
            _err('session_config.verify', 'must be a bool or a path string to a CA bundle')
        if 'cert' in sc and not isinstance(sc['cert'], (str, tuple)):
            _err('session_config.cert', 'must be a path string or (cert, key) tuple')
        if 'proxies' in sc:
            _check_type(sc['proxies'], dict, 'session_config.proxies')
        if 'max_redirects' in sc:
            _check_type(sc['max_redirects'], int, 'session_config.max_redirects')
            if sc['max_redirects'] < 0:
                _err('session_config.max_redirects', 'must be >= 0')

    if 'files' in schema:
        _err('files', 'client-level files is not supported — define files on an endpoint or pass it at call time')

    if 'csv_delimiter' in schema:
        _validate_csv_delimiter(schema['csv_delimiter'], 'csv_delimiter')

    if 'encoding' in schema:
        _validate_encoding(schema['encoding'], 'encoding')

    if 'scrub_headers' in schema:
        _validate_string_list(schema['scrub_headers'], 'scrub_headers')

    if 'scrub_query_params' in schema:
        _validate_string_list(schema['scrub_query_params'], 'scrub_query_params')

    if 'endpoints' not in schema:
        _err('endpoints', 'missing required key')
    _check_type(schema['endpoints'], dict, 'endpoints')

    if not schema['endpoints']:
        _err('endpoints', 'must define at least one endpoint')

    for name, endpoint in schema['endpoints'].items():
        _validate_endpoint(name, endpoint, strict=strict)

    if strict:
        unknown_root = set(schema) - _KNOWN_SCHEMA_KEYS
        if unknown_root:
            _err('root', f'unrecognised key(s): {sorted(unknown_root)} — use strict=False to suppress, or check for typos')
        for name, endpoint in schema['endpoints'].items():
            unknown_ep = set(endpoint) - _KNOWN_ENDPOINT_KEYS
            if unknown_ep:
                _err(f'endpoints.{name}', f'unrecognised key(s): {sorted(unknown_ep)} — use strict=False to suppress, or check for typos')

    return schema


def merge_dicts(base, override):
    '''
    deep merges two dicts. override wins on conflicts.
    non-dict values in override replace base entirely.
    used to merge client-level defaults with endpoint-level config.

    example:
        base     = {'headers': {'Accept': 'application/json'}, 'timeout': 30}
        override = {'headers': {'Authorization': 'Bearer x'}, 'timeout': 60}
        result   = {'headers': {'Accept': 'application/json', 'Authorization': 'Bearer x'}, 'timeout': 60}
    '''
    result = base | override  # override wins on shallow conflicts
    for k in base:
        if k in override and isinstance(base[k], dict) and isinstance(override[k], dict):
            result[k] = merge_dicts(base[k], override[k])
    return result


def resolve_endpoint(client_schema, endpoint_name):
    '''
    returns the fully resolved config for a single endpoint:
    client-level defaults merged with endpoint-level overrides.
    pagination is merged independently since it's a nested dict.
    '''
    endpoint = client_schema['endpoints'].get(endpoint_name)
    if endpoint is None:
        raise SchemaError(f'unknown endpoint: {endpoint_name!r}')

    # fields that propagate from client level to endpoint if not overridden
    inherited_keys = ('headers', 'on_error', 'on_event', 'on_event_kinds', 'on_request', 'log_level', 'response_parser', 'canonical_parser', 'response_format', 'csv_delimiter', 'encoding', 'scrub_headers', 'scrub_query_params', 'retry')
    resolved = {k: client_schema[k] for k in inherited_keys if k in client_schema}
    resolved = merge_dicts(resolved, endpoint)

    # retry merges separately: endpoint None explicitly suppresses retry attempts,
    # endpoint dict merges over client defaults, omission inherits unchanged
    client_retry = client_schema.get('retry')
    endpoint_retry = endpoint.get('retry', 'not_set')
    if endpoint_retry == 'not_set':
        if 'retry' in client_schema:
            resolved['retry'] = client_retry
        else:
            resolved.pop('retry', None)
    elif endpoint_retry is None:
        resolved['retry'] = None
    elif isinstance(endpoint_retry, dict) and client_retry:
        resolved['retry'] = merge_dicts(client_retry, endpoint_retry)
    else:
        resolved['retry'] = endpoint_retry

    # pagination merges separately: endpoint None means explicitly disabled,
    # endpoint dict means merge over client defaults
    client_pagination = client_schema.get('pagination')
    endpoint_pagination = endpoint.get('pagination', 'not_set')

    if endpoint_pagination == 'not_set':
        # endpoint didn't mention pagination — inherit client default
        resolved['pagination'] = client_pagination
    elif endpoint_pagination is None:
        # endpoint explicitly disabled pagination
        resolved['pagination'] = None
    elif isinstance(endpoint_pagination, dict) and client_pagination:
        # merge endpoint pagination over client defaults
        resolved['pagination'] = merge_dicts(client_pagination, endpoint_pagination)
    else:
        resolved['pagination'] = endpoint_pagination

    return resolved
