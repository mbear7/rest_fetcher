import logging
import threading
import time

import requests

from .exceptions import AuthError, SchemaError

logger = logging.getLogger('rest_fetcher.auth')


class BaseAuth:
    "base class for all auth handlers, subclass and implement apply()"

    def apply(self, request_kwargs):
        """
        receives the current request kwargs dict,
        returns modified request kwargs with auth injected.
        request_kwargs keys mirror requests.Session.request() signature:
            headers, params, json, data, auth, ...
        """
        raise NotImplementedError


class BearerAuth(BaseAuth):
    """
    injects Authorization: Bearer <token> header.
    token can be a static string or a callable that returns a string —
    useful for tokens that rotate or are fetched from a vault at runtime.

    schema example (static):
        'auth': {'type': 'bearer', 'token': 'my-secret-token'}

    schema example (dynamic):
        'auth': {'type': 'bearer', 'token_callback': lambda config: get_token_from_vault(config['param_name'])}
    """

    def __init__(self, auth_config, config_view=None):
        self._token = auth_config.get('token')
        self._token_callback = auth_config.get('token_callback')
        self._config_view = config_view

    def apply(self, request_kwargs):
        token = self._token_callback(self._config_view) if self._token_callback else self._token
        if not token:
            raise AuthError('bearer token is empty or None')
        headers = request_kwargs.get('headers', {}) | {'Authorization': f'Bearer {token}'}
        return request_kwargs | {'headers': headers}


class BasicAuth(BaseAuth):
    """
    injects http basic auth credentials.

    schema example:
        'auth': {'type': 'basic', 'username': 'user', 'password': 'pass'}
    """

    def __init__(self, auth_config):
        self._username = auth_config['username']
        self._password = auth_config['password']

    def apply(self, request_kwargs):
        return {**request_kwargs, 'auth': (self._username, self._password)}


class _OAuth2BaseAuth(BaseAuth):
    "shared token-fetching and caching logic for oauth2 handlers."

    def __init__(self, auth_config, timeout=30):
        self._token_url = auth_config['token_url']
        self._client_id = auth_config['client_id']
        self._client_secret = auth_config['client_secret']
        self._scope = auth_config.get('scope')
        self._expiry_margin = auth_config.get('expiry_margin', 60)
        self._timeout = timeout
        self._token = None
        self._expires_at = 0
        self._lock = threading.Lock()

    def _build_payload(self):
        raise NotImplementedError

    def _compute_expires_at(self, expires_in):
        try:
            expires_in_value = int(expires_in)
        except (TypeError, ValueError):
            expires_in_value = 3600
        refresh_after = max(0, expires_in_value - self._expiry_margin)
        return time.monotonic() + refresh_after, expires_in_value

    def _fetch_token(self):
        logger.debug('fetching oauth2 token from %s', self._token_url)
        payload = self._build_payload()
        try:
            response = requests.post(self._token_url, data=payload, timeout=self._timeout)
            response.raise_for_status()
        except requests.RequestException as e:
            raise AuthError(f'oauth2 token request failed: {e}') from e

        try:
            data = response.json()
        except ValueError as e:
            raise AuthError('oauth2 token response is not valid json') from e
        if 'access_token' not in data:
            raise AuthError(f'oauth2 response missing access_token: {data}')

        self._token = data['access_token']
        self._expires_at, expires_in = self._compute_expires_at(data.get('expires_in', 3600))
        logger.debug('oauth2 token acquired, expires in %ds', expires_in)

    def _is_expired(self):
        # Use monotonic time for in-process token expiry bookkeeping so wall-clock
        # adjustments do not make cached tokens appear to expire earlier or later.
        return time.monotonic() >= self._expires_at

    def apply(self, request_kwargs):
        with self._lock:
            if self._token is None or self._is_expired():
                self._fetch_token()
            token = self._token
        headers = request_kwargs.get('headers', {}) | {'Authorization': f'Bearer {token}'}
        return request_kwargs | {'headers': headers}


class OAuth2Auth(_OAuth2BaseAuth):
    """
    client credentials oauth2 flow.
    fetches a token from token_url using client_id and client_secret,
    caches it and refreshes automatically when it expires.

    schema example:
        'auth': {
            'type': 'oauth2',
            'token_url': 'https://auth.example.com/oauth/token',
            'client_id': 'my-client-id',
            'client_secret': 'my-client-secret',
            'scope': 'read:data',          # optional
            'expiry_margin': 60            # optional, seconds before expiry to refresh (default 60)
        }
    """

    def _build_payload(self):
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self._client_id,
            'client_secret': self._client_secret,
        }
        if self._scope:
            payload['scope'] = self._scope
        return payload


class OAuth2PasswordAuth(_OAuth2BaseAuth):
    """
    password grant oauth2 flow.
    fetches a token from token_url using client credentials and user credentials,
    caches it and re-authenticates automatically when it expires.
    """

    def __init__(self, auth_config, timeout=30):
        super().__init__(auth_config, timeout=timeout)
        self._username = auth_config['username']
        self._password = auth_config['password']

    def _build_payload(self):
        payload = {
            'grant_type': 'password',
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'username': self._username,
            'password': self._password,
        }
        if self._scope:
            payload['scope'] = self._scope
        return payload


class CallbackAuth(BaseAuth):
    """
    fully custom auth via user-supplied callable.
    handler receives (request_kwargs, config_view) and must return modified request_kwargs.
    gives full control: can modify headers, params, body, or anything else.

    schema example:
        'auth': {
            'type': 'callback',
            'handler': lambda req, config: {
                **req,
                'headers': {**req.get('headers', {}), 'X-Api-Key': config.get('api_key')}
            }
        }
    """

    def __init__(self, auth_config, config_view=None):
        self._handler = auth_config['handler']
        self._config_view = config_view

    def apply(self, request_kwargs):
        result = self._handler(request_kwargs, self._config_view)
        if not isinstance(result, dict):
            raise AuthError(
                f'auth callback must return a dict of request kwargs, got {type(result).__name__}'
            )
        return result


# registry mapping type string to handler class
_AUTH_HANDLERS = {
    'bearer': BearerAuth,
    'basic': BasicAuth,
    'oauth2': OAuth2Auth,
    'oauth2_password': OAuth2PasswordAuth,
    'callback': CallbackAuth,
}

_OAUTH_TIMEOUT_HANDLERS = {OAuth2Auth, OAuth2PasswordAuth}


def build_auth_handler(auth_config, config_view=None, timeout=30):
    """
    factory: receives the auth sub-dict from the schema and returns
    the appropriate BaseAuth subclass instance.

    returns None if auth_config is None (no auth needed).
    """
    if auth_config is None:
        return None

    auth_type = auth_config.get('type')
    handler_cls = _AUTH_HANDLERS.get(auth_type)
    if handler_cls is None:
        raise SchemaError(f'unknown auth type: {auth_type!r}')

    if handler_cls in _OAUTH_TIMEOUT_HANDLERS:
        return handler_cls(auth_config, timeout=timeout)
    if handler_cls in {BearerAuth, CallbackAuth}:
        return handler_cls(auth_config, config_view=config_view)
    return handler_cls(auth_config)
