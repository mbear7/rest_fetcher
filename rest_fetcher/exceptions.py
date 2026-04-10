from __future__ import annotations

from typing import Any

import requests


class RestFetcherError(Exception):
    'base exception for all rest_fetcher errors'
    pass


class SchemaError(RestFetcherError):
    'raised when the api or endpoint schema dict is invalid or missing required keys'
    pass


class AuthError(RestFetcherError):
    'raised when authentication fails or auth config is invalid'
    pass


class RequestError(RestFetcherError):
    'raised when an http request fails after all retry attempts are exhausted'

    def __init__(self, message: str, status_code: int | None = None,
                 response: requests.Response | None = None,
                 cause: BaseException | None = None,
                 endpoint: str | None = None,
                 method: str | None = None,
                 url: str | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response = response
        self.cause = cause
        if cause is not None:
            self.__cause__ = cause
        # request context — attached by _FetchJob before re-raising, not by RetryHandler
        self.endpoint = endpoint
        self.method = method
        self.url = url


class RateLimitError(RequestError):
    'raised when rate limit is hit and retry-after is not respected or wait is too long'

    def __init__(self, message: str, retry_after: float | None = None, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class ResponseError(RestFetcherError):
    'raised by on_response callback when the api returns an application-level error'
    # http layer succeeded (2xx) but the api body signals an error
    # e.g. {"error": "invalid_token"} with status 200

    def __init__(self, message: str, raw: Any = None) -> None:
        super().__init__(message)
        self.raw = raw


class PaginationError(RestFetcherError):
    'raised when pagination callbacks return unexpected types or contradict each other'
    pass


class PlaybackError(RestFetcherError):
    "raised when playback file is missing, malformed, or cannot represent a response"
    pass


class FixtureFormatError(PlaybackError):
    'raised when a playback/recording fixture violates the internal fixture contract'
    pass


class StateViewMutationError(RestFetcherError):
    '''
    raised when a pagination callback attempts to write to the StateView passed as `state`.

    callbacks receive a StateView — a read-only snapshot of run-local page_state,
    including the current post-auth request under _request. any write raises this immediately
    rather than silently dropping the value.

    to persist values across pages, use update_state:
        'update_state': lambda resp, state: {'key': new_value}
    update_state runs before next_request, so next_request always sees updated counters.

    to read pagination counters without state tracking:
        req_params = (state.get('_request') or {}).get('params') or {}
        current_page = req_params.get('page', 1)
    _request is captured after auth injection, so auth headers are visible.
    '''
    def __init__(self, key: Any) -> None:
        msg = (
            f"State is read-only (callback received a snapshot). "
            f"Attempted mutation: state[{key!r}]. "
            f"Return {{{key!r}: ...}} from update_state(...) to persist counters across pages."
        )
        super().__init__(msg)
        self.key = key


class CallbackError(RestFetcherError):
    'raised when a user-supplied callback raises an unexpected exception'

    def __init__(self, message: str, callback_name: str | None = None,
                 cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.callback_name = callback_name
        self.cause = cause


class DeadlineExceeded(RestFetcherError):
    '''
    raised when the elapsed time (measured with time.monotonic()) exceeds time_limit.
    checked before each page fetch and before each http attempt, so backoff sleeps
    count toward the deadline.

    attributes:
        limit:   the time_limit value that was set (seconds)
        elapsed: actual elapsed time at the point the check fired (seconds)
    '''
    def __init__(self, limit: float, elapsed: float) -> None:
        super().__init__(
            f'time_limit exceeded: {elapsed:.2f}s elapsed, limit was {limit:.2f}s'
        )
        self.limit = limit
        self.elapsed = elapsed


def raise_(exc_or_msg: str | BaseException, cls: type[RestFetcherError] = ResponseError,
           **kwargs: Any) -> None:
    '''
    helper for raising exceptions from lambdas in callbacks.

    usage in schema:
        'on_response': lambda resp, state: raise_(resp['error']) if 'error' in resp else resp['data']
        'on_response': lambda resp, state: raise_(resp['error'], cls=AuthError) if 'error' in resp else resp['data']
    '''
    if isinstance(exc_or_msg, Exception):
        raise exc_or_msg
    raise cls(exc_or_msg, **kwargs)


class RateLimitExceeded(RateLimitError):
    "raised when proactive token-bucket limiting is configured with on_limit='raise'"

    def __init__(self, message: str, retry_after: float | None = None, **kwargs: Any) -> None:
        super().__init__(message, retry_after=retry_after, **kwargs)
