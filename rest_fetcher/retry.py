import logging
import math
import random
import time

import requests

from .exceptions import RateLimitError, RequestError

logger = logging.getLogger('rest_fetcher.retry')

# defaults used when retry config is partially specified
_DEFAULT_MAX_ATTEMPTS = 3
_DEFAULT_BACKOFF = 'exponential'
_DEFAULT_ON_CODES = [429, 500, 502, 503, 504]
_DEFAULT_BASE_DELAY = 1.0    # seconds, base for backoff calculation
_DEFAULT_MAX_DELAY = 120.0   # seconds, cap for backoff to avoid absurd waits
_DEFAULT_JITTER = False


def _apply_jitter(delay, jitter):
    if not jitter:
        return delay
    if jitter == 'full':
        return random.uniform(0.0, delay)
    if jitter == 'equal':
        half = delay / 2.0
        return half + random.uniform(0.0, half)
    return delay


def _backoff_delay(strategy, attempt, base_delay, max_delay, jitter=False):
    '''
    calculates how long to wait before attempt number `attempt` (1-indexed).
    attempt 1 means first retry, i.e. second total request.

        exponential: 1s, 2s, 4s, 8s, ... capped at max_delay
        linear:      1s, 2s, 3s, 4s, ... capped at max_delay
        callable:    user function(attempt) -> seconds
    '''
    if callable(strategy):
        delay = strategy(attempt)
    elif strategy == 'exponential':
        delay = base_delay * (2 ** (attempt - 1))
    elif strategy == 'linear':
        delay = base_delay * attempt
    else:
        delay = base_delay

    return _apply_jitter(min(delay, max_delay), jitter)


def _parse_retry_after(headers):
    '''
    parses Retry-After header, which can be either:
    - a number of seconds: "Retry-After: 30"
    - an http date: "Retry-After: Wed, 21 Oct 2025 07:28:00 GMT"
    returns float seconds or None if header is absent or unparseable.
    '''
    value = headers.get('Retry-After') or headers.get('retry-after')
    if value is None:
        return None

    # try plain seconds first
    try:
        seconds = float(value)
        if not math.isfinite(seconds) or seconds < 0:
            logger.warning('could not parse Retry-After header value: %r', value)
            return None
        return seconds
    except (ValueError, TypeError):
        pass

    # try http date format
    from email.utils import parsedate_to_datetime
    try:
        retry_time = parsedate_to_datetime(value).timestamp()
        wait = retry_time - time.time()
        return max(wait, 0.0)
    except Exception:  # deliberately broad: parsedate_to_datetime can raise many things; don't let header parsing failures mask the underlying error
        logger.warning('could not parse Retry-After header value: %r', value)
        return None


class RetryHandler:
    '''
    handles retry logic, backoff delays, and rate-limit / Retry-After header respect.

    constructed from the retry + rate_limit sub-dicts of the client schema:

        retry = {
            'max_attempts': 3,
            'backoff': 'exponential',   # or 'linear' or callable(attempt) -> seconds
            'on_codes': [429, 500, 502, 503, 504],
            'base_delay': 1.0,          # optional
            'max_delay': 120.0,         # optional
            'max_retry_after': 300.0    # optional: raise instead of waiting longer than this
        }
        rate_limit = {
            'respect_retry_after': True,
            'min_delay': 0.5,           # minimum seconds between any two requests
        }
    '''

    def __init__(self, retry_config=None, rate_limit_config=None):
        retry = retry_config or {}
        rl = rate_limit_config or {}

        self._max_attempts = retry.get('max_attempts', _DEFAULT_MAX_ATTEMPTS)
        self._backoff = retry.get('backoff', _DEFAULT_BACKOFF)
        self._on_codes = set(retry.get('on_codes', _DEFAULT_ON_CODES))
        self._base_delay = retry.get('base_delay', _DEFAULT_BASE_DELAY)
        self._max_delay = retry.get('max_delay', _DEFAULT_MAX_DELAY)
        self._jitter = retry.get('jitter', _DEFAULT_JITTER)
        self._reactive_wait_on_terminal = retry.get('reactive_wait_on_terminal', False)
        self._max_retry_after = retry.get('max_retry_after', None)

        self._respect_retry_after = rl.get('respect_retry_after', True)
        self._min_delay = rl.get('min_delay', 0.0)

        # monotonic tick of the last issued attempt.
        # do not use time.time() here: wall clock adjustments can break min_delay.
        self._last_request_tick = 0.0

    def _enforce_min_delay(self):
        'ensures minimum gap between requests regardless of retry status'
        if self._min_delay <= 0:
            return
        elapsed = time.monotonic() - self._last_request_tick
        wait = self._min_delay - elapsed
        if wait > 0:
            logger.debug('min_delay: sleeping %.2fs', wait)
            time.sleep(wait)

    def _resolve_retry_delay(self, response_headers, status_code, attempt):
        'decides how long to wait after a rate-limit or server error response'
        retry_after = None

        if self._respect_retry_after:
            retry_after = _parse_retry_after(response_headers)

        if retry_after is not None:
            if self._max_retry_after is not None and retry_after > self._max_retry_after:
                raise RateLimitError(
                    f'Retry-After {retry_after}s exceeds max_retry_after {self._max_retry_after}s',
                    retry_after=retry_after,
                    status_code=status_code
                )
            return retry_after

        return _backoff_delay(self._backoff, attempt, self._base_delay, self._max_delay, self._jitter)

    def _resolve_terminal_reactive_delay(self, response_headers, status_code):
        'decides terminal reactive wait when retries are suppressed or exhausted'
        retry_after = None
        if self._respect_retry_after:
            retry_after = _parse_retry_after(response_headers)
        if retry_after is not None:
            if self._max_retry_after is not None and retry_after > self._max_retry_after:
                raise RateLimitError(
                    f'Retry-After {retry_after}s exceeds max_retry_after {self._max_retry_after}s',
                    retry_after=retry_after,
                    status_code=status_code
                )
            return retry_after, 'retry_after'
        if self._min_delay > 0:
            return self._min_delay, 'min_delay'
        return None, None

    def _wait_for_retry_delay(self, delay, status_code, attempt, *, cause, ctx=None, on_wait=None):
        if cause == 'retry_after':
            logger.info(
                'rate limited (status %d), retry_after: sleeping %.1fs',
                status_code, delay
            )
        elif cause == 'min_delay':
            logger.info(
                'status %d on attempt %d, min_delay: sleeping %.1fs',
                status_code, attempt, delay
            )
        else:
            logger.info(
                'status %d on attempt %d, backoff: sleeping %.1fs',
                status_code, attempt, delay
            )
        start_wait = time.monotonic()
        time.sleep(delay)
        actual_wait = time.monotonic() - start_wait
        if on_wait is not None:
            on_wait(actual_wait, delay, cause)
        if ctx is not None:
            ctx.check_deadline()

    def execute(self, request_fn, ctx=None, on_retry=None, on_wait=None):
        '''
        executes request_fn() with retry logic applied.
        request_fn must be a zero-argument callable that:
            - returns a requests.Response on success
            - raises requests.RequestException on network-level failure

        ctx: optional OperationContext. when provided:
            - checks deadline before each attempt
            - checks max_requests before each attempt
            - records each attempt after it is issued

        raises RequestError after all attempts exhausted.
        raises RateLimitError if Retry-After exceeds max_retry_after.
        raises DeadlineExceeded if ctx deadline is hit. max_requests returns a stop signal via ctx.

        request context (endpoint, method, url) is NOT attached here — the caller
        (_FetchJob) is responsible for catching RequestError and enriching it.
        '''
        last_exc = None

        for attempt in range(1, self._max_attempts + 1):
            # check operation-level limits before issuing the attempt
            if ctx is not None:
                ctx.check_deadline()
                stop = ctx.check_max_requests()
                if stop is not None:
                    return stop

            self._enforce_min_delay()

            try:
                logger.debug('attempt %d/%d', attempt, self._max_attempts)
                self._last_request_tick = time.monotonic()
                if ctx is not None:
                    ctx.record_request()
                response = request_fn()

            except requests.RequestException as e:
                # network-level failure: connection error, timeout, DNS, SSL, etc.
                # non-requests exceptions (AttributeError, KeyError, etc.) are programming
                # errors — let them bubble up immediately rather than retrying
                last_exc = RequestError(
                    f'network error: {e}',
                    cause=e
                )
                if attempt == self._max_attempts:
                    break
                delay = _backoff_delay(self._backoff, attempt, self._base_delay, self._max_delay, self._jitter)
                if on_retry is not None:
                    try:
                        on_retry({'reason': 'network_error', 'attempt': attempt + 1, 'current_attempt': attempt, 'bytes_received': 0, 'planned_ms': delay * 1000.0})
                    except Exception:
                        # observability hook must not break retry logic
                        logger.warning('on_retry hook raised; suppressing exception', exc_info=True)
                logger.warning(
                    'network error on attempt %d/%d: %s — retrying in %.1fs',
                    attempt, self._max_attempts, e, delay
                )
                self._wait_for_retry_delay(delay, 0, attempt, cause='backoff', ctx=ctx, on_wait=on_wait)
                continue

            if response.status_code not in self._on_codes:
                # success or a non-retryable error code — return as-is, let caller handle it
                return response

            # retryable status code
            if attempt == self._max_attempts:
                if self._reactive_wait_on_terminal:
                    delay, cause = self._resolve_terminal_reactive_delay(response.headers, response.status_code)
                    if delay is not None and cause is not None:
                        self._wait_for_retry_delay(delay, response.status_code, attempt, cause=cause, ctx=ctx, on_wait=on_wait)
                try:
                    body = response.json()
                    api_msg = (body.get('error', {}).get('message')
                               or body.get('message')
                               or body.get('detail')
                               or '')
                except Exception:  # deliberately broad: don't let JSON decode failure mask the real HTTP error we're about to raise
                    api_msg = response.text[:300] if response.text else ''
                message = (f'status {response.status_code} after '
                           f'{self._max_attempts} attempts')
                if api_msg:
                    message += f' — {api_msg}'
                last_exc = RequestError(
                    message,
                    status_code=response.status_code,
                    response=response
                )
                break

            retry_after = _parse_retry_after(response.headers) if self._respect_retry_after else None
            delay = self._resolve_retry_delay(response.headers, response.status_code, attempt)
            cause = 'retry_after' if retry_after is not None else 'backoff'
            if on_retry is not None:
                try:
                    on_retry({'reason': f'status_{response.status_code}', 'attempt': attempt + 1, 'current_attempt': attempt, 'bytes_received': len(response.content) if hasattr(response, 'content') else 0, 'planned_ms': delay * 1000.0})
                except Exception:
                    logger.warning('on_retry hook raised; suppressing exception', exc_info=True)
            self._wait_for_retry_delay(delay, response.status_code, attempt, cause=cause, ctx=ctx, on_wait=on_wait)

        raise last_exc or RequestError(
            f'all {self._max_attempts} attempts failed'
        )


def build_retry_handler(retry_config=None, rate_limit_config=None):
    'factory: builds a RetryHandler from schema sub-dicts'
    return RetryHandler(retry_config, rate_limit_config)
