# this file is not intended to be run directly.
if __name__ == '__main__':
    raise SystemExit(
        'examples.py is reference code, not a runnable script.\n'
        'See examples/ for runnable scripts (anthropic_example.py etc.).'
    )
# it demonstrates schema-building recipes and API patterns — treat it as
# annotated reference code. undefined names (save_to_warehouse, handle_page, etc.) are
# stand-ins for whatever your pipeline does with the fetched data.
"""
rest_fetcher usage examples

covers:
  1.  simple GET, bearer auth
  2.  paginated GET, cursor strategy
  3.  POST with body, client-level pagination defaults
  4.  call-time param overrides
  5.  custom pagination with state
  6.  api-level error detection in on_response
  7.  context manager
  8.  mock for testing
  9.  error handling, retry, rate limit, safety caps (max_requests, max_pages, time_limit)
  10. airflow dag pattern
  11. SchemaBuilder fluent API
  12. response_parser: 1-arg vs 2-arg
  13. params_mode="replace" — clear sticky params
  14. exception context attributes
  15. state seeding for auth callbacks
  16. path_params — URL template interpolation
  17. form-encoded POST (form= key)
  18. offset_pagination and page_number_pagination built-in strategies
  19. link_header_pagination — RFC 5988 Link header (e.g. GitHub)
  20. oauth2 client credentials auth
  20a. oauth2 password grant auth (e.g. GLPI v2)
  21. bearer token_callback — dynamic / rotating tokens
  22. on_page callback — streaming vs buffering modes (three patterns)
  22a. stream_run() — streaming with a summary handle
  22b. on_page progress logging using requests_so_far
  23. update_state — explicit state mutations across pages
  24. initial_params in custom pagination
  24a. _request in state — reading current params without update_state
  25. per-endpoint log_level and debug flag
  26. playback — record once, replay for dev/test
  26a. playback record_as_bytes — store any response as raw bytes
  27. client-level response_parser + endpoint override
  28. SchemaBuilder — client-level pagination and rate_limit
  29. client-level on_error — shared error handling
  29a. on_error stop-after-error-threshold using errors_so_far
  30. mock with callable — dynamic per-request mock responses
  31. headers — three-layer merge (client / endpoint / call-time)
  32. timeout — scalar, tuple (connect, read), per-endpoint override
  33. retry.backoff as callable — custom delay curve + base_delay / max_delay
  34. scrub_headers / scrub_query_params — request-side scrubbing in logs and playback fixtures
  35. response_format='text' — plain text endpoints
  36. response_format='xml' — XML endpoints
  37. response_format='csv' — CSV endpoints
  38. nested-path pagination — data_path / total_path / next_cursor_path
  39. raw playback for callback debugging — record once, iterate locally
  40. bounded stop behaviour — max_pages / max_requests preserve fetched pages
  41. stream() completion summary — on_complete receives StreamSummary, not page list
  42. on_event + token bucket + playback source semantics
  43. rate_limit on_limit='raise' + RateLimitExceeded
  44. on_request — pre-auth request shaping from run-local state
  45. files + form — multipart upload
  46. session_config — requests.Session transport settings
  47. stream_run summary — endpoint / source / retries / elapsed
  48. on_page_complete — adaptive cooldown after repeated errors
  49. on_page_complete — light pacing after slow pages
  50. on_page_complete — observing error_stop outcome
  51. client metrics session — cumulative top-level totals across runs
  52. shared MetricsSession across multiple clients
  53. JWT auth recipe — Airflow-style token acquisition via callback auth
  54. total_entries-aware pagination — avoids extra empty-page request
  55. fetch_pages() — always-a-list alternative to fetch()
  56. PaginationEvent.to_dict() — JSON-serializable event for ETL audit logging
"""

from rest_fetcher import (
    APIClient,
    DeadlineExceeded,
    PageCycleOutcome,
    RequestError,
    ResponseError,
    cursor_pagination,
    link_header_pagination,
    offset_pagination,
    page_number_pagination,
    raise_,
    MetricsSession,
)
from rest_fetcher import SchemaBuilder

# example 1: simple GET, no pagination, bearer token auth

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-secret-token'},
        'log_level': 'medium',
        'endpoints': {
            'whoami': {
                'method': 'GET',
                'path': '/me',
            }
        },
    }
)

profile = client.fetch('whoami')
# returns a dict: {'id': 123, 'name': 'Alice', ...}


# example 2: paginated GET using built-in cursor strategy

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-secret-token'},
        'endpoints': {
            'list_events': {
                'method': 'GET',
                'path': '/events',
                'params': {'page_size': 200},
                'playback': {
                    'path': 'examples/fixtures/example_02_list_events.json',
                    'mode': 'none',  # change to 'load' to try offline replay
                },
                # response looks like: {'items': [...], 'meta': {'next_cursor': 'xyz'}}
                'pagination': cursor_pagination(
                    cursor_param='cursor', next_cursor_path='meta.next_cursor', data_path='items'
                ),
            }
        },
    }
)

# fetch all pages into memory — returns a list of page values; each page here is the list
# returned by on_response. if you need one final cross-page transform, use on_complete:
#   'on_complete': lambda pages, state: [row for page in pages for row in page]
# that buffering cost is fetch-specific. stream() stays incremental even when on_complete is set.
all_events = client.fetch('list_events')

# or stream page by page — useful for large result sets and lower memory use
for page in client.stream('list_events'):
    save_to_warehouse(page)

# stream_run() is stream() with a handle. iterate it like a generator, then
# inspect run.summary afterwards (only set on normal exhaustion).
run = client.stream_run('list_events', max_pages=50)
for page in run:
    save_to_warehouse(page)

if run.summary is not None:
    print(f'pages={run.summary.pages} requests={run.summary.requests}')
    if run.summary.stop:
        print(
            f'stopped by {run.summary.stop.kind}: {run.summary.stop.observed}/{run.summary.stop.limit}'
        )

# safety caps — protect against runaway pagination or slow APIs
# max_pages / max_requests are non-destructive local caps. the library returns
# normally with already-fetched pages preserved. if you care why it stopped, use
# on_complete and inspect state['stop'].
# time_limit remains destructive: it still raises DeadlineExceeded when elapsed
# time reaches the limit. checked after every sleep (Retry-After and backoff),
# not only at the top of the retry loop, so a Retry-After: 60 response with
# time_limit=0.5 won't oversleep the full 60s.
for page in client.stream('list_events', max_pages=50):
    save_to_warehouse(page)

try:
    for page in client.stream('list_events', time_limit=300.0):
        save_to_warehouse(page)
except DeadlineExceeded as e:
    print(f'stopped at deadline: {e.elapsed:.1f}s elapsed (limit={e.limit}s)')


# example 3: POST with body, client-level pagination defaults

# models the yandex direct pattern: POST endpoints sharing one pagination config

client = APIClient(
    {
        'base_url': 'https://api.direct.yandex.com/json/v5',
        'auth': {'type': 'bearer', 'token': 'yandex-token'},
        'log_level': 'medium',
        # client-level pagination: all endpoints inherit the mechanics (next_request, delay)
        'pagination': {
            'next_request': lambda parsed_body, state: (
                # use 'json' (not 'body') — _build_initial_request maps body -> request['json'],
                # and the pagination runner merges overrides into that same request dict.
                # merge_dicts deep-merges nested dicts, so only the Page.Offset key is updated.
                {'json': {'params': {'Page': {'Offset': parsed_body.get('LimitedBy')}}}}
                if parsed_body.get('LimitedBy')
                else None
            ),
            'delay': 0.1,
        },
        'endpoints': {
            'campaigns': {
                'method': 'POST',
                'path': '/campaigns',
                'body': {
                    'method': 'get',
                    'params': {
                        'SelectionCriteria': {'Statuses': ['ACCEPTED']},
                        'FieldNames': ['Id', 'Name'],
                        'Page': {'Limit': 10000},
                    },
                },
                # endpoint-level pagination overrides client mechanics; lifecycle hooks live here
                'pagination': {
                    'next_request': lambda parsed_body, state: (
                        {'json': {'params': {'Page': {'Offset': parsed_body.get('LimitedBy')}}}}
                        if parsed_body.get('LimitedBy')
                        else None
                    ),
                },
                'on_response': lambda resp, state: resp.get('Campaigns'),
            },
            'reports': {
                'method': 'POST',
                'path': '/reports',
                'pagination': None,  # explicitly disable inherited pagination for this endpoint
            },
        },
    }
)

campaigns = client.fetch('campaigns')


# example 4: call-time param overrides

# params/headers/body passed at call time merge over schema defaults

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'basic', 'username': 'user', 'password': 'pass'},
        'endpoints': {
            'search': {
                'method': 'GET',
                'path': '/search',
                'params': {'limit': 100},  # default limit
            }
        },
    }
)

# override limit and add a filter at call time
results = client.fetch('search', params={'limit': 50, 'q': 'python'})


# example 5: custom pagination with state

# models the yandex metrika pattern: stop condition uses both
# current offset and total_rows from the response

# callbacks receive a StateView of one run-local page_state dict.
# direct mutations (state['key'] = value) raise StateViewMutationError immediately.
# use update_state to persist values across pages; its return dict is merged into page_state.


def metrika_update_state(page_payload, state):
    "runs after on_response and on_page; return dict merged into page_state before next_request"
    current_offset = state.get('offset', 0)
    return {'offset': current_offset + len(page_payload)}


def metrika_next_request(parsed_body, state):
    # state['offset'] is now reliably set by update_state before this is called
    offset = state.get('offset', 0)
    if parsed_body.get('data') is None or offset >= parsed_body.get('total_rows', 0):
        return None
    return {'params': {'offset': offset}}


client = APIClient(
    {
        'base_url': 'https://api-metrika.yandex.net',
        'auth': {
            'type': 'callback',
            'handler': lambda req, config: {
                **req,
                'headers': {**req.get('headers', {}), 'Authorization': 'OAuth my-token'},
            },
        },
        'endpoints': {
            'report': {
                'method': 'GET',
                'path': '/stat/v1/data',
                'playback': {
                    'path': 'examples/fixtures/example_05_custom_pagination_state.json',
                    'mode': 'none',  # change to 'load' to try offline replay
                },
                'pagination': {
                    'next_request': metrika_next_request,  # reads offset set by update_state
                    'delay': 0.2,
                },
                'update_state': metrika_update_state,  # advances offset in page_state
                'on_response': lambda resp, state: resp.get('data', []),
                'on_complete': lambda pages, state: [row for page in pages for row in page],
            }
        },
    }
)

# fetch() + on_complete flattens all pages into one list. this is a final
# cross-page transform, so fetch() keeps the page values in memory until completion.
all_rows = client.fetch(
    'report',
    params={
        'ids': '12345678',
        'metrics': 'ym:s:visits',
        'date1': '2025-01-01',
        'date2': '2025-01-31',
    },
)

# add a wall-clock deadline — useful for Airflow tasks with a hard SLA.
# backoff sleeps and inter-page delay both count toward the budget.
try:
    all_rows = client.fetch(
        'report',
        params={
            'ids': '12345678',
            'metrics': 'ym:s:visits',
            'date1': '2025-01-01',
            'date2': '2025-01-31',
        },
        time_limit=120.0,  # abort if the full fetch exceeds 2 minutes
    )
except DeadlineExceeded as e:
    print(f'fetch aborted after {e.elapsed:.1f}s (limit={e.limit}s)')


# example 6: api-level error detection in on_response

# some apis return 200 OK but signal errors in the body

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {
            'data': {
                'method': 'GET',
                'path': '/data',
                'on_response': lambda resp, state: (
                    raise_(resp['error']['message'], cls=ResponseError)
                    if 'error' in resp
                    else resp['data']
                ),
            }
        },
    }
)


# example 7: context manager — session closes cleanly on exit

with APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {'orders': {'method': 'GET', 'path': '/orders'}},
    }
) as client:
    orders = client.fetch('orders')
# session is closed here automatically


# example 8: mock for testing — no real http calls

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'test-token'},
        'endpoints': {
            'list_users': {
                'method': 'GET',
                'path': '/users',
                'pagination': cursor_pagination('cursor', 'next_cursor'),
                # list of dicts: each entry is one page, served sequentially
                'mock': [
                    {'items': [{'id': 1}, {'id': 2}], 'next_cursor': 'page2'},
                    {'items': [{'id': 3}], 'next_cursor': None},
                ],
            }
        },
    }
)

pages = list(client.stream('list_users'))
# pages == [[{'id': 1}, {'id': 2}], [{'id': 3}]]


# example 9: error handling, retry, rate limit

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'retry': {
            'max_attempts': 3,
            'backoff': 'exponential',
            'on_codes': [429, 500, 502, 503],
        },
        'rate_limit': {
            'respect_retry_after': True,
            'min_delay': 0.5,
            # raise instead of waiting more than 60s
        },
        'endpoints': {
            'fragile': {
                'method': 'GET',
                'path': '/fragile',
                # skip this endpoint on 404 instead of raising
                'on_error': lambda exc, state: 'skip' if exc.status_code == 404 else 'raise',
            }
        },
    }
)

try:
    data = client.fetch('fragile')
except RequestError as e:
    print(f'failed after retries: endpoint={e.endpoint} status={e.status_code}')

# max_requests caps total http attempts (first try + all retries) across the entire operation.
# useful when you want a hard ceiling on API calls regardless of retry config.
# unlike time_limit, this cap is non-destructive: already fetched data is preserved and
# fetch() still returns normally. if you care why the run stopped, inspect state['stop'] in
# on_complete or another callback. stream() follows the same bounded-stop rule; if stream()
# also has on_complete, that callback receives StreamSummary rather than the page list.
data = client.fetch('fragile', max_requests=10)


# example 10: airflow dag usage pattern


def fetch_campaigns_task(**context):
    import json

    with APIClient(
        {
            'base_url': 'https://api.direct.yandex.com/json/v5',
            'auth': {'type': 'bearer', 'token': context['var']['value']['yandex_token']},
            'log_level': 'medium',
            'endpoints': {
                'campaigns': {
                    'method': 'POST',
                    'path': '/campaigns',
                    'body': {
                        'method': 'get',
                        'params': {
                            'SelectionCriteria': {},
                            'FieldNames': ['Id', 'Name', 'Status'],
                        },
                    },
                    'pagination': {
                        'next_request': lambda parsed_body, state: (
                            # use 'json' not 'body' — the request dict uses 'json' after
                            # _build_initial_request maps schema body -> request['json']
                            {'json': {'params': {'Page': {'Offset': parsed_body['LimitedBy']}}}}
                            if parsed_body.get('LimitedBy')
                            else None
                        ),
                    },
                    'on_response': lambda resp, state: resp.get('result', {}).get('Campaigns', []),
                    'on_complete': lambda pages, state: [c for page in pages for c in page],
                }
            },
        }
    ) as client:
        campaigns = client.fetch('campaigns')

    context['ti'].xcom_push(key='campaigns', value=json.dumps(campaigns))


# example 11: SchemaBuilder fluent API
#
# SchemaBuilder produces a plain dict identical to a hand-written schema.
# it is a developer-experience layer only — zero runtime difference.
# use it for IDE autocompletion and to avoid dict typos.

schema = (
    SchemaBuilder('https://api.example.com/v1')
    .bearer('my-secret-token')
    .timeout(30)  # always set — requests has no default timeout
    .retry(max_attempts=3, backoff='exponential', on_codes=[429, 500, 502, 503])
    .state(region='us-east-1')  # read-only auth config + initial run-state seed
    .scrub_headers('X-Tenant-Id')  # extra header to redact in logs
    .endpoint('whoami', method='GET', path='/me')
    .endpoint('search', method='GET', path='/search', params={'limit': 100})
    .endpoint('create', method='POST', path='/users')
    .build()  # returns plain dict — pass to APIClient
)

client = APIClient(schema)
profile = client.fetch('whoami')


# example 12: response_parser — 1-arg vs 2-arg
#
# 1-arg parser: receives the raw requests.Response only.
#
# 2-arg parser: receives:
#   (response, parsed)
# where `parsed` is the library's canonical parsed body for the configured or
# inferred response_format:
#
#   json  -> Python JSON value
#   text  -> str
#   xml   -> ElementTree.Element
#   csv   -> list[dict[str, str]]
#   bytes -> bytes
#   empty body -> None
#
# arity is detected once at construction. a second parameter with a default
# (def p(r, parsed=None)) still counts as 2-arg — the library will call it
# with both arguments. *args also counts as 2-arg.

# 1-arg: parse XML instead of JSON
import xml.etree.ElementTree as ET

client = APIClient(
    {
        'base_url': 'https://api.example.com',
        'response_parser': lambda r: {'root': ET.fromstring(r.content).tag},
        'endpoints': {'data': {'method': 'GET', 'path': '/data.xml'}},
    }
)


# 2-arg: build on top of the library's default parsing
def strict_parser(response, parsed):
    if parsed is None:
        raise ResponseError('empty response body')
    if 'data' not in parsed:
        raise ResponseError(f'missing data key, got: {list(parsed.keys())}')
    return parsed['data']


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'response_parser': strict_parser,
        'endpoints': {'items': {'method': 'GET', 'path': '/items'}},
    }
)


# 2-arg with optional second param — detected as 2-arg, called with both args
def flexible_parser(response, parsed=None):
    if parsed is not None:
        return parsed.get('result', parsed)
    return {'raw': response.text}


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'response_parser': flexible_parser,
        'endpoints': {'ep': {'method': 'GET', 'path': '/ep'}},
    }
)


# example 13: params_mode="replace" — clear sticky params between pages
#
# by default, params carry forward across pages (merge mode).
# use params_mode="replace" when the next-page params must be exact.
# this is common when the API returns a full next-page URL or token
# that should not be combined with the original filter params.


def search_next_request(parsed_body, state):
    next_token = parsed_body.get('pagination', {}).get('next_token')
    if not next_token:
        return None
    # replace: discard original filter params, send only the continuation token
    return {
        'params': {'continuation_token': next_token},
        'params_mode': 'replace',
    }


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'endpoints': {
            'search': {
                'method': 'GET',
                'path': '/search',
                'pagination': {
                    'next_request': search_next_request,
                },
                'on_response': lambda resp, state: resp.get('results', []),
            }
        },
    }
)

# call-time filter params are present on page 1, but cleared on subsequent pages
results = client.fetch('search', params={'q': 'python', 'category': 'books'})


# example 14: exception context attributes
#
# RequestError carries .endpoint, .method, and .url set to the actual
# post-auth request values. useful for structured logging and alerting.

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'retry': {'max_attempts': 3, 'on_codes': [500, 502, 503]},
        'endpoints': {
            'reports': {'method': 'GET', 'path': '/reports'},
        },
    }
)

try:
    data = client.fetch('reports')
except RequestError as e:
    # all three are always populated — even for non-retryable errors like 400, 404
    print(f'request failed: endpoint={e.endpoint} method={e.method} url={e.url}')
    print(f'  status={e.status_code}')
    if e.cause:
        print(f'  network cause: {e.cause}')  # set for network errors, None for HTTP errors


# example 15: state seeding for auth callbacks
#
# schema['state'] is one source consumed in two ways: auth reads it as
# read-only config, and callbacks get a copied run-local seed. use it to
# inject config or secrets that auth needs without global variables.


def signing_auth(request_kwargs, config):
    import hashlib
    import hmac
    import time

    secret = config['signing_secret'].encode()
    ts = str(int(time.time()))
    sig = hmac.new(secret, ts.encode(), hashlib.sha256).hexdigest()
    headers = request_kwargs.get('headers', {})
    return {**request_kwargs, 'headers': {**headers, 'X-Timestamp': ts, 'X-Signature': sig}}


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'state': {'signing_secret': 'super-secret-key'},  # read by auth, copied into run state
        'auth': {'type': 'callback', 'handler': signing_auth},
        'scrub_headers': ['X-Signature'],  # redact from logs
        'endpoints': {'data': {'method': 'GET', 'path': '/data'}},
    }
)

# SchemaBuilder equivalent — cleaner for complex auth setups
schema = (
    SchemaBuilder('https://api.example.com/v1')
    .state(signing_secret='super-secret-key')
    .auth_callback(signing_auth)
    .scrub_headers('X-Signature')
    .endpoint('data', method='GET', path='/data')
    .build()
)


# example 16: path_params — URL template interpolation
#
# path templates use {placeholder} syntax. values are supplied at call time
# via path_params. the library will warn if any placeholders remain unreplaced.

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {
            'get_user': {'method': 'GET', 'path': '/users/{user_id}'},
            'get_post': {'method': 'GET', 'path': '/users/{user_id}/posts/{post_id}'},
            'delete_doc': {'method': 'DELETE', 'path': '/orgs/{org}/repos/{repo}/docs/{doc_id}'},
        },
    }
)

user = client.fetch('get_user', path_params={'user_id': 42})
post = client.fetch('get_post', path_params={'user_id': 42, 'post_id': 7})
client.fetch('delete_doc', path_params={'org': 'acme', 'repo': 'api-docs', 'doc_id': 99})

# SchemaBuilder equivalent
schema = (
    SchemaBuilder('https://api.example.com/v1')
    .bearer('my-token')
    .endpoint('get_user', method='GET', path='/users/{user_id}')
    .endpoint('get_post', method='GET', path='/users/{user_id}/posts/{post_id}')
    .build()
)


# example 17: form-encoded POST (application/x-www-form-urlencoded)
#
# use 'form' instead of 'body' when the API expects form-encoded data.
# the library maps form -> requests' data= parameter, which sets
# Content-Type: application/x-www-form-urlencoded automatically.
# common for OAuth2 token endpoints and form-based REST APIs.
#
# 'body' and 'form' are mutually exclusive — using both raises SchemaError.
# call-time form= merges over schema form (same semantics as body= and params=).

client = APIClient(
    {
        'base_url': 'https://auth.example.com',
        'endpoints': {
            # static form fields in schema — grant_type is always 'client_credentials'
            'token': {
                'method': 'POST',
                'path': '/oauth/token',
                'form': {
                    'grant_type': 'client_credentials',
                    'scope': 'read write',
                },
            },
            # password grant — static grant_type, credentials supplied at call time
            'login': {
                'method': 'POST',
                'path': '/oauth/token',
                'form': {'grant_type': 'password'},
            },
        },
    }
)

# supply client credentials at call time — merges over schema form
token = client.fetch(
    'token',
    form={
        'client_id': 'my-app',
        'client_secret': 'my-secret',
    },
)
# requests sends: grant_type=client_credentials&scope=read+write&client_id=my-app&client_secret=my-secret

# username/password at call time; grant_type=password comes from schema
session = client.fetch('login', form={'username': 'alice', 'password': 's3cr3t'})


# example 18: offset_pagination and page_number_pagination built-in strategies
#
# offset_pagination: classic offset/limit via query params.
# page_number_pagination: 1-indexed page number via query params.
# both accept optional total_path / total_pages_path for precise stop detection.

# offset — response: {'data': [...], 'total': 629}
client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {
            'users': {
                'method': 'GET',
                'path': '/users',
                'playback': {
                    'path': 'examples/fixtures/example_18_offset_users.json',
                    'mode': 'none',  # change to 'load' to try offline replay
                },
                'pagination': offset_pagination(
                    offset_param='offset',
                    limit_param='limit',
                    limit=100,
                    data_path='data',
                    total_path='total',  # avoids over-fetching on the last page
                ),
            }
        },
    }
)

all_users = client.fetch('users')

# page_number — response: {'results': [...], 'meta': {'total_pages': 13}}
client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {
            'posts': {
                'method': 'GET',
                'path': '/posts',
                'playback': {
                    'path': 'examples/fixtures/example_18_page_number_posts.json',
                    'mode': 'none',  # change to 'load' to try offline replay
                },
                'pagination': page_number_pagination(
                    page_param='page',
                    page_size_param='per_page',
                    page_size=50,
                    data_path='results',
                    total_pages_path='meta.total_pages',
                ),
            }
        },
    }
)

all_posts = client.fetch('posts')

# page_number without total_pages_path — stops on short page heuristic
client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'endpoints': {
            'articles': {
                'method': 'GET',
                'path': '/articles',
                'pagination': page_number_pagination(page_size=20, data_path='articles'),
            }
        },
    }
)


# example 19: link_header_pagination — RFC 5988 Link header (e.g. GitHub API)
#
# the API returns a Link header with rel="next" pointing to the next page URL.
# the strategy replaces the full URL each page, so no params accumulate.
# response body can be a list directly or a dict-like object with a data_path.

client = APIClient(
    {
        'base_url': 'https://api.github.com',
        'auth': {'type': 'bearer', 'token': 'github-pat'},
        'headers': {'Accept': 'application/vnd.github+json'},
        'endpoints': {
            'list_repos': {
                'method': 'GET',
                'path': '/orgs/acme/repos',
                'params': {'per_page': 100},
                'playback': {
                    'path': 'examples/fixtures/example_19_link_header_repos.json',
                    'mode': 'none',  # change to 'load' to try offline replay
                },
                # GitHub commonly returns pagination state in the Link header rather than the JSON body.
                # This example intentionally uses data_path='items' to show the helper API; if the
                # response body is a bare JSON array, on_response simply returns the whole parsed page.
                'pagination': link_header_pagination(data_path='items'),
            }
        },
    }
)

all_repos = client.fetch('list_repos')


# example 20: oauth2 client credentials auth
#
# the library fetches a token from token_url on first use, caches it,
# and refreshes automatically when it expires (with expiry_margin seconds buffer).
# no manual token management needed.

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {
            'type': 'oauth2',
            'token_url': 'https://auth.example.com/oauth/token',
            'client_id': 'my-client-id',
            'client_secret': 'my-client-secret',
            'scope': 'read:data write:data',  # optional
            'expiry_margin': 120,  # refresh 2 min before expiry (default 60)
        },
        'endpoints': {'records': {'method': 'GET', 'path': '/records'}},
    }
)

records = client.fetch('records')

# SchemaBuilder equivalent
schema = (
    SchemaBuilder('https://api.example.com/v1')
    .oauth2(
        token_url='https://auth.example.com/oauth/token',
        client_id='my-client-id',
        client_secret='my-client-secret',
        scope='read:data',
        expiry_margin=120,
    )
    .endpoint('records', method='GET', path='/records')
    .build()
)


# example 20a: oauth2 password grant auth (e.g. GLPI v2)
#
# some APIs need both client credentials and a real user identity.
# GLPI v2 is a common example: token endpoint at /api.php/token, API base
# under /api.php/v2, and scope='api' for normal REST access.
# endpoint paths and response shapes vary by instance/version; check your
# instance's /api.php/doc when it is enabled instead of trusting any example too much.

client = APIClient(
    {
        'base_url': 'https://glpi.example.com/api.php/v2',
        'auth': {
            'type': 'oauth2_password',
            'token_url': 'https://glpi.example.com/api.php/token',
            'client_id': 'my-client-id',
            'client_secret': 'my-client-secret',
            'username': 'glpi-api-user',
            'password': 'correct horse battery staple',
            'scope': 'api',
        },
        'endpoints': {
            # single-item endpoint: concrete and less hand-wavy than pretending all
            # collection endpoints paginate the same way everywhere
            'ticket': {'method': 'GET', 'path': '/Ticket/{ticket_id}'},
        },
    }
)

ticket = client.fetch('ticket', path_params={'ticket_id': 123})

# SchemaBuilder equivalent
schema = (
    SchemaBuilder('https://glpi.example.com/api.php/v2')
    .oauth2_password(
        token_url='https://glpi.example.com/api.php/token',
        client_id='my-client-id',
        client_secret='my-client-secret',
        username='glpi-api-user',
        password='correct horse battery staple',
        scope='api',
    )
    .endpoint('ticket', method='GET', path='/Ticket/{ticket_id}')
    .build()
)

# example 21: bearer token_callback — dynamic / rotating tokens
#
# token_callback is called on every request so it always returns the current token.
# it receives a read-only config view, not mutable shared state.
# useful when the token is managed externally (vault, SSM, in-memory cache with refresh).

import os


def get_token_from_env(config):
    "reads the token from environment at call time — picks up rotations without restart"
    return os.environ['API_TOKEN']


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token_callback': get_token_from_env},
        'endpoints': {'data': {'method': 'GET', 'path': '/data'}},
    }
)


# combining token_callback with state — inject the token name or environment at build time
# through read-only config, not a mutable shared dict
def get_token_from_state(config):
    import boto3

    return boto3.client('ssm').get_parameter(Name=config['ssm_param_name'], WithDecryption=True)[
        'Parameter'
    ]['Value']


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'state': {'ssm_param_name': '/prod/api/token'},
        'auth': {'type': 'bearer', 'token_callback': get_token_from_state},
        'endpoints': {'data': {'method': 'GET', 'path': '/data'}},
    }
)

# SchemaBuilder equivalent
schema = (
    SchemaBuilder('https://api.example.com/v1')
    .bearer(get_token_from_env)  # callable detected automatically
    .endpoint('data', method='GET', path='/data')
    .build()
)


# example 22: on_page callback — per-page side effects, streaming vs buffering modes
#
# three patterns for processing large paginated results, in order of preference
# when memory is a concern:
#
# PATTERN A — stream() loop: purest non-accumulating form.
#   the library holds at most one page in memory at any time.
#   nothing is buffered. fetch and process interleave naturally.
#   use this when you don't need a final transform across all pages.

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {
            'events': {
                'method': 'GET',
                'path': '/events',
                'pagination': offset_pagination(data_path='items', limit=500),
                'on_response': lambda resp, state: resp.get('items', []),
            }
        },
    }
)

total = 0
for page in client.stream('events'):
    db.bulk_insert('events', page)
    total += len(page)
# db populated page-by-page, never more than one page in memory


# PATTERN B — on_page callback: side-effect hook inside the pagination loop.
#   fires once per page immediately after on_response, before next_request.
#   useful when the side-effect logic belongs with the schema definition
#   rather than at the call site (e.g. shared client used in multiple places).
#   like stream(), the library does NOT buffer pages when on_complete is absent.


def write_page_to_db(page_data, state):
    db.bulk_insert('events', page_data)
    print(f'inserted {len(page_data)} rows, offset={state.get("offset", "?")}')


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {
            'events': {
                'method': 'GET',
                'path': '/events',
                'pagination': offset_pagination(data_path='items', limit=500),
                'on_response': lambda resp, state: resp.get('items', []),
                'on_page': write_page_to_db,
                # no on_complete here — pages are NOT buffered, one page in memory at a time
            }
        },
    }
)

client.fetch('events')  # return value is a list of raw pages; discard it if on_page does the work
# or: list(client.stream('events')) — same effect, on_page fires identically


# PATTERN C — fetch() + on_page + on_complete: side effects per page AND a final transform.
#   WARNING: this is fetch()-specific buffering. fetch() with on_complete materializes
#   all page values before calling on_complete. stream() no longer does that; its
#   on_complete callback receives StreamSummary instead of the full page list.
#   use this pattern when you truly need a final cross-page aggregation
#   (flatten, deduplicate, sort, etc.).

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {
            'events': {
                'method': 'GET',
                'path': '/events',
                'pagination': offset_pagination(data_path='items', limit=500),
                'on_response': lambda resp, state: resp.get('items', []),
                'on_page': write_page_to_db,  # fires per page as each arrives
                'on_complete': lambda pages,
                state: {  # fetch() materializes all pages before completion
                    'total_pages': len(pages),
                    'total_rows': sum(len(p) for p in pages),
                },
            }
        },
    }
)

summary = client.fetch('events')
# summary == {'total_pages': N, 'total_rows': M}
# db already populated page-by-page via on_page
# NOTE: this buffering happens for fetch() here. stream() with on_complete uses StreamSummary instead.


# example 22b: on_page progress logging using requests_so_far
#
# on_page already receives callback-visible run-local counters, so you can
# log progress without changing library code. requests_so_far is a request
# counter exposed through state. because this hook runs on page completion,
# treat this as convenient progress reporting rather than a strict
# every-Nth-request trigger.


def log_progress_every_10_requests(page_data, state):
    requests_so_far = state.get('requests_so_far', 0)
    if requests_so_far and requests_so_far % 10 == 0:
        print(f'progress: {requests_so_far} requests completed')
    # on_page return value is ignored — omit or return None


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {
            'events': {
                'method': 'GET',
                'path': '/events',
                'pagination': offset_pagination(data_path='items', limit=500),
                'on_response': lambda resp, state: resp.get('items', []),
                'on_page': log_progress_every_10_requests,
            }
        },
    }
)

for page in client.stream('events'):
    save_to_warehouse(page)


# example 23: update_state — explicit state mutations across pagination pages
#
# on_response and next_request receive a merged read-only view of state.
# update_state is the mechanism to persist values into page_state across pages.
# callbacks receive a StateView (read-only snapshot) — direct mutation raises immediately.
# return a dict from update_state; it is merged into page_state before the next iteration.
# useful when the pagination counter is embedded inside the response body,
# not just computed from len(items).


def extract_state(page_payload, state):
    "return a dict — merged into page_state, then next_request sees the updated values"
    return {
        'server_offset': page_payload.get('next_offset'),
        'total': page_payload.get('meta', {}).get('total'),
    }


def next_request_with_state(resp, state):
    server_offset = state.get('server_offset')
    total = state.get('total')
    if server_offset is None or (total is not None and server_offset >= total):
        return None
    return {'params': {'offset': server_offset}}


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'endpoints': {
            'stream': {
                'method': 'GET',
                'path': '/stream',
                'pagination': {
                    'next_request': next_request_with_state,
                },
                'update_state': extract_state,
                'on_response': lambda resp, state: resp.get('items', []),
                'on_complete': lambda pages, state: [r for page in pages for r in page],
            }
        },
    }
)


# example 24: initial_params in custom pagination
#
# initial_params are injected into the first request's params before any
# call-time overrides. useful when a custom pagination strategy needs to
# set a starting page/offset that the caller should not have to supply.
# call-time params override initial_params (same merge order as schema defaults).
#
# note: page_state is seeded from initial_params once at the start of the run.
# callbacks receive a StateView — read-only snapshot, direct mutations raise.
# use update_state to advance counters across pages (see also example 24a for the
# _request alternative that reads the counter directly from what was sent).


def paged_update_state(page_payload, state):
    "advance page counter — runs before next_request each iteration"
    return {'page': state.get('page', 1) + 1}


def paged_next_request(parsed_body, state):
    # state['page'] is reliably current because update_state ran first
    current = state.get('page', 1)
    if current > parsed_body.get('total_pages', 1):
        return None
    return {'params': {'page': current}}


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'endpoints': {
            'reports': {
                'method': 'GET',
                'path': '/reports',
                'pagination': {
                    'next_request': paged_next_request,
                    'initial_params': {
                        'page': 1,
                        'per_page': 50,
                    },  # sent on first request automatically
                },
                'update_state': paged_update_state,
                'on_response': lambda resp, state: resp.get('reports', []),
            }
        },
    }
)

# override initial page size at call time — merges over initial_params
reports = client.fetch('reports', params={'per_page': 100})


# example 24a: _request in state — reading current params without update_state
#
# every pagination callback receives state['_request'] — a snapshot of the exact
# request dict that produced the current response. this includes method, url, params,
# headers, and json/data (post-auth, so any injected tokens are visible too).
#
# the key use case: reading a pagination counter (page, offset) from the request
# that was sent, rather than tracking it in page_state via update_state.
#
# trade-offs vs update_state:
#   + no update_state callback needed — counter is always exactly what was sent
#   + no risk of off-by-one between what we sent and what state thinks we sent
#   - only works for counters driven by request params (not response-body counters)
#   - auth-injected headers visible in _request — note when logging state
#
# this is additive: update_state is still the right tool for response-body counters,
# accumulated totals, or anything not in the request params.
#
# shorthand for safe access to _request params:
#
#   req_params = (state.get('_request') or {}).get('params') or {}
#   current_page = req_params.get('page', 1)


def page_from_request(parsed_body, state):
    "reads current page from _request.params — no mutation, no update_state needed"
    req_params = (state.get('_request') or {}).get('params') or {}
    current_page = req_params.get('page', 1)
    total_pages = parsed_body.get('total_pages', 1)
    if current_page >= total_pages:
        return None
    return {'params': {'page': current_page + 1}}


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'endpoints': {
            'reports': {
                'method': 'GET',
                'path': '/reports',
                'pagination': {
                    'next_request': page_from_request,  # reads page from _request, no state writes
                    'initial_params': {'page': 1, 'per_page': 50},
                },
                'on_response': lambda parsed_body, state: parsed_body.get('reports', []),
            }
        },
    }
)

# the _request approach works identically when starting from a non-1 page
reports = client.fetch(
    'reports', params={'per_page': 100}
)  # initial_params overridden at call time

# contrast: this is what example 24 does with update_state instead:
#
#   def paged_update_state(page_payload, state):
#       return {'page': state.get('page', 1) + 1}    # advance counter explicitly
#
#   def paged_next_request(parsed_body, state):
#       current = state.get('page', 1)               # reads from page_state set by update_state
#       ...
#
# _request is simpler when the counter is just the page param you already sent.
# update_state is necessary when tracking something from the response body (e.g. total rows seen).

# example 25: per-endpoint log_level and debug flag
#
# log_level controls verbosity for a specific endpoint; useful for
# suppressing noise from high-frequency polling endpoints while keeping
# medium or verbose logging for critical ones.
# debug=True additionally logs the raw request and first 500 chars of the response body.

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'log_level': 'medium',  # client-level default
        'endpoints': {
            'heartbeat': {
                'method': 'GET',
                'path': '/ping',
                'log_level': 'none',  # suppress logs for polling endpoints
            },
            'orders': {
                'method': 'GET',
                'path': '/orders',
                # inherits client-level 'medium'
            },
            'auth_exchange': {
                'method': 'POST',
                'path': '/auth/token',
                'log_level': 'verbose',  # full request/response for troubleshooting
                'debug': True,  # also dumps raw body to DEBUG log
            },
        },
    }
)


# example 26: playback — record once, replay for dev/test
#
# playback records real API responses to a JSON file and replays from it.
# four modes:
#   auto  — replay if file exists, otherwise fetch and save (default dev workflow)
#   save  — always fetch and overwrite (force a re-record)
#   load  — always replay; raise PlaybackError if file is missing
#   none  — disable record/replay entirely (treat as if playback were omitted)
#
# string shorthand: 'path/to/file.json' is equivalent to {'path': '...', 'mode': 'auto'}.
# dict form sets mode explicitly.

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {
            # dict form: fixed replay file checked into the repo for local dev / docs examples
            'users': {
                'method': 'GET',
                'path': '/users',
                'playback': {
                    'path': 'examples/fixtures/example_26_playback_users.json',
                    'mode': 'load',
                },
            },
            # dict form: force a fresh recording (e.g. weekly fixture refresh)
            'orders': {
                'method': 'GET',
                'path': '/orders',
                'playback': {'path': 'fixtures/orders.json', 'mode': 'save'},
            },
            # dict form: CI mode — never hit the real API
            'products': {
                'method': 'GET',
                'path': '/products',
                'playback': {'path': 'fixtures/products.json', 'mode': 'load'},
            },
            # dict form: temporarily disable playback without deleting the config block
            'reports_live': {
                'method': 'GET',
                'path': '/reports',
                'playback': {'mode': 'none'},
            },
        },
    }
)

users = client.fetch('users')  # always loads the checked-in fixture; raises if missing
orders = client.fetch('orders')  # always fetches real API and overwrites fixture
products = client.fetch('products')  # always loads from file; raises if missing

# safety caps (max_pages, max_requests, time_limit) are NOT applied during replay.
# In playback load mode, raw fixtures are replayed through the normal parsing,
# callback, and pagination pipeline so you can debug callback logic locally
# without hitting the network.
#
# Runtime caps are intentionally bypassed during playback load:
# playback is for deterministic local replay/debugging, not operational
# throttling or long-running live-call protection.
#
# Caps apply normally in save mode and in auto mode when the fixture is absent
# (both paths make real HTTP requests and go through the live execution stack).
#
# To test cap behaviour without a live API, use a callable mock= instead:
# mock goes through the full pagination/retry stack so caps fire correctly.
# see example 30 for the callable mock pattern.


# example 26a: playback record_as_bytes — store any response as raw bytes
#
# Normally, textual response_format fixtures are stored as readable 'body'.
# If you need exact bytes (weird encodings, byte-for-byte diffs, etc.), set:
#   playback: {'record_as_bytes': True, ...}
#
# You can also load a pre-recorded textual-format fixture stored via content_b64.
# The logical format stays 'json' (or 'xml' / 'csv' / 'text') and parsing still
# starts from response.content bytes.

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'endpoints': {
            'events_raw_bytes': {
                'method': 'GET',
                'path': '/events',
                'response_format': 'json',
                'playback': {
                    'path': 'examples/fixtures/example_40_json_as_bytes.json',
                    'mode': 'load',
                },
            },
        },
    }
)

events = client.fetch('events_raw_bytes')


# example 27: client-level response_parser + endpoint override
# `parsed` in a 2-arg parser is the canonical parsed body for the configured
# response_format — not just JSON-or-None.
#
# response_parser at client level applies to all endpoints that don't override it.
# endpoint-level response_parser takes full precedence over the client default.
# both 1-arg and 2-arg response_parser forms are supported.
# In the 2-arg form, the second argument is the canonical parsed body for the
# configured/inferred response_format, not just pre-parsed JSON.

# client-level: unwrap a common 'data' envelope across all endpoints
client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'response_parser': lambda parsed_body, parsed: parsed.get('data', parsed),  # 2-arg
        'endpoints': {
            'users': {'method': 'GET', 'path': '/users'},  # gets parsed['data']
            'metrics': {'method': 'GET', 'path': '/metrics'},  # gets parsed['data']
            # endpoint override: metrics_raw bypasses the client-level unwrap
            'metrics_raw': {
                'method': 'GET',
                'path': '/metrics',
                'response_parser': lambda parsed_body, parsed: parsed,  # return full response
            },
        },
    }
)


# example 28: SchemaBuilder — client-level pagination and rate_limit
#
# .pagination() sets client-level pagination defaults inherited by all endpoints.
# .rate_limit() on SchemaBuilder configures reactive rate-limit settings
# such as respect_retry_after and min_delay. For token-bucket limiting,
# write the full rate_limit dict by hand.
# individual endpoints can override or disable inherited pagination (see example 3).


def shared_next_request(parsed_body, state):
    next_cursor = parsed_body.get('cursor')
    return {'params': {'cursor': next_cursor}} if next_cursor else None


schema = (
    SchemaBuilder('https://api.example.com/v1')
    .bearer('my-token')
    .timeout(30)
    .retry(max_attempts=3, backoff='exponential')
    .rate_limit(min_delay=0.2, respect_retry_after=True)  # 200 ms floor between requests
    .pagination(
        {  # inherited by all endpoints — mechanics only (next_request, delay)
            'next_request': shared_next_request,
            'delay': 0.1,
        }
    )
    # on_response is an endpoint-level lifecycle hook — not a pagination key
    .endpoint(
        'users', method='GET', path='/users', on_response=lambda resp, state: resp.get('items', [])
    )
    .endpoint(
        'events',
        method='GET',
        path='/events',
        on_response=lambda resp, state: resp.get('items', []),
    )
    .endpoint(
        'webhook', method='POST', path='/webhooks', pagination=None
    )  # disable for this endpoint
    .build()
)

client = APIClient(schema)


# example 29: client-level on_error — shared error handling across all endpoints
#
# on_error at client level applies to all endpoints that don't define their own.
# endpoint on_error always wins over client on_error.
# return 'skip' to swallow the error and return {}, or 'raise' to re-raise.

import logging as _logging

_log = _logging.getLogger(__name__)


def global_on_error(exc, state):
    "log and skip 404s globally; raise everything else"
    if exc.status_code == 404:
        _log.warning('resource not found: %s %s', exc.method, exc.url)
        return 'skip'
    return 'raise'


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'on_error': global_on_error,  # applies to all endpoints
        'endpoints': {
            'users': {'method': 'GET', 'path': '/users'},
            'orders': {'method': 'GET', 'path': '/orders'},
            'fragile': {
                'method': 'GET',
                'path': '/fragile',
                # endpoint on_error overrides global: skip 404 AND 503
                'on_error': lambda exc, state: (
                    'skip' if exc.status_code in (404, 503) else 'raise'
                ),
            },
        },
    }
)


# example 29a: on_error stop-after-error-threshold using errors_so_far
#
# on_error receives callback-visible run-local counters, so you can stop
# cleanly after too many failures while keeping pages already fetched.
# this is a practical stop-after-error-threshold pattern, not a full
# circuit breaker subsystem.


def stop_after_three_errors(exc, state):
    errors_so_far = state.get('errors_so_far', 0)
    pages_so_far = state.get('pages_so_far', 0)

    if errors_so_far >= 3:
        print(f'stopping after {errors_so_far} errors with {pages_so_far} pages already collected')
        return 'stop'

    return 'skip'  # on_error must return 'raise', 'skip', or 'stop' — None raises CallbackError


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {
            'fragile_feed': {
                'method': 'GET',
                'path': '/fragile-feed',
                'pagination': offset_pagination(data_path='items', limit=100),
                'on_response': lambda resp, state: resp.get('items', []),
                'on_error': stop_after_three_errors,
            }
        },
    }
)

for page in client.stream('fragile_feed'):
    save_to_warehouse(page)


# example 30: mock with callable — dynamic per-request mock responses
#
# mock can be a callable(request_kwargs, *, run_state) -> response_dict for dynamic mocks.
# useful when the mock response depends on the request params (e.g. pagination,
# filters) rather than a fixed sequence of pages.


def dynamic_mock(request_kwargs, *, run_state):
    page = int(request_kwargs.get('params', {}).get('page', 1))
    if page >= 3:
        return {'items': [], 'total_pages': 3}
    return {
        'items': [{'id': (page - 1) * 2 + i} for i in range(1, 3)],
        'total_pages': 3,
    }


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'endpoints': {
            'paged': {
                'method': 'GET',
                'path': '/items',
                'mock': dynamic_mock,  # called once per page with actual request_kwargs and run_state
                'pagination': page_number_pagination(
                    data_path='items', total_pages_path='total_pages'
                ),
            }
        },
    }
)

all_items = client.fetch('paged')
# all_items == [{'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}]


# example 31: headers — three-layer merge (client / endpoint / call-time)
#
# headers flow through three layers, each merging over the previous:
#   1. client-level 'headers' — applied to every request via requests.Session
#   2. endpoint-level 'headers' — merged over client headers for that endpoint only
#   3. call-time headers= kwarg — merged over endpoint headers for that one call
#
# merge is shallow: a key present at a later layer overwrites the earlier value.
# auth-injected headers (Authorization, X-Api-Key, etc.) are applied after all
# three layers by the auth handler — they always win.
#
# scrubbing applies to logged output only — the actual HTTP request is unaffected.

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        # layer 1: client-level — inherited by every endpoint
        'headers': {
            'Accept': 'application/json',
            'X-Client-Version': '2.1.0',
        },
        'endpoints': {
            'events': {
                'method': 'GET',
                'path': '/events',
                # layer 2: endpoint-level — adds/overrides for this endpoint only
                'headers': {
                    'X-Stream-Format': 'ndjson',  # new key for this endpoint
                    'Accept': 'application/x-ndjson',  # overrides client-level Accept
                },
            },
            'create': {
                'method': 'POST',
                'path': '/orders',
                # no endpoint headers — inherits client-level Accept and X-Client-Version only
            },
        },
    }
)

# effective headers on GET /events:
#   Accept: application/x-ndjson   (endpoint overrides client)
#   X-Client-Version: 2.1.0         (from client)
#   X-Stream-Format: ndjson         (from endpoint, new key)
#   Authorization: Bearer my-token  (injected by auth handler, always last)
events = client.fetch('events')

# layer 3: call-time headers= — adds/overrides for this single call only
# effective: Accept=application/x-ndjson (endpoint), X-Idempotency-Key added for this call only
order = client.fetch(
    'create',
    body={'item': 'ABC'},
    headers={'Idempotency-Key': 'idem-20260101-xyz'},
)


# example 32: timeout — scalar, tuple (connect, read), per-endpoint override
#
# requests has NO default timeout — without an explicit value it hangs indefinitely.
# always set a timeout. the library uses 30s as fallback if you don't, but set it
# explicitly so the intent is clear.
#
# scalar:        applies to both connect and read phases.
# tuple (c, r):  separate limits — connect phase and read phase respectively.
#                use tuple form when the API is fast to connect but slow to stream
#                large responses (or vice versa).
# per-endpoint:  overrides the client-level timeout for that endpoint only.

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'timeout': 30,  # scalar: 30s for both connect and read (client default)
        'endpoints': {
            'ping': {
                'method': 'GET',
                'path': '/ping',
                'timeout': 5,  # fast endpoint — fail quickly if it doesn't respond
            },
            'export': {
                'method': 'POST',
                'path': '/export',
                # tuple: (connect_timeout, read_timeout)
                # connect must succeed in 5s, but the export body can take up to 5 min
                'timeout': (5, 300),
            },
            'search': {
                'method': 'GET',
                'path': '/search',
                # inherits client-level 30s scalar
            },
        },
    }
)

# SchemaBuilder equivalent — .timeout() accepts both forms
schema = (
    SchemaBuilder('https://api.example.com/v1')
    .bearer('my-token')
    .timeout((5, 60))  # tuple form: 5s connect, 60s read
    .endpoint('ping', method='GET', path='/ping', timeout=3)
    .endpoint('export', method='POST', path='/export', timeout=(5, 300))
    .endpoint('search', method='GET', path='/search')  # inherits client (5, 60)
    .build()
)


# example 33: retry.backoff as callable — custom delay curve + base_delay / max_delay
#
# backoff accepts three forms:
#   'exponential' — 1s, 2s, 4s, 8s, ... (default, base_delay * 2^(attempt-1))
#   'linear'      — 1s, 2s, 3s, 4s, ... (base_delay * attempt)
#   callable      — fn(attempt: int) -> float  (attempt is 1-indexed retry count)
#
# base_delay (default 1.0) and max_delay (default 120.0) tune the built-in strategies.
# a custom callable bypasses base_delay/max_delay — implement your own cap if needed.
# all delays are in seconds.

import random

# fixed 2-second delay regardless of attempt number
client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'retry': {
            'max_attempts': 4,
            'backoff': lambda attempt: 2.0,  # always wait 2 seconds
            'on_codes': [429, 500, 503],
        },
        'endpoints': {'data': {'method': 'GET', 'path': '/data'}},
    }
)


# jittered exponential: reduces thundering-herd when many clients retry simultaneously
def jittered_backoff(attempt):
    base = min(2 ** (attempt - 1), 30)  # cap at 30s before jitter
    return base + random.uniform(0, 1)  # add up to 1s of random jitter


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'retry': {
            'max_attempts': 5,
            'backoff': jittered_backoff,
            'on_codes': [429, 500, 502, 503, 504],
        },
        'endpoints': {'data': {'method': 'GET', 'path': '/data'}},
    }
)

# tuning built-in exponential with base_delay and max_delay
client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'retry': {
            'max_attempts': 6,
            'backoff': 'exponential',
            'on_codes': [429, 500, 503],
            'base_delay': 0.5,  # start at 0.5s instead of 1s: 0.5, 1, 2, 4, 8, 16
            'max_delay': 30.0,  # cap at 30s instead of default 120s
        },
        'endpoints': {'data': {'method': 'GET', 'path': '/data'}},
    }
)


# example 34: scrub_headers / scrub_query_params — request-side scrubbing in logs and playback fixtures
#
# the library redacts sensitive header values in logs at two levels:
#
#   default exact-match set (always scrubbed, case-insensitive):
#       Authorization, X-Api-Key, X-Api-Secret, X-Auth-Token,
#       Api-Key, Proxy-Authorization, Cookie, Set-Cookie
#
#   substring patterns (any header whose name contains one of these is scrubbed):
#       'token', 'secret', 'password', 'key', 'auth'
#       e.g. X-Access-Token, X-Refresh-Token, X-Webhook-Secret all auto-scrubbed
#
#   schema scrub_headers list: additional headers for exact-match (case-insensitive).
#       use this for custom header names that don't match the built-in patterns above.
#
# scrubbing is save-time only — live HTTP behavior is unaffected.
# request headers are scrubbed in logs and raw playback fixtures; recorded request
# URLs also have sensitive query parameter values redacted in playback fixtures.
# response headers, response bodies, and request bodies are intentionally
# not scrubbed in v1.
# If your API uses a generic sensitive query param like `code`, add it explicitly:
#   .scrub_query_params('code')

# schema dict form — list of header names to add to the exact-match set
client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'scrub_headers': [
            'X-Tenant-Id',  # business identifier — redact from logs / fixtures
            'X-Request-Trace',  # could contain internal routing data
        ],
        'scrub_query_params': [
            'tenant_token',  # API-specific query param to redact in recorded URLs
        ],
        'endpoints': {'data': {'method': 'GET', 'path': '/data'}},
    }
)

# headers that are scrubbed automatically (no scrub_headers needed):
#   Authorization: Bearer ...    <- exact match
#   X-Api-Key: ...               <- exact match
#   X-Access-Token: ...          <- contains 'token' (pattern)
#   X-Refresh-Token: ...         <- contains 'token' (pattern)
#   X-Webhook-Secret: ...        <- contains 'secret' (pattern)
#   X-Client-Password: ...       <- contains 'password' (pattern)
#   X-Signing-Key: ...           <- contains 'key' (pattern)
#
# headers that need explicit scrub_headers (no pattern match):
#   X-Tenant-Id: ...             <- 'tenant' doesn't match any pattern
#   X-Correlation-Id: ...        <- same
#   X-Internal-User: ...         <- same

# SchemaBuilder form — same list, passed as positional args
schema = (
    SchemaBuilder('https://api.example.com/v1')
    .bearer('my-token')
    .scrub_headers('X-Tenant-Id', 'X-Request-Trace')  # varargs, not a list
    .scrub_query_params('tenant_token')
    .endpoint('data', method='GET', path='/data')
    .build()
)


# example 35: response_format='text' — plain text endpoints

client = APIClient(
    {
        'base_url': 'https://example.com',
        'endpoints': {
            'robots': {
                'method': 'GET',
                'path': '/robots.txt',
                'response_format': 'text',
                'playback': {
                    'path': 'examples/fixtures/example_35_text_status.json',
                    'mode': 'none',  # change to 'load' to try offline replay
                },
            }
        },
    }
)

robots_txt = client.fetch('robots')
# returns a str


# example 36: response_format='xml' — XML endpoints

client = APIClient(
    {
        'base_url': 'https://example.com',
        'endpoints': {
            'feed': {
                'method': 'GET',
                'path': '/feed.xml',
                'response_format': 'xml',
                'playback': {
                    'path': 'examples/fixtures/example_36_xml_feed.json',
                    'mode': 'none',  # change to 'load' to try offline replay
                },
            }
        },
    }
)

xml_root = client.fetch('feed')
# returns xml.etree.ElementTree.Element
first_title = xml_root.findtext('.//title')


# example 37: response_format='csv' — CSV endpoints

client = APIClient(
    {
        'base_url': 'https://example.com',
        'endpoints': {
            'report': {
                'method': 'GET',
                'path': '/report.csv',
                'response_format': 'csv',
                # The default csv_delimiter is ';'.
                # Set csv_delimiter=',' (or another single character) when the API uses a different separator.
                # Set encoding='cp1251' (or any codec name) when the response is not UTF-8.
                'playback': {
                    'path': 'examples/fixtures/example_37_csv_report.json',
                    'mode': 'none',  # change to 'load' to try offline replay
                },
            }
        },
    }
)

rows = client.fetch('report')
# returns list[dict[str, str]]


# example 38: nested-path pagination helpers

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'endpoints': {
            'nested_users': {
                'method': 'GET',
                'path': '/users',
                'playback': {
                    'path': 'examples/fixtures/example_38_nested_pagination.json',
                    'mode': 'none',  # change to 'load' to try offline replay
                },
                'pagination': offset_pagination(
                    limit=100,
                    data_path='payload.items',
                    total_path='meta.total',
                ),
            }
        },
    }
)


# example 39: raw playback for callback debugging
#
# Purpose:
#   record real API responses once, then iterate locally on callback logic
#   without burning API quota or re-hitting a commercial endpoint.
#
# On the first run, mode='save' writes a raw playback fixture.
# On later runs, mode='load' replays the recorded responses through:
#   - default parsing
#   - response_parser
#   - on_response
#   - on_page
#   - update_state / next_request
#   - on_complete
#
# This lets you change callback logic locally and re-run against the same
# recorded wire data.


def extract_names(resp, state):
    return [item['name'] for item in resp.get('items', [])]


def debug_update_state(page_payload, state):
    return {'offset': state.get('offset', 0) + len(page_payload)}


def debug_next_request(parsed_body, state):
    current_offset = state.get('offset', 0)
    total = ((parsed_body.get('meta') or {}).get('total')) or 0
    if current_offset >= total:
        return None
    return {'params': {'offset': current_offset, 'limit': 100}}


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {
            'users_debug': {
                'method': 'GET',
                'path': '/users',
                'pagination': {
                    'initial_params': {'offset': 0, 'limit': 100},
                    'next_request': debug_next_request,
                },
                'on_response': extract_names,
                'update_state': debug_update_state,
                'playback': {
                    'path': 'examples/fixtures/example_39_callback_debugging.json',
                    'mode': 'load',  # change to 'save' to re-record against a live API
                },
                'scrub_query_params': ['tenant_token'],
            }
        },
    }
)

debug_names = client.fetch('users_debug')

# workflow:
#   1. run once with mode='save' against the real API
#   2. switch mode='load'
#   3. change extract_names / pagination callbacks locally
#   4. rerun without hitting the network


# example 40: bounded stop behaviour — max_pages / max_requests preserve fetched pages


def on_complete_with_stop(pages, state):
    stop = state.get('stop')
    if stop:
        print(f'stopped by {stop["kind"]} at {stop["observed"]} / {stop["limit"]}')
    else:
        print('completed naturally')
    return pages


client = APIClient(
    {
        'base_url': 'https://api.example.com',
        'endpoints': {
            'items': {
                'path': '/items',
                'pagination': cursor_pagination(
                    cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                ),
                'on_complete': on_complete_with_stop,
            }
        },
    }
)

# returns up to 3 pages normally; if the cap is hit, state['stop'] explains why
pages = client.fetch('items', max_pages=3)


# example 41: stream() completion summary — on_complete receives StreamSummary, not page list
# the same on_complete key is reused, but the first argument is StreamSummary here,
# not the full page list that fetch() would pass.


def on_stream_complete(summary, state):
    if summary.stop is not None:
        print(
            f'stream stopped by {summary.stop.kind} at {summary.stop.observed} / {summary.stop.limit}'
        )
    else:
        print(
            f'stream completed naturally after {summary.pages} pages and {summary.requests} requests'
        )


client = APIClient(
    {
        'base_url': 'https://api.example.com',
        'endpoints': {
            'items': {
                'path': '/items',
                'pagination': cursor_pagination(
                    cursor_param='cursor', next_cursor_path='next_cursor', data_path='items'
                ),
                'on_complete': on_stream_complete,
            }
        },
    }
)

for page in client.stream('items', max_pages=3):
    handle_page(page)

# example 42: on_event + token bucket + playback source semantics
#
# on_event is the lifecycle telemetry hook. It is separate from data callbacks
# such as on_response / on_page / update_state.
#
# token-bucket rate limiting uses:
#   - requests_per_second for sustained rate
#   - burst for short spikes when capacity has accumulated
#
# playback re-emits lifecycle events with source='playback', but proactive
# rate limiting is bypassed during playback, so wait events are a live-run concern.

from rest_fetcher import RateLimitExceeded

telemetry = []


def capture_event(ev):
    telemetry.append((ev.kind, ev.source, ev.data))


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'on_event': capture_event,
        'rate_limit': {
            'strategy': 'token_bucket',
            'requests_per_second': 2.0,  # sustained refill rate
            'burst': 5,  # allow short spikes up to 5 immediate requests
            'on_limit': 'wait',
        },
        'endpoints': {
            'users': {
                'method': 'GET',
                'path': '/users',
                'response_format': 'json',
                'playback': {
                    'path': 'examples/fixtures/example_41_events_rate_limit.json',
                    'mode': 'load',
                },
            },
        },
    }
)

run = client.stream_run('users', max_pages=3)
for page in run:
    save_to_warehouse(page)

summary = run.summary
if summary and summary.stop:
    print(summary.stop.kind, summary.stop.observed, summary.stop.limit)

live_events = [item for item in telemetry if item[1] == 'live']
playback_events = [item for item in telemetry if item[1] == 'playback']


# example 43: rate_limit on_limit='raise' + RateLimitExceeded
#
# If the bucket is empty and on_limit='raise', the client emits a rate-limit
# event and then raises RateLimitExceeded instead of sleeping.

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'rate_limit': {
            'strategy': 'token_bucket',
            'requests_per_second': 1.0,
            'burst': 1,
            'on_limit': 'raise',
        },
        'endpoints': {
            'users': {'method': 'GET', 'path': '/users'},
        },
    }
)

try:
    client.fetch('users')
except RateLimitExceeded as exc:
    print(exc)


# example 44: on_request — pre-auth request shaping from run-local state
#
# on_request sees the assembled request kwargs and read-only run-local state.
# Use it for request shaping that is not auth: correlation ids, late params,
# or body/header tweaks based on call-time state.


def add_correlation_id(request_kwargs, state):
    request_kwargs = dict(request_kwargs)
    headers = dict(request_kwargs.get('headers', {}))
    headers['X-Correlation-ID'] = state['run_id']
    request_kwargs['headers'] = headers
    return request_kwargs


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'state': {'run_id': 'job-2026-03-11-01'},
        'on_request': add_correlation_id,
        'endpoints': {
            'users': {'method': 'GET', 'path': '/users'},
        },
    }
)

client.fetch('users')


# example 45: files + form — multipart upload
#
# files may coexist with form data. body/json and files are mutually exclusive.

with open('report.csv', 'rb') as fh:
    client = APIClient(
        {
            'base_url': 'https://api.example.com/v1',
            'endpoints': {
                'upload_report': {
                    'method': 'POST',
                    'path': '/reports/upload',
                    'form': {'kind': 'daily'},
                    'files': {'file': ('report.csv', fh, 'text/csv')},
                },
            },
        }
    )
    client.fetch('upload_report')


# example 46: session_config — requests.Session transport settings
#
# session_config is client-level transport configuration applied once when the
# APIClient session is created.

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'session_config': {
            'verify': '/etc/ssl/certs/internal-ca.pem',
            'max_redirects': 5,
            'proxies': {'https': 'http://proxy.internal:8080'},
        },
        'endpoints': {
            'users': {'method': 'GET', 'path': '/users'},
        },
    }
)


# example 47: stream_run summary — endpoint / source / retries / elapsed
#
# stream_run() keeps streaming incremental pages, and after exhaustion exposes a
# richer StreamSummary object.

run = client.stream_run('users')
for page in run:
    handle_page(page)

summary = run.summary
if summary is not None:
    print(
        summary.endpoint, summary.source, summary.requests, summary.retries, summary.elapsed_seconds
    )
    if summary.stop is not None:
        print(summary.stop.kind, summary.stop.limit, summary.stop.observed)


# example 48: on_page_complete — adaptive cooldown after repeated errors
#
# on_page_complete fires after every complete page cycle (after on_error resolves,
# before next_request is called). Return seconds to sleep, None, or 0 for no delay.
# Does not fire when on_error returns 'raise'.


def adaptive_cooldown(outcome: PageCycleOutcome, state: dict) -> float | None:
    # back off progressively when errors accumulate
    errors = state.get('errors_so_far', 0)
    if outcome.kind in {'skipped', 'stopped'} and errors >= 3:
        return min(errors * 1.0, 10.0)  # 3s, 4s, … up to 10s
    return None


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'endpoints': {
            'events': {
                'path': '/events',
                'on_error': lambda exc, state: 'skip',
                'pagination': cursor_pagination('cursor', 'meta.next_cursor'),
                'on_page_complete': adaptive_cooldown,
            },
        },
    }
)

client.fetch('events')


# example 49: on_page_complete — light pacing after slow pages
#
# cycle_elapsed_ms is the total wall-clock time from the first attempt start
# to the final response for the page cycle. It includes retries and retry/backoff
# waits, but not later inter-page static/adaptive delays.


def pace_after_slow_page(outcome: PageCycleOutcome, state: dict) -> float | None:
    if (
        outcome.kind == 'success'
        and outcome.cycle_elapsed_ms is not None
        and outcome.cycle_elapsed_ms > 2000
    ):
        return 0.5
    return None


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'endpoints': {
            'reports': {
                'path': '/reports',
                'pagination': cursor_pagination('cursor', 'meta.next_cursor'),
                'on_page_complete': pace_after_slow_page,
            },
        },
    }
)

client.fetch('reports')


# example 50: on_page_complete — observing error_stop outcome
#
# When on_error returns 'stop', the hook fires with outcome.kind='stopped' and
# outcome.stop_signal set.
# The return value is validated but the sleep is skipped since the run is
# already terminating.


def log_stop_reason(outcome: PageCycleOutcome, state: dict) -> float | None:
    if outcome.kind == 'stopped' and outcome.stop_signal is not None:
        print(
            f'run stopped: kind={outcome.stop_signal.kind} '
            f'status={outcome.status_code} '
            f'after {state.get("pages_so_far", 0)} pages'
        )
    return None


client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'endpoints': {
            'jobs': {
                'path': '/jobs',
                'on_error': lambda exc, state: 'stop' if exc.status_code == 503 else 'raise',
                'pagination': cursor_pagination('cursor', 'meta.next_cursor'),
                'on_page_complete': log_stop_reason,
            },
        },
    }
)

client.fetch('jobs')


# example 51: client metrics session — default private session via metrics=True
#
# summary() is side-effect free; reset() atomically returns the pre-reset
# snapshot and clears counters.

client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'metrics': True,
        'endpoints': {
            'users': {
                'path': '/users',
                'mock': [{'items': [1, 2, 3]}],
            },
        },
    }
)

client.fetch('users')
print(client.metrics.summary())
interval_report = client.metrics.reset()
print(interval_report.total_runs, interval_report.total_requests)


# example 52: shared MetricsSession across multiple clients
#
# Inject an explicit MetricsSession when you want several clients to
# contribute to the same top-level totals.

shared_metrics = MetricsSession()

users_client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'metrics': shared_metrics,
        'endpoints': {
            'users': {
                'path': '/users',
                'mock': [{'items': [1, 2, 3]}],
            },
        },
    }
)

orders_client = APIClient(
    {
        'base_url': 'https://api.example.com/v1',
        'metrics': shared_metrics,
        'endpoints': {
            'orders': {
                'path': '/orders',
                'mock': [{'items': [4, 5]}],
            },
        },
    }
)

users_client.fetch('users')
orders_client.fetch('orders')
print(shared_metrics.summary())


# example 53: JWT auth recipe — Airflow-style token acquisition via callback auth
#
# Many APIs (Airflow 3.x, internal services, etc.) use a JWT pattern:
#   1. POST credentials to a token endpoint
#   2. receive a JWT (access_token) with an expiry
#   3. pass it as Authorization: Bearer <token> on subsequent requests
#   4. refresh when expired
#
# rest_fetcher does not have a built-in JWT auth type because JWT acquisition
# flows vary too much across APIs (username/password, API key exchange, client
# credentials, custom headers, nested response shapes). Instead, callback auth
# handles any token acquisition flow cleanly.
#
# This example shows the pattern for Airflow 3.x-style JWT auth. Adapt the
# token endpoint, request body, and response parsing for your API.

import time as _time
import requests as _requests


def make_airflow_jwt_auth(token_url: str, username: str, password: str, *, margin: float = 60.0):
    """
    Build a callback auth handler for Airflow-style JWT authentication.

    The handler acquires a JWT from the token endpoint, caches it, and
    refreshes automatically before expiry. Thread safety follows the same
    contract as built-in OAuth2 handlers: single-threaded is fine, and
    multi-threaded callers should use one APIClient per thread.

    Args:
        token_url: full URL to the token endpoint (e.g. 'https://airflow.example.com/auth/token')
        username: Airflow username
        password: Airflow password
        margin: seconds before expiry to trigger refresh (default 60)

    Returns:
        a callback function suitable for auth.handler in a rest_fetcher schema
    """
    _cached = {'token': None, 'expires_at': 0.0}

    def _refresh_if_needed():
        now = _time.monotonic()
        if _cached['token'] is not None and now < _cached['expires_at'] - margin:
            return
        resp = _requests.post(
            token_url,
            json={
                'username': username,
                'password': password,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        # Airflow returns {"access_token": "...", "token_type": "Bearer"}
        # Some APIs also return "expires_in" (seconds). If absent, default
        # to a conservative 30-minute window.
        _cached['token'] = data['access_token']
        expires_in = data.get('expires_in', 1800)
        _cached['expires_at'] = _time.monotonic() + expires_in

    def handler(request_kwargs, config):
        _refresh_if_needed()
        headers = request_kwargs.get('headers') or {}
        headers['Authorization'] = f'Bearer {_cached["token"]}'
        return {**request_kwargs, 'headers': headers}

    return handler


# Usage with rest_fetcher:
#
# jwt_handler = make_airflow_jwt_auth(
#     token_url='https://airflow.example.com/auth/token',
#     username='my-user',
#     password='my-password',
# )
#
# client = APIClient({
#     'base_url': 'https://airflow.example.com/api/v2',
#     'auth': {'type': 'callback', 'handler': jwt_handler},
#     'timeout': 30,
#     'retry': {'max_attempts': 3, 'on_codes': [429, 500, 502, 503]},
#     'endpoints': {
#         'dags': {
#             'path': '/dags',
#             'pagination': offset_pagination(limit=100, data_path='dags'),
#         },
#         'dag_runs': {
#             'path': '/dags/{dag_id}/dagRuns',
#             'pagination': offset_pagination(limit=100, data_path='dag_runs'),
#         },
#         'task_instances': {
#             'path': '/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances',
#             'pagination': offset_pagination(limit=100, data_path='task_instances'),
#         },
#         'variables': {
#             'path': '/variables',
#             'pagination': offset_pagination(limit=100, data_path='variables'),
#         },
#     },
# })
#
# # fetch all DAGs
# all_dags = client.fetch('dags')
#
# # fetch runs for a specific DAG
# runs = client.fetch('dag_runs', path_params={'dag_id': 'my_etl_pipeline'})
#
# # stream task instances page by page
# for page in client.stream('task_instances',
#                           path_params={'dag_id': 'my_etl_pipeline', 'dag_run_id': 'run_123'},
#                           max_pages=10):
#     process_tasks(page)


# example 54: total_entries-aware pagination for Airflow
#
# Airflow collection endpoints return a total_entries field alongside the data.
# The built-in offset_pagination helper uses "empty page means stop," which
# works but issues one extra request at the end to discover the empty page.
#
# A custom next_request that checks total_entries avoids that final request.
# This pairs with a simple on_response for item extraction.


def airflow_offset_pagination(data_path: str, limit: int = 100):
    """
    Build pagination config for Airflow collection endpoints that return
    total_entries alongside the paginated data.

    Response shape expected:
        {"dags": [...], "total_entries": 250}

    Stops when offset + limit >= total_entries instead of waiting for an
    empty page. One fewer HTTP request per paginated fetch.
    """

    def next_request(parsed_body, state):
        total = parsed_body.get('total_entries')
        if total is None:
            return None
        current_offset = state.get('offset', 0)
        next_offset = current_offset + limit
        if next_offset >= total:
            return None
        return {'params': {'offset': next_offset, 'limit': limit}}

    def on_response(page_payload, state):
        # Walk the data_path to extract items (supports dotted paths like 'data.items')
        result = page_payload
        for key in data_path.split('.'):
            if isinstance(result, dict):
                result = result.get(key, [])
            else:
                return []
        return result

    return {
        'pagination': {
            'next_request': next_request,
            'initial_params': {'offset': 0, 'limit': limit},
        },
        'on_response': on_response,
    }


# Usage — unpack the returned dict into the endpoint config:
#
# pagination_cfg = airflow_offset_pagination(data_path='dags', limit=100)
#
# client = APIClient({
#     'base_url': 'https://airflow.example.com/api/v2',
#     'auth': {'type': 'callback', 'handler': jwt_handler},
#     'timeout': 30,
#     'endpoints': {
#         'dags': {
#             'path': '/dags',
#             **pagination_cfg,
#         },
#     },
# })
#
# all_dags = client.fetch('dags')
# # stops exactly at total_entries — no extra empty-page request


# example 55: fetch_pages() — always-a-list alternative to fetch()
#
# fetch() returns the bare page value for single-page endpoints and a list for
# paginated ones. This means the return type changes depending on how many pages
# the API returns — which can surprise downstream code when a small dataset fits
# on one page.
#
# fetch_pages() is list(stream(...)) materialized. It always returns a list
# regardless of page count, so the shape is predictable no matter the data volume.
#
# on_complete in fetch_pages() follows stream semantics: it fires with
# (StreamSummary, state) at normal completion and its return value is ignored.
# Use fetch() if you need on_complete to transform the returned data.

client = APIClient(
    {  # noqa: F821
        'base_url': 'https://api.example.com/v1',
        'auth': {'type': 'bearer', 'token': 'my-token'},
        'endpoints': {
            'users': {
                'method': 'GET',
                'path': '/users',
                'pagination': offset_pagination(limit=100, data_path='items', total_path='total'),  # noqa: F821
            }
        },
    }
)

# fetch() — shape varies: dict for one page, list[dict] for many
result = client.fetch('users')

# fetch_pages() — always a list, even when the API returns a single page
pages = client.fetch_pages('users')

# same safety caps as fetch() and stream()
pages = client.fetch_pages('users', max_pages=5)
pages = client.fetch_pages('users', params={'active': True}, max_requests=20)

# works the same for non-paginated single-request endpoints
# fetch() returns the dict directly; fetch_pages() wraps it in a list
single = client.fetch_pages('users')  # [{'items': [...], 'total': 42}]


# example 56: PaginationEvent.to_dict() — JSON-serializable event for ETL audit logging
#
# PaginationEvent.to_dict() serializes all event fields except mono (a process-local
# monotonic timestamp with no meaning outside the current run). The key set is stable
# across all event kinds — None fields are included so consumers can rely on a
# consistent structure without guarding for missing keys.
#
# The primary use case is ETL audit logging: write one JSON line per event to a file,
# structured log, or warehouse staging table, then query by kind/endpoint/ts downstream.

import json  # noqa: E402
import logging  # noqa: E402

audit_logger = logging.getLogger('etl.api_audit')


def on_event(ev):
    audit_logger.info(json.dumps(ev.to_dict()))


client = APIClient(
    {  # noqa: F821
        'base_url': 'https://api.example.com/v1',
        'on_event': on_event,
        'endpoints': {
            'reports': {'method': 'GET', 'path': '/reports'},
        },
    }
)

# Each event produces one JSON line, e.g.:
# {"kind": "request_start", "source": "live", "ts": 1718000000.123, "endpoint": "reports",
#  "url": "https://api.example.com/v1/reports", "request_index": null, "attempt": null,
#  "page_index": null, "data": {"method": "GET"}}
#
# {"kind": "request_end", "source": "live", "ts": 1718000000.456, "endpoint": "reports",
#  "url": "https://api.example.com/v1/reports", "request_index": null, "attempt": null,
#  "page_index": null, "data": {"status_code": 200, "elapsed_ms": 312.4, ...}}

# filter to just the events you care about using on_event_kinds:
client_filtered = APIClient(
    {  # noqa: F821
        'base_url': 'https://api.example.com/v1',
        'on_event': on_event,
        'on_event_kinds': {'request_end', 'stopped', 'error'},
        'endpoints': {
            'reports': {'method': 'GET', 'path': '/reports'},
        },
    }
)
