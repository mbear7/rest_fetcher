# rest_fetcher — Schema Guide

How to think about the config dict you pass to `APIClient`.

This guide covers structure, ownership, and layering — why each key lives
where it does. For the full key reference, see `cheatsheet.md`. For
behavioral contracts (firing order, error propagation, timing semantics),
see `semantics.md`.

---

## 1. The three layers

Every schema has three conceptual layers:

**Client config** — shared defaults and cross-cutting behavior that applies
across all endpoints. Things like auth, retry policy, rate limiting, error
handling, telemetry, and optional client-level metrics accumulation. Set once, inherited everywhere unless overridden.

**Endpoint config** — per-endpoint behavior. What data to extract, how to
react to each page, what to do on completion. These hooks depend on what a
specific endpoint returns, so they don't inherit from client level.

**Pagination config** — the mechanics of how to get the next page. Just
three keys: `next_request` (the continuation function), `delay` (static
inter-page spacing), and `initial_params` (starting values for the first
request). Nothing about what to do with the data — that's endpoint config.

A minimal schema touches all three:

```python
from rest_fetcher import APIClient, offset_pagination

client = APIClient({
    # --- client config ---
    'base_url': 'https://api.example.com/v1',
    'auth': {'type': 'bearer', 'token': 'my-token'},
    'timeout': 30,

    'endpoints': {
        'users': {
            # --- endpoint config ---
            'method': 'GET',
            'path': '/users',
            'on_response': lambda resp, state: resp.get('items', []),

            # --- pagination config ---
            'pagination': offset_pagination(limit=100, data_path='items'),
        }
    }
})
```

The pagination helper (`offset_pagination`) provides both `next_request` and
a default `on_response` that extracts items. Since `on_response` is also set
at endpoint level here, the endpoint-level one wins. More on this below.

---

## 2. Inheritance rules

### Cross-cutting hooks inherit from client to endpoint

These keys can be set at client level and are inherited by all endpoints
unless the endpoint explicitly overrides them:

- `on_error` — error disposition policy (`'raise'` / `'skip'` / `'stop'`)
- `on_event` — lifecycle telemetry handler
- `on_event_kinds` — filter which event kinds reach `on_event`
- `on_request` — pre-auth request shaping

These are cross-cutting concerns — error policy, telemetry, request
interception — that naturally default across all endpoints. Setting
`on_error` once at client level means every endpoint gets the same error
handling unless one endpoint needs something different.

```python
{
    'base_url': 'https://api.example.com/v1',
    'on_error': lambda exc, state: 'skip',     # all endpoints inherit this
    'on_event': log_event,                      # all endpoints inherit this
    'on_event_kinds': {'retry', 'stopped'},     # all endpoints inherit this
    'endpoints': {
        'users': {'path': '/users'},            # inherits all three
        'admin': {
            'path': '/admin',
            'on_error': lambda exc, state: 'raise',  # overrides for this endpoint
        },
    }
}
```

### Lifecycle hooks are endpoint-only

These keys live at endpoint level and do **not** inherit from client level:

- `on_response` — extract/transform page data from parsed response
- `on_page` — side-effect per page (log, write to DB, push to queue)
- `on_complete` — final aggregation or summary handling
- `on_page_complete` — post-cycle adaptive throttling
- `update_state` — persist values into run-local state across pages

These hooks are response-shape-sensitive — they depend on what a specific
endpoint returns. A `on_response` that extracts `resp.get('items')` makes
no sense for an endpoint that returns `resp.get('reports')`. Inheriting
them from client level would cause silent bugs.

```python
{
    'base_url': 'https://api.example.com/v1',
    'endpoints': {
        'users': {
            'path': '/users',
            'pagination': offset_pagination(data_path='items'),
            'on_response': lambda resp, state: resp.get('items', []),
            'on_complete': lambda pages, state: [u for page in pages for u in page],
        },
        'reports': {
            'path': '/reports',
            'on_response': lambda resp, state: resp.get('reports', []),
            # different shape, different hook — that's the point
        },
    }
}
```

### Pagination config inherits from client to endpoint

Pagination mechanics (`next_request`, `delay`, `initial_params`) can be set
at client level and inherited by all endpoints. An endpoint can override
with its own `pagination` dict, set `pagination: None` to disable, or omit
it to inherit.

```python
{
    'base_url': 'https://api.direct.yandex.com/json/v5',
    'pagination': {
        'next_request': yandex_next_page,   # all endpoints inherit this
        'delay': 0.1,
    },
    'endpoints': {
        'campaigns': {
            'path': '/campaigns',
            'on_response': lambda resp, state: resp.get('Campaigns'),
            # inherits pagination from client level
        },
        'reports': {
            'path': '/reports',
            'pagination': None,   # explicitly disable pagination for this endpoint
        },
    }
}
```

### Parser-hook decision guide

Both `canonical_parser` and `response_parser` let you control how a response body is interpreted, but they operate at different points in the pipeline and affect different consumers.

| Hook | Input | Runs instead of built-in parsing? | Affects `next_request`? | Affects `on_response` / stream items? |
|---|---|---|---|---|
| `canonical_parser` | `content_bytes: bytes`, `context: dict` | Yes — replaces `response_format` parsing | Yes | Yes, indirectly (sets the value `response_parser` receives as `parsed`) |
| `response_parser` (1-arg) | `response: requests.Response` | No (runs after) | No | Yes |
| `response_parser` (2-arg) | `response`, `parsed: any` | No (runs after) | No | Yes — `parsed` is the already-decoded canonical body |

**Use `canonical_parser` when** `next_request` needs to navigate a non-standard parsed body — for example, XML elements, protobuf objects, or a custom structure that the built-in `response_format` decoders cannot produce.

**Use `response_parser` when** the built-in or canonical parsing is correct for pagination, but you want to reshape what downstream hooks (`on_response`, stream items) receive. The 2-arg form is the right choice when you need to transform the already-decoded body without re-parsing the raw bytes.

```python
# canonical_parser — XML feed: next_request can navigate an Element tree
import xml.etree.ElementTree as ET

def parse_atom(content_bytes, context):
    return ET.fromstring(content_bytes.decode('utf-8'))

client = APIClient({
    'base_url': 'https://example.com',
    'endpoints': {
        'feed': {
            'canonical_parser': parse_atom,
            'pagination': {
                # next_request receives the Element returned by canonical_parser
                'next_request': lambda root, state: (
                    {'params': {'page': state.get('page', 1) + 1}}
                    if root.find('.//next') is not None else None
                ),
            },
        }
    }
})

# response_parser (2-arg) — pagination is fine as JSON, just extract the items list
client = APIClient({
    'base_url': 'https://example.com',
    'endpoints': {
        'users': {
            # next_request still receives the full JSON dict
            'response_parser': lambda resp, parsed: parsed.get('items', []),
            'pagination': {
                'next_request': lambda parsed, state: (
                    {'params': {'cursor': parsed.get('next_cursor')}}
                    if parsed.get('next_cursor') else None
                ),
            },
        }
    }
})
```

### Other inheriting config keys

Several non-hook keys also inherit from client to endpoint:

- `headers` — deep-merged (endpoint headers add to / override client headers)
- `timeout` — float or `(connect, read)` tuple
- `response_parser` — 1-arg or 2-arg callable
- `canonical_parser` — custom bytes→parsed conversion
- `response_format` — `auto` / `json` / `text` / `xml` / `csv` / `bytes`
- `csv_delimiter` — single character for CSV parsing
- `encoding` — text-layer codec name (default `utf-8`)
- `log_level` — `none` / `error` / `medium` / `verbose`
- `scrub_headers` — extra header names to redact in logs and fixtures
- `scrub_query_params` — extra query param names to redact in fixture URLs

These follow simple override semantics: if the endpoint sets the key, the
endpoint value wins. If the endpoint omits it, the client value is used.
`headers` is the exception — it deep-merges rather than replacing.

---

## 3. Retry and rate-limit config resolution

### Retry resolution

Retry config has three-way resolution at endpoint level:

- **omitted** — inherits client-level `retry` config in full
- **`retry: None`** — suppresses additional retry attempts for that endpoint.
  The endpoint gets exactly one attempt. However, reactive rate-limit
  behavior (honoring `Retry-After` headers, respecting `min_delay`) still
  applies to that single attempt. This is intentional — `retry: None` means
  "don't retry," not "ignore the server's rate-limit signals."
- **`retry: {...}`** — merges over client-level retry config. Only the keys
  you specify are overridden; the rest are inherited from client.

```python
{
    'base_url': 'https://api.example.com/v1',
    'retry': {'max_attempts': 3, 'backoff': 'exponential', 'on_codes': [429, 500, 502, 503]},
    'endpoints': {
        'fast': {
            'path': '/fast',
            # inherits client retry — 3 attempts with exponential backoff
        },
        'fragile': {
            'path': '/fragile',
            'retry': {'max_attempts': 5, 'base_delay': 2.0},
            # merges: 5 attempts, 2s base delay, inherits backoff and on_codes from client
        },
        'oneshot': {
            'path': '/oneshot',
            'retry': None,
            # one attempt only, but still honors Retry-After from the server
        },
    }
}
```

`max_retry_after` lives under `retry`, not `rate_limit`. It caps how long
the library will wait on a `Retry-After` header before raising
`RateLimitError`. Placing it under `rate_limit` raises `SchemaError` with
a redirecting message.

### Rate-limit resolution

Rate-limit config uses **full replacement** semantics at endpoint level,
not merging:

- **omitted** — inherits client-level `rate_limit`
- **`rate_limit: None`** — disables rate limiting for that endpoint
- **`rate_limit: {...}`** — fully replaces client-level rate-limit config.
  If you override, you are responsible for the complete desired policy.

This is different from retry (which merges). The rationale: rate-limit
config is a coherent policy bundle (token-bucket params + reactive pacing),
and partial overrides would create confusing hybrid policies.

```python
{
    'base_url': 'https://api.example.com/v1',
    'rate_limit': {
        'requests_per_second': 5.0,
        'burst': 10,
        'min_delay': 0.1,
        'respect_retry_after': True,
    },
    'endpoints': {
        'normal': {'path': '/normal'},   # inherits full rate-limit policy
        'bulk': {
            'path': '/bulk',
            'rate_limit': {'requests_per_second': 1.0, 'burst': 2},
            # fully replaces — no min_delay inherited from client
        },
        'unthrottled': {
            'path': '/health',
            'rate_limit': None,          # no rate limiting at all
        },
    }
}
```

---

## 4. `state` as config source and run seed

`schema['state']` is one source consumed in two separate ways:

**Auth reads it as read-only config.** `CallbackAuth` and
`BearerAuth.token_callback` receive a frozen view of `state` at
construction time. They can read keys like API credentials or tenant
identifiers, but they cannot write back into it. Auth runtime data (cached
tokens, expiry timestamps) stays internal to the auth handler.

**Each run gets a copied seed.** When `fetch()` or `stream()` starts, the
library copies `state` into a fresh per-run `page_state` dict. Pagination
callbacks receive a `StateView` of this copy. Mutations don't leak between
runs and don't affect auth's config view.

`initial_params` (from pagination config) overrides seed keys when they
overlap. This is the seeding order:

1. Copy `schema['state']` → `page_state`
2. Merge `initial_params` on top
3. Merge call-time `params` on top
4. Run starts — callbacks see `StateView(page_state)`

```python
{
    'base_url': 'https://api.example.com/v1',
    'state': {'region': 'eu-west-1', 'env': 'prod'},
    'auth': {
        'type': 'callback',
        'handler': lambda req, config: {
            # config is a read-only view of state — can read 'region' here
            **req,
            'headers': {**req.get('headers', {}), 'X-Region': config['region']}
        }
    },
    'endpoints': {
        'users': {
            'path': '/users',
            'pagination': offset_pagination(data_path='items'),
            # callbacks see state['region'] and state['env'] as seeded values
        },
    }
}
```

---

## 5. Non-paginated endpoints

Non-paginated endpoints (no `pagination` key) go through the same callback
engine as paginated ones. The library synthesizes a single-page run
internally, so all five lifecycle hooks fire exactly once:

- `on_response` — transforms the single response
- `on_page` — side-effect on the single result
- `update_state` — shapes final state before completion hooks
- `on_page_complete` — fires with `PageCycleOutcome` for the single cycle
- `on_complete` — receives the result

This means `on_page_complete` is available for non-paginated endpoints too.
The single request is the cycle. `PageCycleOutcome` carries the same fields
(status code, attempt count, elapsed time, error/stop signal) regardless
of whether pagination was involved.

For `on_complete` specifically: in `fetch()` mode it receives a
single-element list `[page_data]` (not the raw page data). In `stream()`
mode it receives `StreamSummary`. Without `on_complete`, `fetch()` returns
the page data directly (not wrapped in a list).

```python
{
    'base_url': 'https://api.example.com/v1',
    'endpoints': {
        'health': {
            'path': '/health',
            # non-paginated — all hooks still work
            'on_response': lambda resp, state: {'ok': resp.get('status') == 'healthy'},
            'on_page_complete': lambda outcome, state: (
                print(f'health check: {outcome.kind}, status={outcome.status_code}')
            ),
        },
    }
}
```

---

## 6. Pagination config owns mechanics only

The `pagination` dict contains exactly three keys:

- `next_request` — **required** when pagination is enabled. Returns request
  overrides for the next page, or `None` to stop.
- `delay` — static inter-page sleep in seconds.
- `initial_params` — dict injected into the first request. Set automatically
  by built-in strategies (e.g. `offset=0`, `page=1`).

Everything else — `on_response`, `on_page`, `on_complete`, `on_page_complete`,
`update_state` — lives at endpoint level. Placing them inside a `pagination`
dict raises `SchemaError` with a redirecting message:

```
schema[endpoints.ep.pagination]: endpoints.ep.pagination.on_response is not
supported; use on_response at endpoint level instead
```

This is enforced in both strict and non-strict validation modes, because
silently ignoring moved hooks would cause them to stop firing with no
diagnostic path.

### Why `initial_params` stays in pagination

`initial_params` is seed state for the pagination strategy — it's "what
params does page 1 need." For non-paginated endpoints, the same effect is
achieved by putting params directly in the endpoint's `params` config.
`initial_params` is pagination-strategy plumbing, not lifecycle behavior.

---

## 7. Built-in helpers and how they resolve

Built-in pagination helpers like `offset_pagination(...)` and
`cursor_pagination(...)` return a dict that packages both pagination
mechanics and a convenience `on_response` together:

```python
offset_pagination(limit=100, data_path='items')
# returns something like:
# {
#     'next_request': <function>,
#     'on_response': <function that extracts items>,
#     'initial_params': {'offset': 0, 'limit': 100},
# }
```

The library resolves this transparently:

- Pagination mechanics (`next_request`, `delay`, `initial_params`) go to
  the pagination runner.
- The helper's `on_response` is used **unless** the endpoint also defines
  `on_response` at endpoint level, in which case the endpoint-level hook
  wins.

This means you can use helpers as one-liners without thinking about the
split:

```python
'endpoints': {
    'users': {
        'pagination': offset_pagination(data_path='items'),
        # helper's on_response extracts items — you're done
    }
}
```

Or override the helper's `on_response` when you need custom extraction:

```python
'endpoints': {
    'users': {
        'pagination': offset_pagination(data_path='items'),
        'on_response': my_custom_extractor,  # wins over helper's
    }
}
```

Or suppress the helper's `on_response` entirely:

```python
'endpoints': {
    'raw': {
        'pagination': offset_pagination(data_path='items'),
        'on_response': None,  # explicit None suppresses helper's hook
    }
}
```

Users should think in terms of endpoint lifecycle hooks and pagination
mechanics. The helper resolution is a convenience, not a concept to learn.

---

## 8. `on_event_kinds` as a filter

`on_event_kinds` controls which event kinds reach `on_event`. It is a
filter key, not a hook and not a suppression mechanism.

```python
{
    'on_event': my_handler,
    'on_event_kinds': {'retry', 'stopped'},  # only these two kinds fire
}
```

Accepted shapes:

- `None` or `'all'` — all events pass through
- `'retry'` — single kind (string shorthand)
- `{'retry', 'stopped'}` — set of kinds
- `[]` or `set()` — empty collection silences all events

Event kind strings are validated against the library's known set. Typos
like `'retyr'` raise `SchemaError` at construction time.

`on_event_kinds` inherits from client to endpoint, same as `on_event`.
Setting `on_event_kinds` without `on_event` is valid — it supports
inherited handlers and preconfiguration where the handler is wired
separately.

---

## 9. Other config areas

These are not hooks or pagination, but they're part of the schema and have
their own placement and inheritance rules. This section covers where each
lives and whether it inherits — not the full behavioral contract (see
`semantics.md` and `cheatsheet.md` for that).

### Authentication (`auth`)

`auth` is client-level only. It applies to all endpoints uniformly.

Auth config is type-dispatched: `auth.type` selects the variant, and each
variant has its own required keys. The six variants are `bearer`, `basic`,
`api_key`, `oauth2`, `oauth2_password`, and `callback`.

Fixed-shape variants (`bearer`, `basic`, `api_key`, `oauth2`, `oauth2_password`)
have a known set of keys and are strictly validated. `callback` auth is
intentionally open-ended — extra keys are allowed because callback handlers
may use arbitrary user-defined config fields.

Auth runtime state (cached tokens, expiry metadata) stays internal to the
handler and does not appear in callback-visible state.

### Session config (`session_config`)

`session_config` is client-level only. It configures the underlying
`requests.Session`: TLS verification (`verify`), client certificates
(`cert`), proxy settings (`proxies`), and redirect limits
(`max_redirects`). These are session-wide — there is no per-endpoint
override.

### Playback (`playback`)

`playback` is per-endpoint. It controls fixture recording and replay for
deterministic development and testing. Three modes: `auto` (save if no
fixture exists, load if it does), `save` (always record), `load` (always
replay). An optional `record_as_bytes` flag forces raw-byte storage for
textual formats.

Playback is not inherited from client level — each endpoint that uses
playback must configure it explicitly. This is intentional because fixture
paths are endpoint-specific.

### Response format and text-layer config

`response_format`, `encoding`, and `csv_delimiter` all inherit from client
to endpoint. They control how raw HTTP responses are parsed into Python
values:

- `response_format` — selects the parser: `auto`, `json`, `text`, `xml`,
  `csv`, `bytes`
- `encoding` — text-layer codec for `text` and `csv` formats (default
  `utf-8`). This is a general text-layer setting, not CSV-specific.
- `csv_delimiter` — single character for CSV field separation (default `;`).
  This is CSV-specific and independent of `encoding`.

These can also be overridden at call time (`client.fetch('ep', response_format='csv')`).

### Scrubbing config

`scrub_headers` and `scrub_query_params` inherit from client to endpoint.
They add to the built-in default scrubbing rules for logs and playback
fixtures. The library has built-in scrubbing for common sensitive headers
(`Authorization`, `X-Api-Key`, etc.) and pattern-based matching
(`token`, `secret`, `password`, `key`, `auth` substrings).

### Timeout

`timeout` inherits from client to endpoint. It accepts a single float
(applied to both connect and read) or a `(connect, read)` tuple.
Always set a timeout — `requests` has no default and will hang indefinitely.

### Logging

`log_level` inherits from client to endpoint. Accepted values: `none`,
`error`, `medium`, `verbose`. Default is `medium`.

`debug` is endpoint-only (does not inherit). When `True`, logs full
request/response details at DEBUG level regardless of `log_level`.

### Mock

`mock` is endpoint-only. It provides fake responses for testing without
HTTP. Accepts a callable or a list of response dicts. Mock responses go
through the same parsing and callback pipeline as live responses.

### Metrics (`metrics`)

`metrics` is client-level only. It does not inherit to endpoints and is
not overridable per endpoint.

Accepted values: `True` (construct a default `MetricsSession`),
`False` / `None` / omitted (disabled), or an explicit `MetricsSession()`
instance. Any other value is rejected with `SchemaError`.

When enabled, `client.metrics` is the `MetricsSession` object directly.
When disabled, `client.metrics` is `None`. The session accumulates
additive totals across runs: `total_runs`, `total_failed_runs`,
`total_requests`, `total_retries`, `total_pages`, `total_bytes_received`,
`total_retry_bytes_received`, `total_wait_seconds`, `total_elapsed_seconds`.

`summary()` returns a frozen `MetricsSummary` snapshot with those grand
totals plus `by_endpoint: dict[str, EndpointMetrics]` — a per-endpoint
breakdown using the same fields without the `total_` prefix. The sum of any
`EndpointMetrics` field across all keys equals the corresponding grand total.
Runs with no endpoint name bucket under `'<unknown>'`. `reset()` atomically
returns the pre-reset snapshot and clears all counters and buckets.

Users may pass `metrics: MetricsSession()` to share one session across
multiple clients.

---

## 10. Annotated build-up example

### Step 1: minimal non-paginated client

```python
from rest_fetcher import APIClient

client = APIClient({
    'base_url': 'https://api.example.com/v1',
    'auth': {'type': 'bearer', 'token': 'my-token'},
    'timeout': 30,
    'endpoints': {
        'status': {
            'method': 'GET',
            'path': '/status',
            # no pagination, no hooks — just fetch and return the parsed JSON
        },
    },
})

result = client.fetch('status')
# result is the parsed JSON response
```

### Step 2: add a paginated endpoint with a helper

```python
from rest_fetcher import APIClient, offset_pagination

client = APIClient({
    'base_url': 'https://api.example.com/v1',
    'auth': {'type': 'bearer', 'token': 'my-token'},
    'timeout': 30,
    'endpoints': {
        'status': {
            'path': '/status',
        },
        'users': {
            'path': '/users',
            # pagination mechanics — helper provides next_request + initial_params
            'pagination': offset_pagination(limit=100, data_path='items'),
            # helper also provides on_response that extracts resp['items']
            # so we don't need to set on_response ourselves here
        },
    },
})

all_users = client.fetch('users')
# all_users is a list of pages, each page is a list of user dicts
```

### Step 3: add lifecycle hooks

```python
from rest_fetcher import APIClient, offset_pagination, cursor_pagination

client = APIClient({
    'base_url': 'https://api.example.com/v1',
    'auth': {'type': 'bearer', 'token': 'my-token'},
    'timeout': 30,

    # cross-cutting: inherits to all endpoints
    'on_error': lambda exc, state: 'skip',

    'endpoints': {
        'status': {
            'path': '/status',
            # non-paginated endpoint with on_page_complete for cycle observability
            'on_page_complete': lambda outcome, state: (
                print(f'status check: {outcome.kind}, {outcome.status_code}')
            ),
        },
        'users': {
            'path': '/users',
            'pagination': offset_pagination(limit=100, data_path='items'),
            # endpoint-level on_response overrides helper's extraction
            'on_response': lambda resp, state: [
                {**u, 'source': 'api'} for u in resp.get('items', [])
            ],
            # flatten all pages into one list
            'on_complete': lambda pages, state: [u for page in pages for u in page],
        },
        'events': {
            'path': '/events',
            'pagination': cursor_pagination(
                cursor_param='cursor',
                next_cursor_path='meta.next_cursor',
                data_path='data',
            ),
            # track state across pages
            'update_state': lambda resp, state: {
                'events_seen': state.get('events_seen', 0) + len(resp.get('data', []))
            },
            # adaptive throttling — slow down after errors
            'on_page_complete': lambda outcome, state: (
                2.0 if outcome.kind in {'skipped', 'stopped'} else None
            ),
        },
    },
})
```

### Step 4: add event filtering

```python
from rest_fetcher import APIClient, offset_pagination

def log_event(ev):
    print(f'[{ev.source}] {ev.kind} endpoint={ev.endpoint}')

client = APIClient({
    'base_url': 'https://api.example.com/v1',
    'auth': {'type': 'bearer', 'token': 'my-token'},
    'timeout': 30,

    # cross-cutting telemetry — all endpoints inherit
    'on_event': log_event,
    'on_event_kinds': {'retry', 'stopped', 'error'},  # only these fire

    'on_error': lambda exc, state: 'skip',

    'endpoints': {
        'users': {
            'path': '/users',
            'pagination': offset_pagination(limit=100, data_path='items'),
            'on_complete': lambda pages, state: [u for page in pages for u in page],
        },
        'debug_endpoint': {
            'path': '/debug',
            # override: this endpoint wants ALL events for debugging
            'on_event_kinds': None,
        },
    },
})
```

---

## 11. Common mistakes

**Putting lifecycle hooks inside `pagination`:**

```python
# WRONG — raises SchemaError
'pagination': {
    'next_request': my_next,
    'on_response': my_extractor,   # not allowed here
}

# RIGHT — hooks live at endpoint level
'pagination': {'next_request': my_next},
'on_response': my_extractor,
```

**Expecting lifecycle hooks to inherit from client level:**

```python
# WRONG — on_response is endpoint-only, this does nothing
{
    'base_url': '...',
    'on_response': lambda resp, state: resp['items'],  # ignored by endpoints
    'endpoints': {'ep': {'path': '/x'}},
}

# RIGHT — set on each endpoint that needs it
{
    'base_url': '...',
    'endpoints': {
        'ep': {
            'path': '/x',
            'on_response': lambda resp, state: resp['items'],
        }
    },
}
```

**Treating `on_event_kinds` as a hook:**

```python
# WRONG — on_event_kinds is a filter, not a callable
'on_event_kinds': lambda ev: ev.kind == 'retry'

# RIGHT — pass a set of kind strings
'on_event_kinds': {'retry', 'stopped'}
```

**Confusing pagination mechanics with response processing:**

Pagination config answers "how do I get the next page?" — continuation
logic, delay, starting params. Everything about "what do I do with the
data" belongs at endpoint level: `on_response` for extraction,
`on_page` for side effects, `on_complete` for aggregation, `update_state`
for cross-page tracking, `on_page_complete` for adaptive pacing.

**Expecting endpoint `rate_limit` to merge with client:**

```python
# SURPRISE — endpoint rate_limit fully replaces client, not merges
{
    'rate_limit': {'requests_per_second': 5.0, 'burst': 10, 'min_delay': 0.1},
    'endpoints': {
        'ep': {
            'rate_limit': {'requests_per_second': 1.0, 'burst': 2},
            # min_delay is NOT inherited — endpoint owns the full policy
        }
    }
}
```

**Putting `max_retry_after` under `rate_limit`:**

```python
# WRONG — raises SchemaError with redirecting message
'rate_limit': {'max_retry_after': 30}

# RIGHT — max_retry_after belongs under retry
'retry': {'max_retry_after': 30}
```

---

## 12. Schema validation and IDE-friendly authoring

### Validation behavior

`validate()` is called automatically by `APIClient`. It defaults to `strict=True`.

**strict=True (default):** unknown keys at any schema boundary raise `SchemaError`.
All unknown-key issues across the entire schema are collected before raising, so you
see every problem in one pass:

```
SchemaError:
schema[root]: unknown key 'retrry' in 'root'. Allowed keys: auth, base_url, ...
  Did you mean 'retry'?
schema[endpoints.users]: unknown key 'on_respnse' in 'endpoints.users'. Allowed keys: ...
  Did you mean 'on_response'?
```

**strict=False:** unknown keys emit `SchemaValidationWarning` instead of raising.
Useful during development or when introducing experimental keys. Filter with Python's
standard `warnings` module:

```python
import warnings
from rest_fetcher.exceptions import SchemaValidationWarning

warnings.filterwarnings('ignore', category=SchemaValidationWarning)
```

**Always strict, regardless of mode:** type errors, missing required fields, and
semantic checks (e.g. `body` and `form` mutually exclusive) fail immediately on the
first problem found. The `strict` flag only controls unknown-key handling.

### IDE-friendly schema authoring

Inline untyped dict literals give no key suggestions in most IDEs. Two approaches fix this:

**Option 1: TypedDict annotation**

```python
from rest_fetcher.types import ClientSchema, EndpointSchema

schema: ClientSchema = {
    'base_url': 'https://api.example.com/v1',
    'auth': {'type': 'bearer', 'token': 'my-token'},
    'endpoints': {
        'users': EndpointSchema(method='GET', path='/users', params={'active': True}),
    },
}
client = APIClient(schema)
```

`ClientSchema` covers all top-level keys. `EndpointSchema` covers all endpoint-level keys.
Both are `TypedDict(total=False)` — all keys are optional at the type level; required keys
are enforced at runtime by `validate()`.

**Option 2: SchemaBuilder**

```python
from rest_fetcher import SchemaBuilder

schema = (
    SchemaBuilder('https://api.example.com/v1')
    .bearer('my-token')
    .endpoint('users', method='GET', path='/users', params={'active': True})
    .build()
)
client = APIClient(schema)
```

Both approaches produce identical plain dicts at runtime. The choice is stylistic.
