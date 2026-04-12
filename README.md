# rest_fetcher

Internal Python library for fetching data from REST APIs in ETL pipelines.
It centralizes auth, retry, pagination, parsing, playback, and error context
so pipeline code can stay boring in the good way.

## Requirements

Python 3.10+, `requests>=2.28`.

```bash
pip install -e .
```

## Features

- **Declarative endpoint configuration for ETL fetch flows**  
  Define request behavior in schema instead of rewriting ad hoc request loops in every pipeline.

- **Built-in and custom pagination with callback-driven control flow**  
  Five built-in strategies cover offset, cursor, page-number, Link-header, and URL-header APIs. Custom pagination is driven by `next_request(parsed_body, state)`.

- **Callback and state hooks for pipeline control**  
  Supports hooks such as `on_response`, `update_state`, `on_complete`, and `on_page_complete` for post-cycle adaptive throttling, plus custom pagination callbacks.

- **Flexible response parsing for JSON and non-JSON APIs**  
  Supports built-in `auto`, `json`, `text`, `xml`, `csv`, and `bytes` formats, plus `canonical_parser(content_bytes, context)` for custom parsing before pagination and downstream callbacks.

- **Streaming and full-fetch execution modes**  
  Use `fetch()` to collect results, `fetch_pages()` for a predictable list that never unwraps single-page results, or `stream()` / `stream_run()` when page-by-page processing and stop observability matter.

- **Playback mode for deterministic development, debugging, and tests**  
  Record live responses once and replay them later through the same parsing and callback pipeline.

- **Structured exceptions and safety caps**  
  Includes ETL-oriented stop and failure controls such as `max_pages`, `max_requests`, `time_limit`, and rich exception context.

- **Retry handling and rate limiting for real ETL workloads**  
  Supports retry policies, reactive `Retry-After` handling, and proactive token-bucket throttling with `requests_per_second` and `burst`.

- **Authentication support for common API setups**  
  Includes bearer token, basic auth, OAuth2 client credentials, OAuth2 password grant, and custom auth callbacks.

- **Lifecycle telemetry via `on_event`**  
  Emits structured runtime events for request, retry, parse, callback-error, stop, playback, and rate-limit behavior. `PaginationEvent.to_dict()` returns a stable, JSON-serializable dict for ETL audit logging.

- **Typed schema builder for IDE-friendly configuration**  
  `SchemaBuilder` provides a more guided alternative to writing raw schema dictionaries by hand.

## Quick start

```python
from rest_fetcher import APIClient, offset_pagination

client = APIClient({
    'base_url': 'https://api.example.com/v1',
    'auth': {'type': 'bearer', 'token': 'my-token'},
    'timeout': 30,
    'endpoints': {
        'users': {
            'method': 'GET',
            'path': '/users',
            'pagination': offset_pagination(limit=100, data_path='items', total_path='total'),
        }
    }
})

# collect all pages
all_users = client.fetch('users')

# or stream page by page
for page in client.stream('users'):
    write_to_warehouse(page)
```

If you prefer a typed builder with IDE autocompletion, use `SchemaBuilder`:

```python
from rest_fetcher import SchemaBuilder

schema = (
    SchemaBuilder('https://api.example.com/v1')
    .bearer('my-token')
    .timeout(30)
    .endpoint('users', method='GET', path='/users')
    .build()
)
client = APIClient(schema)
```

## fetch_pages() and event serialization

`fetch_pages()` materializes `stream()` into a list and always returns a list — unlike `fetch()`, which unwraps single-page results to the bare value:

```python
# fetch() shape depends on page count — dict for one page, list for many
result = client.fetch('users')

# fetch_pages() is always a list, even for a single-page response
pages = client.fetch_pages('users')
pages = client.fetch_pages('users', max_pages=10)  # same caps as fetch() and stream()
```

`on_complete` in `fetch_pages()` follows stream semantics: fires with `(StreamSummary, state)`, return value ignored. Use `fetch()` if you need `on_complete` to transform the returned data.

`PaginationEvent.to_dict()` returns a stable, JSON-serializable dict for all event fields except `mono`. Useful for ETL audit logging:

```python
import json

client = APIClient({
    ...
    'on_event': lambda ev: print(json.dumps(ev.to_dict())),
})
```

## Schema validation

`validate()` is called automatically by `APIClient` and defaults to `strict=True`: unknown keys raise `SchemaError` with all issues reported together, including typo suggestions and the list of allowed keys. Pass `strict=False` to emit `SchemaValidationWarning` instead of raising.

For IDE autocompletion and mypy support, annotate raw dict schemas with `ClientSchema` / `EndpointSchema` from `rest_fetcher.types`, or use `SchemaBuilder` — see [docs/schema_guide.md](docs/schema_guide.md#12-schema-validation-and-ide-friendly-authoring).

## Response formats

`rest_fetcher` supports non-JSON responses as a first-class feature via
`response_format` on the client schema, endpoint schema, or per-call overrides.

| Format | Returns |
|---|---|
| `auto` | Infer from `Content-Type`; falls back to JSON if no header. |
| `json` | JSON-decoded Python value. |
| `text` | `str` decoded with `encoding` (default `utf-8`). |
| `xml` | `xml.etree.ElementTree.Element`. |
| `csv` | `list[dict[str, str]]` via `csv.DictReader`; uses `csv_delimiter` (default `;`) and `encoding`. |
| `bytes` | Raw `bytes`. |

For custom parsing, use `canonical_parser(content_bytes, context)` to convert raw bytes
into any canonical form before pagination and callbacks run. See `examples/atom_example.py`
for a small XML reference and `examples/case_studies/pcal_example.py` for a fuller lxml/XPath
example. The 2-arg `response_parser(response, parsed)` receives the canonical parsed body
as its second argument.

```python
client.fetch('feed', response_format='xml')
client.fetch('report', response_format='csv')
client.fetch('report', response_format='csv', csv_delimiter=',')  # override default ';'
```

## Adaptive throttling

`on_page_complete(outcome, state)` is a post-cycle pagination hook for adaptive pacing.
It runs after the current page cycle resolves and may return seconds to sleep before
continuing to the next page.

```python
from rest_fetcher import APIClient, cursor_pagination, PageCycleOutcome

def pace_after_slow_cycle(outcome: PageCycleOutcome, state: dict) -> float | None:
    if outcome.kind == 'success' and outcome.cycle_elapsed_ms and outcome.cycle_elapsed_ms > 2000:
        return 0.5
    return None

client = APIClient({
    'base_url': 'https://api.example.com/v1',
    'endpoints': {
        'reports': {
            'path': '/reports',
            'pagination': cursor_pagination('cursor', 'meta.next_cursor'),
            'on_page_complete': pace_after_slow_cycle,
        },
    },
})
```

See `docs/schema_guide.md` for the config ownership model, `docs/semantics.md` for
behavioral contracts, `docs/cheatsheet.md` for a compact reference, and
`examples/examples.py` for more patterns.

## OAuth2 auth modes

`rest_fetcher` supports two built-in OAuth2 styles:

- `auth.type='oauth2'` — client credentials flow (`grant_type=client_credentials`)
- `auth.type='oauth2_password'` — password grant (`grant_type=password`)

Both cache the access token on the handler instance, refresh automatically
before expiry using `expiry_margin` (default 60 seconds), and inject
`Authorization: Bearer ...` into outgoing requests.

`oauth2_password` exists mainly for APIs such as GLPI v2 that want both client
credentials and a real user identity for automated access.

```python
client = APIClient({
    'base_url': 'https://glpi.example.com/api.php/v2',
    'auth': {
        'type': 'oauth2_password',
        'token_url': 'https://glpi.example.com/api.php/token',
        'client_id': 'my-client-id',
        'client_secret': 'my-client-secret',
        'username': 'api-user',
        'password': 'correct horse battery staple',
        'scope': 'api',
    },
    'endpoints': {
        'ticket': {'method': 'GET', 'path': '/Ticket/123'},
    }
})
```

## Lifecycle events and rate limiting

Observe what the pagination engine is doing via `on_event`, without mixing
telemetry into data callbacks:

```python
def on_event(ev):
    # ev.kind: 'request_start' | 'request_end' | 'retry' | 'page_parsed' |
    #          'stopped' | 'callback_error' | 'error' |
    #          'rate_limit_wait_start' | 'rate_limit_wait_end' | 'rate_limit_exceeded'
    # ev.source: 'live' or 'playback'
    print(ev.kind, ev.source, ev.data)

client = APIClient({
    'base_url': 'https://api.example.com',
    'on_event': on_event,
    'rate_limit': {
        'requests_per_second': 2.0,   # sustained refill rate
        'burst': 5,                   # max immediate requests when bucket is full
        'on_limit': 'wait',           # or 'raise' to raise RateLimitExceeded
    },
    'endpoints': {
        'users': {'method': 'GET', 'path': '/users'},
        'bulk': {
            'method': 'GET',
            'path': '/bulk',
            'rate_limit': {'requests_per_second': 0.5, 'burst': 1},
        },  # endpoint override
        'ping': {
            'method': 'GET',
            'path': '/ping',
            'rate_limit': None,
        },  # opt out
    },
})
```

The `rate_limit` dict covers proactive token-bucket limiting plus reactive pacing keys (`respect_retry_after`, `min_delay`). The retry-layer cap `max_retry_after` lives under `retry`. See `docs/semantics.md` §Rate Limiting and `examples/events_rate_limit_example.py` for details.

## Playback

Record real API responses to a fixture file on first run, then replay them
offline on every subsequent run — no network required:

```python
'endpoints': {
    'campaigns': {
        'playback': 'fixtures/campaigns.json',  # 'auto' mode: save once, replay after
    }
}
```

Fixture files store raw response envelopes that feed the same parsing and
callback pipeline as a live run. Recording scrubs request-side secrets at save
time using the same rules as log redaction (`scrub_headers`, `scrub_query_params`).
Response bodies are left untouched. If your API uses a generic sensitive query
parameter such as `code`, add it explicitly: `scrub_query_params=['code']`.

Playback config also accepts `mode: 'save'` (always refresh), `mode: 'load'`
(always replay, error if missing), and `mode: 'none'` (disable without removing
the block). Set `record_as_bytes: true` to store raw bytes instead of decoded
text when exact wire fidelity matters.

A curated set of examples ships with companion fixtures under `examples/fixtures/`
(and `examples/case_studies/fixtures/`). Those examples default to `mode: 'none'`;
change to `'load'` to try offline replay. Fixtures are an in-repo artifact —
regenerate from the current project state if you copy them elsewhere.

## Layout

```text
rest_fetcher/
├── rest_fetcher/
│   ├── __init__.py
│   ├── auth.py
│   ├── callbacks.py
│   ├── client.py
│   ├── context.py
│   ├── events.py
│   ├── exceptions.py
│   ├── pagination.py
│   ├── parsing.py
│   ├── playback.py
│   ├── rate_limit.py
│   ├── retry.py
│   ├── schema.py
│   ├── metrics.py
│   └── types.py
├── docs/
│   ├── schema_guide.md              — config ownership, inheritance, layering
│   ├── semantics.md                 — behavioral contracts and firing semantics
│   ├── cheatsheet.md                — compact key reference
│   ├── contributor_contract_map.md  — maintainer invariants and fragile seams
│   └── render_cheatsheet.py
├── examples/
│   ├── examples.py                  — annotated examples, curated playback fixtures
│   ├── atom_example.py              — canonical_parser XML pagination reference
│   ├── events_rate_limit_example.py — on_event + token bucket with fake clock
│   ├── glpi_example.py              — oauth2_password auth
│   ├── anthropic_example.py
│   ├── booze_example.py
│   ├── nager_example.py
│   ├── whoisjson_example.py
│   ├── fixtures/                    — curated raw playback fixtures
│   └── case_studies/
│       ├── pcal_example.py          — lxml/XPath canonical_parser, streaming ETL
│       └── fixtures/
├── tests/
│   ├── test_core.py
│   ├── test_events_rate_limit.py
│   ├── test_examples_replay.py
│   ├── test_parsing_pagination.py
│   └── test_playback.py
├── pyproject.toml
├── CONTRIBUTING.md
└── README.md
```

## Running tests

Use pytest as the single test runner:

```bash
pytest -q
```

The suite includes `unittest.TestCase`-style tests; pytest runs them fine.
New tests may use either style, but the runner is always `pytest -q`.

## Documentation

- **`docs/semantics.md`** — behavioural contracts: response parsing, pagination/state rules,
  lifecycle events, rate limiting, playback semantics, header scrubbing, exception context,
  retry semantics, and safety caps
- **`docs/cheatsheet.md`** — complete schema reference, callback signatures, built-in strategies,
  `SchemaBuilder` API, usage examples, and exception hierarchy
- **`docs/render_cheatsheet.py`** — optional helper to render `cheatsheet.md` to `.docx`;
  convenience utility, not part of the library runtime contract
