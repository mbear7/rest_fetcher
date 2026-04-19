# rest_fetcher — Behavioural Semantics

Non-obvious decisions and contracts that are not immediately clear from the
API surface. Read this before assuming something is a bug.

**Related docs:** `schema_guide.md` covers config ownership, inheritance, and layering. `cheatsheet.md` is the compact key reference.

---

## Contents

- [Authentication](#authentication)
- [Header Scrubbing](#header-scrubbing)
- [Rate Limiting](#rate-limiting)
- [Config Override Semantics](#config-override-semantics)
- [Response Parsing](#response-parsing)
- [Pagination](#pagination)
- [State Scopes](#state-scopes)
- [Callback Contract](#callback-contract)
- [Stop and Completion Semantics](#stop-and-completion-semantics)
- [Lifecycle Events](#lifecycle-events)
- [Playback](#playback)
- [Error Semantics](#error-semantics)
- [Typing and Helper Objects](#typing-and-helper-objects)
- [Observability](#observability)
- [Request execution order](#request-execution-order)
- [Pre-request shaping](#pre-request-shaping)
- [Adaptive throttling](#adaptive-throttling)

---

## Authentication

### OAuth2 token caching

Built-in OAuth2 handlers (`auth.type='oauth2'` for client credentials and
`auth.type='oauth2_password'` for password grant) cache the access token and
its expiry time on the handler instance. Before each request they check whether
the token has expired (using `expiry_margin`, default 60 seconds) and re-fetch
if needed.

The public contract is deliberately small:
- token acquisition is automatic
- cached tokens are reused until refresh time
- expiry triggers a fresh token request
- no stronger promise is made about refresh-token support; handlers may simply
  obtain a new access token again

**Thread safety:** the check-then-fetch is not atomic. In a single-threaded
context this is fine. In a multi-threaded context two threads can race and
perform two redundant token fetches. If that matters, create one `APIClient`
per thread.

**Clock adjustments:** expiry tracking uses `time.monotonic()` so wall-clock
changes do not make cached tokens appear to expire earlier or later. If the
token is rejected by the server for any reason, the caller gets `AuthError`.

---

## Header Scrubbing

Headers are redacted in logs (not in requests) via two mechanisms:

**Exact match** — the following headers are always scrubbed:
`Authorization`, `X-Api-Key`, `X-Api-Secret`, `X-Auth-Token`, `Api-Key`,
`Proxy-Authorization`, `Cookie`, `Set-Cookie`.

Add extra names via `schema['scrub_headers']`.

**Pattern match** — any header whose name contains one of these substrings
(case-insensitive) is also scrubbed: `token`, `secret`, `password`, `key`,
`auth`.

Scrubbing is applied to logged output only. The actual HTTP request is
unaffected.

---

## Rate Limiting

The `rate_limit` config dict serves two distinct purposes that share one config
key. Understanding this is important for using the config correctly.

### Proactive rate limiting (token bucket)

Token-bucket rate limiting proactively paces requests before they are sent.
Configure it by including `requests_per_second` and `burst` in the `rate_limit`
dict:

```python
'rate_limit': {
    'strategy': 'token_bucket',      # only supported strategy; default if omitted
    'requests_per_second': 2.0,      # sustained refill rate
    'burst': 5,                      # bucket capacity — max immediate requests when full
    'on_limit': 'wait',              # 'wait' (default) or 'raise'
}
```

- `requests_per_second` is the refill rate. It controls the sustained average
  request rate.
- `burst` is the bucket capacity. It controls how many requests may proceed
  immediately when capacity has accumulated after idle time.
- Each request consumes one token. Once the bucket is empty:
  - `on_limit='wait'` (default) sleeps until capacity is available.
  - `on_limit='raise'` emits `rate_limit_exceeded` and raises `RateLimitExceeded`.
- Token bucket is not the same as "one request every X seconds". It allows
  short spikes without changing the long-run average rate.
- Proactive rate limiting is bypassed during playback.

`rate_limit` can be set at client level and replaced per endpoint.
Endpoint `rate_limit` is a full policy override: it fully replaces the
client-level `rate_limit` for that endpoint, and it is **not merged** with the
client default. If you override `rate_limit` at endpoint level, you are
responsible for all rate-limit options for that endpoint. Set `rate_limit:
None` at the endpoint level to opt out explicitly.

**`requests_per_second` and `burst` are required when using proactive rate
limiting.** If the `rate_limit` dict includes any token-bucket key
(`strategy`, `requests_per_second`, `burst`, `on_limit`), all required
token-bucket keys must be present.

### Reactive rate limiting (Retry-After and min_delay)

Reactive settings are also placed in the `rate_limit` dict and control how
the retry layer responds after a rate-limit response is received:

```python
'rate_limit': {
    'respect_retry_after': True,   # default True — honour Retry-After header on 429/503
    'min_delay': 0.5,              # minimum seconds between any two requests
}
```

- `respect_retry_after` (bool, default `True`) — if `True`, the retry layer
  reads the `Retry-After` response header and sleeps for the indicated duration
  before retrying.
- `min_delay` (float ≥ 0, default `0.0`) — enforces a minimum gap between any
  two consecutive requests, regardless of retry status.

Reactive pacing keys can still be combined with proactive token-bucket limiting in the same `rate_limit` dict, but `max_retry_after` now lives under `retry` because it bounds retry-layer waiting rather than token-bucket policy.

> **Note:** `SchemaBuilder.rate_limit()` currently generates only reactive
> keys. Do not use it if you need proactive token-bucket limiting — build the
> `rate_limit` dict by hand instead.

---


## Config Override Semantics

The library supports client-, endpoint-, and call-level configuration, but not
every config family resolves the same way. The table below summarizes the
public contract for the main families that are easiest to misread.

| Family | Scopes | Rule | `None` meaning | Mode / sharing notes |
| --- | --- | --- | --- | --- |
| `headers` | client, endpoint, call | merged across scopes; later scopes win per key | no special disable meaning | applies in live, mock, and playback fixture execution |
| `params` | client, endpoint, call, pagination overrides | merged across scopes; later scopes win per key; pagination `params_mode='replace'` clears carry-forward params for that next request | no special disable meaning | applies in live and playback request construction; sticky call params carry across pagination unless replaced |
| `pagination` | client, endpoint | endpoint config replaces client pagination config for that endpoint | `pagination: None` disables inherited pagination | pagination callbacks run in live and playback; non-paginated runs still use the callback engine for one completed page |
| `retry` | client, endpoint | endpoint dict merges over client retry config | `retry: None` disables additional retry attempts for that endpoint | applies to live requests and playback fixture replays that surface retryable responses |
| `rate_limit` | client, endpoint, call | full replacement, not field-wise merge | explicit `None` disables inherited rate-limit policy | proactive token-bucket limiting applies in live and mock, not playback; client/endpoint token buckets may be shared, call-level overrides are not |
| `canonical_parser` | client, endpoint, call | later scopes replace earlier parser | `None` means no canonical parser at that scope | affects canonical parsed body visible to pagination and downstream callbacks in live and playback |
| `metrics` | client | client-level only | `False` / `None` / omitted disable metrics | `True` constructs a default `MetricsSession`; explicit session instance is reused directly and shared across runs/clients that hold it |

Notes:

- `rate_limit` uses replacement semantics. Overriding it at endpoint or call
  scope means you are supplying the full active policy for that scope rather
  than partially patching inherited fields.
- `retry` and `rate_limit` are related but distinct. Retry controls retry-loop
  behavior after a failure; proactive token-bucket rate limiting runs before a
  live request is sent.
- `params_mode='replace'` is a pagination override-processing control key, not
  persistent request payload state. It affects the next request being built and
  does not become a sticky stored request param.
- Playback bypasses proactive token-bucket waits by design, so mode-specific
  rate-limit behavior should be interpreted with that exception in mind.

---

## Response Parsing

### response_format

Responses are parsed according to `response_format`, which can be set at the
client level, endpoint level, or per call. Supported values are:

- `auto` — infer from `Content-Type`
- `json` — JSON-decoded Python value
- `text` — `str`
- `xml` — `xml.etree.ElementTree.Element`
- `csv` — `list[dict[str, str]]` parsed with `csv.DictReader` using `csv_delimiter` (default `;`) and `encoding` (default `utf-8`)
- `bytes` — raw `bytes`

When `response_format='auto'` and the server does not send a `Content-Type`,
the library falls back to JSON as the default parsing choice.

### CSV parsing notes

For CSV responses, the built-in parser intentionally keeps its CSV-specific
surface small: `csv_delimiter` is the format-specific knob. `encoding` is
broader text-decoding config used for textual response handling, including
`text` and `csv`. Other CSV-specific concerns (metadata rows, dialect quirks,
format-specific surprises) are intentionally out of scope for the built-in CSV
parser; use `response_format='text'` plus a custom `response_parser` in those
cases.

---

### canonical_parser

Endpoints may optionally provide `canonical_parser(content_bytes, context)`.
If set, it replaces the built-in `response_format` parsing step and its return
value becomes the canonical `parsed_body` visible to pagination
(`next_request(parsed_body, state)`).

- `content_bytes` is the raw response body as `bytes`.
- `context` is a dict with the following keys:
  - `response_format` — the configured format string
  - `csv_delimiter` — the configured delimiter
  - `encoding` — the configured text-layer encoding
  - `headers` — response headers dict
  - `status_code` — HTTP status code (int)
  - `url` — request URL (str)
  - `request_kwargs` — the full request kwargs dict (post-auth)

This is the intended generic mechanism for custom non-JSON parsing. Use it to
turn raw response bytes into canonical data before pagination and downstream
callbacks run. The library does not ship a separate generic non-JSON helper
layer; `canonical_parser` is that extension point.

It remains especially useful for advanced / example-scoped parsing (e.g.
`lxml.html` + XPath) without forcing the core library to grow format-specific
helpers. See `examples/atom_example.py` for a small reference pattern.

### response_parser arity

Custom `response_parser` callbacks support two forms:

- `parser(response)` — full control from the raw `requests.Response`
- `parser(response, parsed)` — receives the raw response and the library's
  canonical parsed body for the configured/inferred response format

The second argument is the canonical parsed body (output of `canonical_parser`
if configured, otherwise the built-in parse for `response_format`). It can be
a JSON value, `str`, XML root element, CSV row list, `bytes`, or `None` for an
empty body.

---
## Pagination

### Reading the current request inside a callback: `_request`

Every pagination callback receives `state['_request']` — a snapshot of the
exact request dict sent to produce the current response. It contains `method`,
`url`, `params`, `headers`, and `json`/`data` (post-auth values).

This is the cleanest way to read a pagination counter without any state
tracking:

```python
def next_request(parsed_body, state):
    req_params   = (state.get('_request') or {}).get('params') or {}
    current_page = req_params.get('page', 1)
    if current_page >= parsed_body.get('total_pages', 1):
        return None
    return {'params': {'page': current_page + 1}}
```

Use `_request` when the counter is a request param you already sent.
Use `update_state` when the counter comes from the response body (e.g. a
`total_rows` accumulator that spans multiple pages).

Note: `_request` is captured **after** auth injection, so auth-added headers
(e.g. `Authorization: Bearer ...`) are visible to callbacks. `on_complete`
receives a final state snapshot without `_request` because the loop is done
and there is no current request.

### Built-in strategies do not mutate state

`offset_pagination` and `page_number_pagination` read from
`state['_request']['params']` instead of mutating pagination counters in
`state`, so they work correctly with the read-only `StateView` and produce no
side effects.

If you call these strategy callbacks directly (e.g. in unit tests) without a
`_request` key in state, they fall back to `state.get('offset', 0)` and
`state.get('page', 1)`.

### Stop signal priority (offset and page_number strategies)

When a `total_path` / `total_pages_path` is configured, stop decisions follow
this priority order:

1. `total_path` resolves to a parseable integer — compare offset vs total,
   stop when `next_offset >= total`.
2. `total_path` resolves to a value that cannot be parsed as an integer
   (e.g. `"unknown"`, `""`, `"25 items"`) — **stop immediately** and log a
   warning. The library does not fall back to the short-page heuristic.
   Rationale: if you configured `total_path`, you expected the API to return
   it. Silently ignoring a malformed value would hide a contract violation.
3. `total_path` resolves to `None` (path is present but the key is missing
   from the response, or an intermediate segment is not a dict) — same as
   above: stop immediately with a warning.
4. No `total_path` configured — short-page heuristic: stop when
   `len(items) < effective_limit`.

The same priority applies to `total_pages_path` in `page_number_pagination`.

### params_mode: replace

By default, `next_request` return values are deep-merged into the current
request (`params_mode: "merge"`). Sticky keys — anything set in `params` at
the call site — carry forward to every subsequent page. Built-in helpers
intentionally omit `params_mode` when staying on the default merge behavior and
only emit it for `"replace"`. `params_mode` is an override-processing control
key, not persistent request payload state, so it is consumed during override
application and does not appear later in `state['_request']`. Invalid values
raise instead of silently falling back to merge.

Use `params_mode: "replace"` when the next-page params must be exact with no
carry-forward:

```python
# merge (default): filter param survives on page 2
return {'params': {'page': 2}}

# replace: only page=2 is sent, filter is cleared
return {'params': {'page': 2}, 'params_mode': 'replace'}

# replace with no params key: all params cleared
return {'params_mode': 'replace'}
```

If `params_mode: "replace"` is present with no `params` key, the request
params are cleared entirely — this is intentional and useful when the
next-page URL is already complete (e.g. Link header pagination).

### path_resolver for total_path

`total_path` and `total_pages_path` accept dot-separated paths
(e.g. `"meta.pagination.total"`) resolved by the default dict-only resolver.

The default resolver returns `None` if any segment is missing or is not a
dict. It does support list indices when the current value is a list.

For advanced traversal, inject a custom resolver:

```python
def my_resolver(resp, path):
    # supports list indices: "pages.0.total"
    value = resp
    for part in path.split('.'):
        if isinstance(value, list):
            value = value[int(part)]
        elif isinstance(value, dict):
            value = value.get(part)
        else:
            return None
    return value

offset_pagination(total_path='pages.0.total', path_resolver=my_resolver)
```

---

## State Scopes

There are two separate state scopes that exist simultaneously during a
paginated run:

**schema['state']** is one source consumed in two separate ways. Auth reads it as read-only config. Each `PaginationRunner.run()` call also receives a copied seed from it and uses that to initialize run-local `page_state`.

**page_state** — created fresh per `PaginationRunner.run()` call. Tracks pagination counters (`offset`, `page`, `cursor`, etc.), `_response_headers`, `_request`, and clean bounded-stop metadata. Discarded when the run completes.

Pagination callbacks receive a **`StateView`** of run-local `page_state` only.

Any direct write to this object raises `StateViewMutationError` immediately:

```python
def bad_callback(page_payload, state):
    state['counter'] = 1  # raises StateViewMutationError
```

To persist values across pages, return a dict from `update_state`:

```python
'update_state': lambda page_payload, state: {
    'rows_seen': state.get('rows_seen', 0) + len(page_payload.get('items', []))
}
```

**Callback ordering within one page:** `on_response` → `on_page` →
`update_state` → `next_request`.

The state snapshot is rebuilt after `update_state` runs, so `next_request`
sees updated counters. `on_response` and `on_page` see the pre-update snapshot.

`StateView` is a `dict` subclass — `isinstance(state, dict)` is `True`.
Nested dicts inside the view (for example `state['_request']`) are plain dicts;
the mutation guard covers top-level keys only.

Auth callbacks do not receive mutable shared state. Built-in auth handlers own their runtime cache internally. `CallbackAuth` and `BearerAuth.token_callback` read a read-only config view derived from `schema['state']`. Derived auth values such as fetched tokens or renewed session IDs do not write back into callback-visible state.

```python
auth.apply(request_kwargs)
next_request(parsed_body, StateView(page_state))
```

---

## Callback Contract

### Callback lifecycle

A paginated or non-paginated run uses the same callback structure:

- `on_response(page_payload, state)` transforms the parsed response into the current page value
- `on_page(page, state)` runs after the page value is produced
- `update_state(page_payload, state)` can return a dict merged into per-run page state
- `on_page_complete(outcome, state)` fires after the full page cycle (including any `on_error` resolution), before `next_request` is called — see [Adaptive throttling](#adaptive-throttling)
- `next_request(parsed_body, state)` returns request overrides for the next page, or `None` to stop
- `on_complete(...)` runs once on normal completion; its first argument depends on execution mode

### Override and suppression rules

Callback hooks are resolved by scope according to their own public contract:

- endpoint-level `on_request` replaces client-level `on_request`
- endpoint-level `on_request: None` suppresses client-level `on_request`
- endpoint-level `on_event` replaces client-level `on_event`
- built-in pagination helper callbacks such as `on_response` may be overridden
  by endpoint callbacks, and endpoint `on_response: None` suppresses the helper
  callback entirely
- `on_complete` runs once on clean normal completion only; it does not run for
  raised failures or `time_limit` expiry

The docs treat these rules as public contract. Lower-level implementation
details, such as exactly how helper callbacks are wired internally, are not
part of the public guarantee.

### Callback arguments

Callback arguments are part of the public contract. The important distinction is:

- `parsed_body` in `next_request(parsed_body, state)` is the canonical parsed
  payload for the current page before `response_parser`
- `page_payload` in `on_response` / `update_state` is the value after
  `response_parser`
- `page` is the value produced **after** `on_response`
- `state` is a read-only **`StateView`** snapshot, not a mutable working dict

More concretely:

- `on_response(page_payload, state)`
  - `page_payload`: parsed payload for the current request **after** `response_parser` (if configured)
  - `state`: `StateView` snapshot for the current page
  - returns: page value passed to `on_page`, buffered into fetch results, or yielded directly

- `on_page(page, state)`
  - `page`: current page value after `on_response`
  - `state`: same `StateView` shape as above
  - returns: ignored

- `update_state(page_payload, state)`
  - `page_payload`: parsed payload for the current request **after** `response_parser` (if configured)
  - `state`: pre-update `StateView` snapshot
  - returns: `dict` of keys to merge into per-run page state, or `None` (ignored)

- `next_request(parsed_body, state)`
  - `parsed_body`: canonical parsed body for the current request based on `response_format`, **before** `response_parser`
  - `state`: post-`update_state` `StateView` snapshot
  - returns: request override dict for the next page, or `None` to stop

- `on_complete(result, state)`
  - in `fetch()` mode: `result` is the aggregate fetch result that `fetch()` would otherwise return
  - in `stream()` mode: `result` is `StreamSummary(pages, requests, stop, endpoint, source, retries, elapsed_seconds, bytes_received, retry_bytes_received, ...)`
  - `state`: final `StateView` snapshot for the completed run

`on_complete` has different return-value semantics by mode:

- in `fetch()` mode, if `on_complete` returns a non-`None` value, that value
  becomes the final `fetch()` result
- in `fetch()` mode, if `on_complete` returns `None`, `fetch()` keeps the
  normal aggregate result
- in `stream()` mode, the `on_complete` return value is always ignored
  (streaming stays incremental)
- `on_complete` runs only on clean normal completion; it does not run for
  raised failures or `time_limit` expiry

For non-paginated runs, the same callback engine still applies: `response` is
still the parsed payload, `page` is still the post-`on_response` value, and a
successful run is treated as one completed page.

### Argument types and structures

There are two parsed values in play during a page:

- `parsed_body`: the canonical parse for the endpoint's configured
  `response_format` (JSON/XML/CSV/text/bytes). This is what
  `next_request(parsed_body, state)` receives.
- `page_payload`: the value after `response_parser` (if configured). This is
  what `on_response(page_payload, state)` and `update_state(page_payload, state)` receive.

The canonical `parsed_body` shapes include:

- JSON value (`dict`, `list`, `int`, `str`, `bool`, `None`, ...)
- `str` for `text`
- `xml.etree.ElementTree.Element` for `xml`
- `list[dict[str, str]]` for `csv`
- `bytes` for `bytes`

That means pagination callbacks operate on parsed payloads, not raw transport
objects. If you need HTTP headers or status codes, use
`state['_response_headers']` (includes `_status_code`).

Mock caveat: when using `mock=...`, there is no raw body to pre-parse. In that
case `parsed_body` is the same object as the mock payload and may reflect the
post-parser shape.

`state` is a `StateView`, which is a `dict` subclass with read-only top-level
mutation semantics. It can include:

- client-level values seeded from schema state
- per-run pagination values returned by `update_state`
- helper-managed transport keys such as `_response_headers` and `_request`
- observability keys such as `*_so_far` counters/timers and `stop`

The authoritative list and semantics for observability keys live in the
[Observability](#observability) section below.

`state['_request']` is the exact request dict used for the current response,
captured after auth injection. Its shape is:

- `method`: `str`
- `url`: `str`
- `params`: `dict | None`
- `headers`: `dict | None`
- `json`: request JSON body if present
- `data`: request form/body data if present

`state['_request']` is available during per-page callbacks. `on_complete`
receives cleaned final run-local state rather than transient transport/control
keys such as `_response_headers` and `_stop_signal`, and it does not include
`_request` because there is no current in-flight request anymore.

On clean normal stops, `state['stop']` may be present for observability.
See [Observability](#observability) for stop kinds and payload shape.

It is absent on natural completion and is not used for destructive duration
expiry, which raises `DeadlineExceeded` instead of completing normally.

### Callback ownership

Lifecycle hooks (`on_response`, `on_page`, `on_complete`, `on_page_complete`, `update_state`) live at endpoint level. They do not inherit from client level. Cross-cutting hooks (`on_error`, `on_event`, `on_event_kinds`, `on_request`) inherit from client level and can be overridden at endpoint level. Pagination config owns only mechanics: `next_request`, `delay`, `initial_params`.

Built-in pagination helpers include a convenience `on_response` that extracts items from the response. If an endpoint also defines `on_response` at endpoint level, the endpoint-level hook wins. Explicit `None` at endpoint level suppresses the helper-provided hook.

For the full ownership model, inheritance rules, and annotated examples, see `schema_guide.md`.

---

## Stop and Completion Semantics

### Safety caps

`fetch()` and `stream()` accept three optional keyword arguments that impose
limits on an operation regardless of the pagination or retry configuration:

- `max_pages`
- `max_requests`
- `time_limit`

The current contract is intentionally split:

- `max_pages` — **non-destructive** bounded stop
- `max_requests` — **non-destructive** bounded stop
- `time_limit` — **destructive** deadline abort

### Bounded page/request stops

`max_pages` is checked before fetching the next page. If page `N+1` would
exceed the cap, the run stops cleanly after page `N`.

`max_requests` is checked before each HTTP attempt. Retries count toward this
cap. If the next attempt would exceed the limit, the run stops cleanly with the
completed pages/results preserved.

For both of these caps:

- the run returns normally
- already completed pages/results are preserved
- `on_complete` still runs
- `state['stop']` is set to `{'kind': ..., 'limit': ..., 'observed': ...}`

For `fetch()`, `on_complete` sees the normal aggregate result.
For `stream()`, `on_complete` sees `StreamSummary(pages, requests, stop, endpoint, source, retries, elapsed_seconds, bytes_received, retry_bytes_received, ...)`. The summary fields are described in [Observability](#observability) and [Typing and Helper Objects](#typing-and-helper-objects).

If the same `on_complete` callable is reused across both modes, its first
argument changes shape:

- in `fetch()` mode: aggregate results/pages
- in `stream()` mode: `StreamSummary`

If you intentionally share one callback across both modes, branch on the first
argument type (for example `isinstance(result, StreamSummary)`).

`observed` means the counter value at the point the stop was decided:

- completed page count for `max_pages`
- HTTP attempt count for `max_requests`

### Deadline stop

`time_limit` remains exceptional. The deadline is checked before page fetches,
inside the retry/request path, and after retry/backoff / Retry-After sleeps.

When the deadline is hit:

- `DeadlineExceeded` is raised
- no normal completion result is returned
- `on_complete` does not run
- `state['stop']` is not used for this case

### Non-paginated endpoints

Non-paginated endpoints still run through the same callback engine and count as
one completed page when they succeed.

That means:

- `max_pages=0` stops cleanly before the request and yields/returns no page data
- `max_pages=1` or higher allows the request through
- `max_requests` applies normally to the request attempt count
- `time_limit` still raises on deadline exhaustion

### Cap interactions

All three caps share a single `OperationContext` per call. They are independent
counters checked at different points; whichever limit is hit first wins.

The non-obvious combination is `max_pages` with `max_requests`. Because
`max_requests` counts raw HTTP attempts (including retries), it can fire before
`max_pages` does. With `max_pages=10, max_requests=10` on an endpoint that
needs 2 retries per page, the request cap can stop the run after page 3 or 4 —
well short of 10 pages. This is correct behaviour: the two caps measure
different things.

### Streaming completion

`stream()` stays incremental even when `on_complete` is set.

The stream callback contract is therefore:

- pages are yielded one at a time as they arrive
- `on_complete(summary, state)` runs once on clean normal completion only
- `summary` is `StreamSummary(pages, requests, stop, endpoint, source, retries, elapsed_seconds, bytes_received, retry_bytes_received, ...)`
- `summary.requests` counts request attempts for the current source (`'live'` or `'playback'`)
- the callback return value is ignored in stream mode
- failures and deadline expiry do not call `on_complete`

This avoids the old fetch-like buffering behaviour where `stream()` had to
retain all pages just to call `on_complete`.

#### Stop reason at the stream call site

If you need the completion summary at the same level you iterate the stream
(without relying on callback closures), use `stream_run()`:

- `run = client.stream_run(...)` returns an iterable implementing the public `StreamRun` protocol
- iterating `run` yields pages just like `stream()`
- after normal exhaustion, `run.summary` is a `StreamSummary`
- if the endpoint schema already defines `on_complete`, it still runs normally;
  `stream_run()` captures the summary in addition to that callback running
- if iteration stops early or an exception aborts the run, `run.summary` stays `None`

### Fetch completion

`fetch()` remains aggregate-oriented:

- without `on_complete`, it returns the aggregate fetch result
- with `on_complete`, it passes the aggregate result to the callback
- if the callback returns a non-`None` value, `fetch()` returns that value
- if the callback returns `None`, `fetch()` keeps the aggregate result
- failures and deadline expiry do not call `on_complete`

If you need a final cross-page transform over all fetched pages/results,
`fetch()` is the natural place to do it.

### Choosing the right completion pattern

| Goal | Pattern | Completion payload | Memory profile |
| --- | --- | --- | --- |
| Final aggregate result over all pages | `fetch()` without `on_complete` | normal aggregate fetch result | full result in memory |
| Final transformed result over all pages | `fetch()` with `on_complete` | aggregate pages/results | buffers pages/results for completion |
| Incremental page processing only | `stream()` without `on_complete` | none | keeps one page at a time |
| Incremental streaming plus final stop/count summary | `stream()` with `on_complete` | `StreamSummary` | keeps one page at a time |

---

## Lifecycle Events

In addition to the data callbacks (`on_response`, `update_state`, `on_page`,
`next_request`), the client emits **lifecycle events** via `on_event` for
observability and telemetry.

- `on_event` is a side-channel for telemetry only. Never use it to drive
  application logic.
- If `on_event` raises, the exception is swallowed and logged once per run;
  the run continues.
- Events are emitted during both live runs and playback runs.

### PaginationEvent fields

Every event is a frozen `PaginationEvent` dataclass with these fields:

| Field | Type | Notes |
|---|---|---|
| `kind` | `str` | Discriminator — see event kinds below. |
| `source` | `'live' \| 'playback'` | Filter on this to exclude playback timings. |
| `ts` | `float` | Wall-clock timestamp (`time.time()`). |
| `mono` | `float` | Monotonic timestamp (`time.monotonic()`). |
| `endpoint` | `str \| None` | Endpoint name. |
| `url` | `str \| None` | Request URL. |
| `request_index` | `int \| None` | Reserved for future use; currently always `None`. |
| `attempt` | `int \| None` | Reserved for future use; currently always `None`. |
| `page_index` | `int \| None` | Reserved for future use; currently always `None`. |
| `data` | `dict \| None` | Kind-specific payload — see below. |

### Event kinds and data payloads

The event table below focuses on event-specific payload. Many events are also
enriched with shared run-local progress/timing fields documented in
[Observability](#observability).

| Kind | When emitted | `data` keys |
|---|---|---|
| `request_start` | Before the request cycle for a page/run path, prior to the retry loop. | `method` |
| `request_end` | After a request cycle ends for the current page/run path, including error paths. `elapsed_ms` is `None` if timing is unavailable. | `status_code`, `elapsed_ms`, `bytes_received` (int, `len(response.content)` for the final response body in the cycle; 0 for error/stop paths without a response body), `retry_bytes_received` (int, total response body bytes from retry attempts in the same cycle; 0 if no retry attempts produced a measurable body), plus shared progress/timing enrichment |
| `retry` | Before each retry sleep, after a retriable response or network error. | `reason` (e.g. `'status_429'`, `'network_error'`), `attempt`, `planned_ms` (final planned sleep before the next attempt), `retries_so_far` |
| `page_parsed` | After `canonical_parser` and `response_parser` have run for a page. | `status_code`, plus shared progress/timing enrichment |
| `stopped` | When pagination stops cleanly (`max_pages`, `max_requests`, `next_request` returned `None`, or `on_error` returned `'stop'`). | `stop_kind` (`'max_pages'`, `'max_requests'`, `'next_request_none'`, or `'error_stop'`), optional `observed` and `limit`, plus shared progress/timing enrichment |
| `callback_error` | When a data callback raises. The exception is wrapped in `CallbackError` and re-raised after this event fires. | `callback`, `exception_type`, `exception_msg` |
| `error` | When the run aborts due to an unexpected exception. Not emitted for `RateLimitExceeded` (which has its own `rate_limit_exceeded` event). | `exception_type`, `exception_msg` |
| `rate_limit_wait_start` | When the token bucket is empty and `on_limit='wait'` — just before sleeping. Live runs only. | `planned_ms`, `rps`, `burst` |
| `rate_limit_wait_end` | After a proactive or reactive rate-limit wait completes. | `wait_ms`, `planned_ms`, `wait_type`, plus wait-type-specific fields such as `rps`/`burst` for proactive waits and `cause` (`retry_after`/`min_delay`) for reactive waits |
| `adaptive_wait_end` | After an adaptive sleep requested by `on_page_complete` completes. Only emitted when the sleep duration is greater than zero. | `wait_ms`, plus shared progress/timing enrichment |
| `rate_limit_exceeded` | When the token bucket is empty and `on_limit='raise'` — just before raising `RateLimitExceeded`. Terminal. | `rps`, `burst` |

### Playback event notes

- Events fire with `source='playback'` during playback runs.
- `elapsed_ms` in playback `request_end` events is near-zero (local file read).
  Filter by `source` if timing is relevant.
- `retry` events can appear in playback if the fixture contains retriable
  responses (e.g. 429 followed by 200).
- Proactive rate limiting is bypassed in playback, so proactive
  `rate_limit_wait_start`, `rate_limit_exceeded`, and proactive
  `rate_limit_wait_end` payloads do not appear there. Reactive retry waits can
  still occur during playback and emit `rate_limit_wait_end` with
  `wait_type='reactive'`.

### Known event nuance

`request_start` is emitted once per page/run path before the retry loop, while
`requests_so_far` increments per actual attempt. This mismatch is intentional
and should be taken into account when reading event streams.

### on_event scope

`on_event` can be set at the client level or overridden per endpoint. An
endpoint-level `on_event` replaces (does not stack with) the client-level one.
It is a cross-cutting hook configured at client or endpoint scope.

`on_event_kinds` filters which event kinds reach `on_event`. It accepts `None` (all events), `'all'` (alias), a single kind string, a set/list of kind strings, or an empty collection (silences all events). Unknown kind strings raise `SchemaError`. It inherits from client to endpoint, same as `on_event`. See `schema_guide.md` for the full filter model.

---

## Playback

### Fixture format

Playback files store **raw response envelopes** rather than a pre-parsed result.
On load, each recorded response is reconstructed and fed through the normal
parsing and callback pipeline, so `response_parser`, `on_response`, `on_page`,
`on_complete`, and pagination callbacks all run as they would against a live
API. This is true for both paginated and non-paginated endpoints because both
use the same callback engine.

Playback fixtures distinguish **logical format** from **storage form**:

- `format` is the logical parse family (json/xml/csv/text/bytes)
- payload is stored as either:
  - `body: str` for readable text-backed fixtures, or
  - `content_b64: str` for raw-byte storage

If you copy fixtures outside the project tree, treat them as unsupported and
regenerate them from the current project state.

Envelope format (JSON, text-backed):

```json
{
  "kind": "raw_response",
  "format": "json",
  "status_code": 200,
  "headers": {"Content-Type": "application/json"},
  "url": "https://api.example.com/v1/items?page=1",
  "body": "{\"items\": [...]}"
}
```

Raw-byte storage uses a single-string `content_b64` field:

```json
{
  "kind": "raw_response",
  "format": "bytes",
  "status_code": 200,
  "headers": {"Content-Type": "application/octet-stream"},
  "url": "https://api.example.com/v1/blob",
  "content_b64": "AAECAwQ="
}
```

XML stored as readable `body` is treated as normalized UTF-8 text: playback
reconstructs bytes as UTF-8, and recording strips or rewrites any XML encoding
declaration to match. If you need exact original XML bytes/prolog, record as
raw bytes (`content_b64`).

**Playback files must contain raw response envelopes (`kind: "raw_response"`).**
That is the supported playback fixture format.

**Mock + playback:** save mode is not supported with `mock:` because raw
playback requires a real HTTP response to record.

Extra recorded responses are treated as an error. Playback load expects the
recorded envelope stream to match the execution flow; if recorded responses
remain after execution completes, playback raises an explicit `PlaybackError`
instead of silently ignoring them.

### Fixture scrubbing

Raw playback fixtures are scrubbed **at save time** before being written to
disk.

The built-in scrubber currently focuses on **request-side secrets** only:

- recorded request headers are scrubbed using the same header rules used for
  logging (`scrub_headers` extends the default exact-match set; built-in
  substring matching still applies)
- sensitive query parameter values in the recorded request URL are scrubbed
  using built-in defaults plus optional `scrub_query_params` extensions

The scrubber preserves fixture shape by replacing values with the literal
string `[REDACTED]` (request headers use `***` to match log output; URL query
values use `[REDACTED]` to be unambiguous in JSON). It currently covers
**only** recorded request headers and the recorded request URL. Response
headers, response bodies, and request bodies are **not** scrubbed by default.

If your API uses a generic sensitive query parameter such as `code`, add it
explicitly via `scrub_query_params=['code']`.

---

### Safety caps during playback

Safety caps (`max_pages`, `max_requests`, `time_limit`) are **not applied**
during playback replay (`load` mode, or `auto` mode when the fixture file
exists). This is intentional.

The caps exist to protect against runaway live APIs. Those concerns do not
apply to local file reads, which are deterministic and effectively immediate.

**Mode-by-mode behaviour:**

| Playback mode | Real HTTP? | Caps apply? |
|---|---|---|
| `load` | No — reads file | No |
| `auto` (file exists) | No — reads file | No |
| `auto` (file missing) | Yes — fetches + saves | Yes |
| `save` | Yes — fetches + overwrites | Yes |

The raw-response replay path still skips cap checks: playback does not call the
live `OperationContext` guards, and `PaginationRunner.run()` is invoked without
a live context in replay mode.

**Testing cap behaviour without a live API:** use a callable `mock` instead of
`playback`. A mock goes through the full pagination and retry stack, so caps
fire normally.

A playback config of `{'mode': 'none'}` disables playback entirely for that
endpoint and behaves as if no playback config had been supplied.

---

### Curated example fixtures

Some examples ship with companion raw playback fixtures under
`examples/fixtures/`. In those examples, playback is usually configured with
`mode: 'none'`; change it to `'load'` to try offline replay. Fixtures copied
outside the project tree are not supported and must be regenerated.

---
## Error Semantics

### Request error context

`RequestError` carries three attributes set by `_FetchJob` at the point of
failure, after auth has run:

- `.endpoint` — the endpoint name as passed to `client.fetch()`
- `.method` — the HTTP method of the actual request (post-auth)
- `.url` — the URL of the actual request (post-auth, so auth rewrites are reflected)

These are `None` for `RequestError` instances constructed directly. The
enrichment is idempotent — re-raising does not overwrite values already set.

### Retry semantics

Only `requests.RequestException` subclasses trigger retry:
`ConnectionError`, `Timeout`, `TooManyRedirects`, SSL errors, and so on.

Programming errors (`AttributeError`, `KeyError`, `TypeError`, etc.) bubble up
immediately on the first attempt — they are not retried.

Retry jitter is optional and applies only to computed backoff delays. `jitter: true` is treated as `"full"`; `"full"` uses `uniform(0, delay)` and `"equal"` uses `delay / 2 + uniform(0, delay / 2)`. Jitter is not applied to `Retry-After` or reactive `min_delay`. Callable `backoff` remains authoritative and cannot be combined with enabled `jitter`.

After all retry attempts are exhausted, a `RequestError` is raised with
`.cause` set to the original `requests.RequestException`.

### on_error return contract

`on_error` must return `'raise'`, `'skip'`, or `'stop'`. Returning `None` (for
example a callback that falls through without an explicit return) raises
`CallbackError` immediately.

- `'raise'` propagates the error
- `'skip'` suppresses the failing page and continues
- `'stop'` ends the run cleanly with pages already fetched preserved

```python
'on_error': lambda exc, state: 'skip' if exc.status_code == 404 else 'raise'
```

### StateViewMutationError

Raised when a pagination callback attempts to write to the `StateView` passed
as the `state` argument. The error message names the key and points to
`update_state(...)` as the persistence path.

---

## Typing and Helper Objects

`ResponseParser` is the public alias for response parser callbacks. Internally
the library still uses arity-specific helper aliases, but `ResponseParser` is
the user-facing name.

`StreamSummary` is the stream-mode completion payload. It exposes:

- `pages`
- `requests`
- `stop`
- `endpoint`
- `source`
- `retries`
- `elapsed_seconds`
- `retry_wait_seconds`
- `rate_limit_wait_seconds`
- `adaptive_wait_seconds`
- `static_wait_seconds`
- `total_wait_seconds`

`requests` counts actual attempts in both live and playback runs.

`stop` is either `None` or a `StopSignal` carrying `kind`, `limit`, and
`observed`. `limit` / `observed` are optional for non-cap stops such as
`next_request_none` and `error_stop`.

---

## Observability

Observability is exposed through three surfaces:

- callback-visible run-local state (`StateView`)
- lifecycle events emitted to `on_event`
- final `StreamSummary` in stream-mode completion

These surfaces overlap, but they do different jobs: callback state is for
inspecting the current run from inside callbacks, events are for telemetry, and
`StreamSummary` is the final rolled-up view after normal completion. For usage
patterns, see `examples/examples.py`.

### Callback-visible state

Callback-visible run-local state includes these reserved top-level keys:

- `requests_so_far`
- `pages_so_far`
- `retries_so_far`
- `elapsed_seconds_so_far`
- `proactive_waits_so_far`
- `proactive_wait_seconds_so_far`
- `reactive_waits_so_far`
- `reactive_wait_seconds_so_far`
- `adaptive_waits_so_far`
- `adaptive_wait_seconds_so_far`
- `errors_so_far`
- `bytes_received_so_far`
- `retry_bytes_received_so_far`
- `seconds_since_last_request`

Additional transport/control keys include `_request`, `_response_headers`, and
`stop` when a clean stop has been determined. These fields are read-only
through callback state.

Key timing and volume semantics:

- `elapsed_seconds_so_far` is derived from the stored run-start monotonic
  timestamp when state is exposed rather than eagerly updated everywhere
- `seconds_since_last_request` is `None` until a true previous request exists
- `requests_so_far` counts actual request attempts, including retries
- `bytes_received_so_far` accumulates `len(response.content)` for each response processed by the library, including error responses with a body; playback measures fixture-reconstructed content
- `retry_bytes_received_so_far` accumulates the same body-byte measure, but only for retry attempts in the current run. The initial attempt that triggered retry does not count; attempts 2+ in the same page cycle do. If no retries happen, the value remains `0`.

### Event enrichment

Existing lifecycle events are enriched in place rather than replaced with a new
event family. The shared enrichment model means event payloads often include
current progress/timing fields such as:

- `requests_so_far`
- `pages_so_far`
- `retries_so_far`
- `elapsed_seconds_so_far`
- wait counters/durations where relevant

See [Lifecycle Events](#lifecycle-events) for event timing and event-specific
payloads.

### Stop signals

The current stop kinds are:

- `max_pages`
- `max_requests`
- `next_request_none`
- `error_stop`

The stop payload is represented as `StopSignal` / `state['stop']` / summary
`stop` with the same basic shape:

```python
{'kind': 'max_pages' | 'max_requests' | 'next_request_none' | 'error_stop',
 'limit': int | None,
 'observed': int | None}
```

`limit` and `observed` are populated for cap-based stops and may be `None` for
non-cap stops.

### StreamSummary

`StreamSummary` is the final observability surface for normal stream
completion. It rolls up:

- `pages`
- `requests`
- `stop`
- `endpoint`
- `source`
- `retries`
- `elapsed_seconds`
- `retry_wait_seconds`
- `rate_limit_wait_seconds`
- `adaptive_wait_seconds`
- `static_wait_seconds`
- `total_wait_seconds`

`source` distinguishes live and playback completion, and `requests` counts attempts for that source rather than only live HTTP traffic. `StreamSummary` remains intentionally compact but may grow additively as observability evolves; prefer named attributes over positional assumptions.


### StreamSummary and MetricsSummary

`StreamSummary` is the canonical per-run observability surface for normal
stream completion. `MetricsSummary` is the additive session-level surface
produced by `client.metrics.summary()`.

For a single run in a fresh metrics session, the overlapping totals should tell
the same numeric story even though the schemas are different:

| `StreamSummary` | `MetricsSummary` |
| --- | --- |
| `pages` | `total_pages` |
| `requests` | `total_requests` |
| `retries` | `total_retries` |
| `bytes_received` | `total_bytes_received` |
| `retry_bytes_received` | `total_retry_bytes_received` |
| `total_wait_seconds` | `total_wait_seconds` |
| `elapsed_seconds` | `total_elapsed_seconds` |

The surfaces are intentionally not identical:

- `StreamSummary` carries run-specific context such as `stop`, `endpoint`, and
  `source`
- `MetricsSummary` adds session counters such as `total_runs` and
  `total_failed_runs`
- failed and abandoned runs still contribute their accumulated totals to the
  metrics session

### Client metrics session

When client schema enables `metrics`, each run exit records one terminal `StreamSummary` into a thread-safe `MetricsSession`.

Accepted forms:
- `metrics: True` — construct a default session
- `metrics: MetricsSession()` — use the supplied session directly
- `metrics: False` / `None` / omitted — disabled

`client.metrics.summary()` returns a frozen `MetricsSummary` snapshot with top-level additive totals only:
- `total_runs`
- `total_failed_runs`
- `total_requests`
- `total_retries`
- `total_pages`
- `total_bytes_received`
- `total_retry_bytes_received`
- `total_wait_seconds`
- `total_elapsed_seconds`

Recording rules:
- metrics are recorded once per run exit, not incrementally in the hot path
- raised runs still contribute their accumulated work totals
- abandoned generators count as failed runs
- `total_elapsed_seconds` is the sum of per-run elapsed durations and may exceed observed wall-clock time when runs overlap concurrently
- endpoint breakdowns and derived ratios are intentionally out of scope for V1

`MetricsSession.reset()` is an atomic flush-and-return operation: it returns the
pre-reset `MetricsSummary` snapshot and clears the session totals for future
runs. Runs that finish after the reset contribute to the fresh post-reset
state, not to the snapshot that was returned.

## Request execution order

For every HTTP attempt, the library executes the following steps in order:

1. **Request assembly** — `_build_initial_request` merges base URL, path, schema-level headers/params/body, and call-time overrides into a `request_kwargs` dict.
2. **`on_request`** — if configured, the hook receives the assembled `request_kwargs` and a read-only `StateView`. It must return the request dict to continue with.
3. **Auth** — `auth.apply(request_kwargs)` injects credentials. Auth sees the post-`on_request` request.
4. **Proactive rate limiting** — the token bucket acquires a slot. If `on_limit='raise'` and the bucket is empty, `RateLimitExceeded` is raised here without an HTTP attempt.
5. **HTTP send** (inside the retry loop):
   a. Deadline and `max_requests` checks via `OperationContext`.
   b. `requests.Session.request(...)` — the actual network call.
   c. On a retryable status code: `on_retry` hook fires, then `Retry-After` / `min_delay` backoff, then back to step 5a.
   d. On a non-retryable error or exhausted retries: `RequestError` / `RateLimitError` is raised.
6. **Response parsing** — `default_parse_response` (or `response_parser`) produces the parsed body.
7. **Per-page callbacks** — `on_response` → `on_page` → `update_state` → `on_page_complete` → `next_request`. State is rebuilt after `update_state`, so `next_request` sees updated values.
8. **`on_complete`** — fires once after the final page on normal completion (natural stop, `max_pages`, or `max_requests`). Not called on error or `time_limit` expiry.

**`on_error` placement:** `on_error` fires after all retry attempts are exhausted and the final error is about to propagate — not on each individual attempt. It receives the exception and a read-only state snapshot. It must return `'raise'`, `'skip'`, or `'stop'`. `'stop'` ends the run cleanly, `'skip'` suppresses the failing page and continues, and `'raise'` re-propagates the exception. For HTTP failures, `state['_response_headers']` contains the final HTTP error response headers in the same normalized shape used elsewhere, including `_status_code`; network failures do not populate it.

**`on_page_complete` placement:** fires after `on_error` has resolved the outcome for the current cycle. It does not fire when `on_error` returns `'raise'`. See [Adaptive throttling](#adaptive-throttling) for the full contract.

**`on_event` placement:** `on_event` is a passive observer and fires at various points throughout the pipeline (rate limit waits, retry attempts, page completion). It does not affect execution order.

## Pre-request shaping

`on_request(request_kwargs, state)` runs after the request is assembled from schema + call params and before auth is applied. It receives the raw request kwargs and read-only run-local state (`StateView(page_state)`). It must return the request dict to continue with; it may build a new dict or mutate the passed dict in place, as long as it returns it. Returning `None` is an error. On the first page, `state` contains only values seeded from `initial_state` and `initial_params`; on later pages, it also contains whatever `update_state` persisted from prior pages. At endpoint level, `on_request: None` suppresses any client-level `on_request` hook. Endpoint-level `retry` follows similar resolution semantics: omitted inherits client retry, `None` disables additional retry attempts for that endpoint while still honoring reactive rate-limit waits, and dict values merge over client retry config.

---

## Adaptive throttling

`on_page_complete` is a post-cycle hook for adaptive pacing. It fires after each complete page cycle — after all retries, after `on_response`/`on_page`/`update_state`, and after `on_error` has resolved the outcome — but before `next_request` is called.

### Hook signature

```python
def on_page_complete(outcome: PageCycleOutcome, state: StateView) -> float | None:
    ...
```

The hook returns an adaptive delay in seconds. `None` or `0` means no delay. A positive float causes the library to sleep for that many seconds before calling `next_request`. The library owns the sleep so that `time_limit` semantics are enforced and the wait is visible to observability.

Return value contract:

- `None` — no delay
- `0` — no delay
- positive `float` — sleep for that many seconds
- negative value — `CallbackError` raised immediately
- `bool` — `CallbackError` raised (explicit rejection; `True` would otherwise silently become `1.0`)
- non-numeric, non-None — `CallbackError` raised

If the hook raises, the exception propagates directly without wrapping, consistent with `on_error`.

### Firing semantics

The hook fires for every completed page cycle, including error outcomes that resolved back into loop control. Specifically:

- **success** — fires; return value may produce a delay
- **`on_error` → `'skip'`** — fires with `outcome.kind == 'skipped'`; return value may produce a delay
- **`on_error` → `'stop'`** — fires with `outcome.kind == 'stopped'` and `outcome.stop_signal` set; return value is validated but the sleep is skipped since the run is already terminating
- **`on_error` → `'raise'`** — does **not** fire; the exception propagates immediately

### PageCycleOutcome fields

`PageCycleOutcome` is a frozen dataclass exported from the public package surface:

```python
@dataclass(frozen=True)
class PageCycleOutcome:
    kind: Literal['success', 'skipped', 'stopped']
    status_code: int | None        # final HTTP status code for the cycle, if available
    error: Exception | None        # terminal error, if any; None on success
    stop_signal: StopSignal | None # set when the cycle resolved via stop semantics
    attempts_for_page: int         # total HTTP attempts for this page cycle
    cycle_elapsed_ms: float | None # wall-clock time from first attempt start to final response
```

Timing notes:

- `cycle_elapsed_ms` includes retries and retry/backoff waits inside the page cycle.
- `cycle_elapsed_ms` does not include later static pagination delay or adaptive delay.
- For a first-attempt success, `attempts_for_page == 1` and `cycle_elapsed_ms` reflects the single cycle.

Expected relationships:

- `kind == 'success'` normally implies `error is None` and `stop_signal is None`
- `kind == 'skipped'` normally implies `error is not None`
- `kind == 'stopped'` normally implies `stop_signal is not None`

### Interaction with `pagination.delay`

When both `delay` (static inter-page delay) and `on_page_complete` are configured, their effect is additive: total inter-page sleep is the sum of both. The implementation may choose whatever ordering is natural in the code path, and callers should not rely on a specific internal sleep sequence. `on_page_complete` does not receive the configured or already-applied static delay value.

Neither sleep runs for the final page (when `next_request` returns `None` or a stop signal is already set).

### Observability

Adaptive waits are tracked separately from proactive and reactive waits:

- `adaptive_waits_so_far` — counter in callback-visible state
- `adaptive_wait_seconds_so_far` — cumulative duration in callback-visible state
- `adaptive_wait_end` event — emitted after each non-zero adaptive sleep, with `wait_ms` and standard progress enrichment fields; never emitted for zero or `None` returns, and not emitted when termination is already decided so no adaptive sleep occurs

### Schema placement

`on_page_complete` is configured at endpoint level, alongside other lifecycle hooks such as `on_response`, `on_page`, `on_complete`, and `update_state`:

```python
'endpoints': {
    'events': {
        'pagination': cursor_pagination('cursor', 'meta.next_cursor'),
        'on_page_complete': my_hook,
    },
}
```

For non-paginated endpoints, `on_page_complete` is equally available — the single request is the cycle:

```python
'endpoints': {
    'simple': {
        'path': '/status',
        'on_page_complete': observe_cycle,
    },
}
```

`PageCycleOutcome` is importable from the top-level package:

```python
from rest_fetcher import PageCycleOutcome
```

For the full config ownership model — which keys live where and why — see `schema_guide.md`.
