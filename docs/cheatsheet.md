# rest_fetcher — Developer Cheatsheet

_Python ETL library for fetching data from REST APIs_

---

## 1. Module Hierarchy

| Module | Key Class / Function | Responsibility |
|---|---|---|
| `exceptions.py` | `RestFetcherError` + subclasses, `raise_()` | All exception types. Every other module imports from here. |
| `schema.py` | `validate()` `merge_dicts()` `resolve_endpoint()` | Schema validation, deep dict merge, resolves client + endpoint config into one dict. |
| `auth.py` | `build_auth_handler()` `BearerAuth` / `BasicAuth` / `OAuth2Auth` / `OAuth2PasswordAuth` / `CallbackAuth` | Auth handlers. OAuth2 caches token and auto-refreshes before expiry. |
| `retry.py` | `RetryHandler` `build_retry_handler()` | Retry with backoff, Retry-After header parsing, min delay between requests. |
| `pagination.py` | `CycleRunner` `build_cycle_runner()` 5 built-in strategies | Pagination loop orchestration. Built-in: offset, cursor, link_header, page_number, url_header. |
| `context.py` | `OperationContext` | Per-call safety caps. Tracks `started_at`, `request_count`, `page_count`. Holds `max_pages`, `max_requests`, `time_limit` limits. |
| `playback.py` | `PlaybackHandler` `build_playback_handler()` | Save real API responses to file and replay them. Three modes: auto, save, load. |
| `client.py` | `APIClient` `_FetchJob` `_StreamRunImpl` | Public entry point. `APIClient` owns session, auth, retry. `_FetchJob` runs one fetch operation. `_StreamRunImpl` is the wrapper returned by `stream_run()`. |
| `metrics.py` | `MetricsSession` `MetricsSummary` | Optional client-level cumulative metrics session. Thread-safe top-level totals across runs. |
| `types.py` | TypedDicts + `SchemaBuilder` | Optional typing layer. TypedDicts for IDE autocompletion and mypy. `SchemaBuilder` is a fluent builder that returns plain dicts. Zero runtime cost if unused. |
| `tests/` | ~600 tests (unit + scenario) | Full test suite. No network calls — all HTTP intercepted via mock. Run: `pytest -q` |
| `examples/` | Runnable scripts | `examples.py`, `atom_example.py`, `events_rate_limit_example.py`, `glpi_example.py`, `anthropic_example.py`, `booze_example.py`, `nager_example.py`, `whoisjson_example.py`, `case_studies/pcal_example.py`. Live-API scripts require real credentials. |

---

## 2. Object Hierarchy

**`APIClient`** — created once per API host, reused across calls. Owns the `requests.Session`, auth handler, and retry handler.

&nbsp;&nbsp;&nbsp;&nbsp;└─ **`_FetchJob`** — created per call by `fetch()`, `stream()`, or `stream_run()`. Never reused. Runs the full pipeline for one endpoint call.

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└─ **`CycleRunner`** — drives the page loop inside `_FetchJob`. Created from the resolved pagination config.

**`_StreamRunImpl`** — returned by `stream_run()`. Wraps the `_FetchJob` generator and exposes `run.summary` (`StreamSummary | None`) after exhaustion.

**`MetricsSession`** — optional, owned by `APIClient` when `metrics` is enabled. Accumulates top-level totals across runs. Thread-safe. Exposes `summary()` and `reset()`.

---

## 3. Schema Reference

> For the ownership model, inheritance rules, and annotated examples, see `schema_guide.md`.

### 3.1 Top-level (APIClient) keys

| Key | Description |
|---|---|
| `base_url` | **Required.** Base URL for all endpoints, e.g. `"https://api.example.com/v1"` |
| `auth` | Auth config dict. Keys: `type` (`bearer`\|`basic`\|`oauth2`\|`oauth2_password`\|`callback`) + type-specific keys. See §3.3. |
| `retry` | Retry config dict. Keys: `max_attempts` (default 3), `backoff` (`exponential`\|`linear`\|callable, default `exponential`), `on_codes` (default `[429,500,502,503,504]`), `base_delay` (default 1.0s), `max_delay` (default 120s), `jitter` (`false`\|`true`/`"full"`\|`"equal"`), `max_retry_after` (float, optional — raise `RateLimitError` instead of waiting when `Retry-After` exceeds this value), `reactive_wait_on_terminal` (bool, default `False` — honour `Retry-After` / `min_delay` even on non-retried terminal HTTP errors). Jitter applies only to computed backoff, not `Retry-After` or reactive `min_delay`. |
| `rate_limit` | Rate limit config dict. Controls both proactive token-bucket limiting and reactive Retry-After behaviour. See §3.1a. |
| `on_event` | `Callable(PaginationEvent) → None`. Lifecycle event hook for telemetry. Overridable per endpoint. Exceptions swallowed; run continues. |
| `on_event_kinds` | Filter which event kinds reach `on_event`. Accepts: `None` (all events), `'all'` (alias for `None`), single kind string, set/list/tuple of kind strings, or empty collection (silences all events). Unknown kinds raise `SchemaError`. Inherits from client to endpoint. |
| `pagination` | Client-level pagination defaults. Inherited by all endpoints unless overridden. |
| `headers` | Default headers applied to every request. Merged with endpoint-level headers. |
| `on_error` | `Callable(exc, state) → "raise"\|"skip"\|"stop"`. Returning `None` raises `CallbackError`. Global error handler. |
| `on_request` | `Callable(request_kwargs, state) → dict`. Pre-auth request shaping hook. Return the request dict to continue with. |
| `log_level` | `none` \| `error` \| `medium` \| `verbose`. Default: `medium`. |
| `response_parser` | 1-arg `(response) → any`: full control, no pre-decoding. 2-arg `(response, parsed) → any`: library pre-decodes per `response_format` first (JSON value, str, Element, CSV rows, bytes, or None for empty). Arity detected once at construction via `inspect.signature`. |
| `canonical_parser` | Optional: `Callable(content_bytes, context) → any`. Runs *instead of* built-in `response_format` parsing; return becomes `parsed_body` for `next_request`. Generic extension point for custom non-JSON parsing. See §3.1b. |
| `endpoints` | **Required.** Dict of `endpoint_name → endpoint config dict`. |
| `state` | Dict used in two ways: auth reads it as read-only config, and each run gets a copied seed for callback-visible state. Use for secrets, tokens, region config. Auth runtime caches do not live here. |
| `session_config` | Dict of `requests.Session` settings: `verify` (bool or CA bundle path), `cert` (client cert path or `(cert, key)` tuple), `proxies` (scheme → URL dict), `max_redirects` (int, default 30). Unrecognised keys raise `SchemaError`. |
| `scrub_headers` | List of extra header names (str) to redact in logs and playback fixtures. On top of built-in defaults (`Authorization`, `X-Api-Key`, `X-Auth-Token`, etc.) and pattern matching (any key containing: `token`, `secret`, `password`, `key`, `auth`). |
| `scrub_query_params` | List of extra query param names (str) to redact in recorded URLs in playback fixtures. On top of built-in defaults (`access_token`, `api_key`, `token`, `client_secret`, etc.) and pattern matching (any param containing: `token`, `secret`, `key`, `sig`, `auth`). |
| `timeout` | **IMPORTANT:** `requests` has no default timeout and will hang indefinitely without one. Always set this. Single float: applies to connect + read. Tuple `(connect, read)`: separate timeouts, e.g. `(5, 60)`. |
| `metrics` | Optional client metrics session. Accepts `True` (construct a default `MetricsSession`), `False` / `None` / omitted (disabled), or an explicit `MetricsSession()` instance. When enabled, `client.metrics.summary()` returns top-level cumulative totals and `client.metrics.reset()` atomically flushes-and-clears them. |
| `response_format` | Default response format for all endpoints. `auto` \| `json` \| `text` \| `xml` \| `csv` \| `bytes`. Default: `auto` (infer from Content-Type). Overridable per endpoint and per call. |
| `csv_delimiter` | Default CSV column separator for all endpoints. Single character. Default: `';'`. Overridable per endpoint and per call. |
| `encoding` | Default text-layer encoding for textual responses (`text`, `csv`). Codec name. Default: `'utf-8'`. Overridable per endpoint and per call. |

#### 3.1a `rate_limit` config

The `rate_limit` dict handles two concerns. All keys are optional; use only what you need.

**Proactive token-bucket limiting** — paces requests before they go out. `requests_per_second` and `burst` are both required when using the token bucket. Setting any token-bucket key requires both.

| Key | Default | Description |
|---|---|---|
| `strategy` | `'token_bucket'` | Only supported value. May be omitted. |
| `requests_per_second` | — | **Required for token bucket.** Sustained refill rate. |
| `burst` | — | **Required for token bucket.** Bucket capacity — max immediate requests when full. |
| `on_limit` | `'wait'` | `'wait'` sleeps until capacity is available. `'raise'` raises `RateLimitExceeded` immediately. |

**Reactive Retry-After / min-delay** — controls how the retry layer reacts after the server signals throttling.

| Key | Default | Description |
|---|---|---|
| `respect_retry_after` | `True` | Honour the `Retry-After` response header on 429/503. |
| `min_delay` | `0.0` | Minimum seconds between any two consecutive requests. |

`max_retry_after` now lives under `retry`, while `rate_limit` remains responsible for proactive token-bucket settings plus reactive pacing policy (`respect_retry_after`, `min_delay`). Client-level `rate_limit` is the default policy. Endpoint `rate_limit` fully replaces that policy for the endpoint; it is **not merged** with client-level settings. If you override `rate_limit` at endpoint level, you are responsible for the full desired policy for that endpoint. Set `rate_limit: None` at the endpoint level to opt out.

> **SchemaBuilder note:** `SchemaBuilder.rate_limit()` generates only reactive keys. Do not use it when you need token-bucket limiting — write the `rate_limit` dict by hand instead.

#### 3.1b `canonical_parser` context dict

The `context` argument passed to `canonical_parser(content_bytes, context)` contains:

| Key | Type | Description |
|---|---|---|
| `response_format` | `str` | The configured format string. |
| `csv_delimiter` | `str` | The configured delimiter. |
| `encoding` | `str` | The configured text-layer encoding. |
| `headers` | `dict` | Response headers. |
| `status_code` | `int` | HTTP status code. |
| `url` | `str` | Request URL. |
| `request_kwargs` | `dict` | Full request kwargs (post-auth). |

### 3.2 Endpoint-level keys

> All endpoint keys override their client-level equivalents. Omitted keys inherit from client.
> Keys that inherit: `headers`, `on_error`, `on_event`, `on_event_kinds`, `on_request`, `log_level`, `response_parser`,
> `response_format`, `csv_delimiter`, `encoding`, `scrub_headers`, `scrub_query_params`.
> Endpoint-only hooks (do not inherit): `on_response`, `on_page`, `on_complete`, `on_page_complete`, `update_state`.

| Key | Description |
|---|---|
| `method` | `GET` (default), `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`, `OPTIONS`. |
| `path` | URL path appended to `base_url`, e.g. `"/users"`. Omit to use `base_url` directly. |
| `params` | Default query parameters dict. Merged with call-time params. |
| `headers` | Endpoint-level headers. Deep-merged over client headers. |
| `body` | Request body dict. Sent as JSON. Merged with call-time body. |
| `form` | Form-encoded body dict. Sent as `application/x-www-form-urlencoded`. Mutually exclusive with `body`. Merged with call-time form. |
| `pagination` | Pagination config dict (mechanics only: `next_request`, `delay`, `initial_params`), or `None` to explicitly disable inherited pagination. |
| `on_response` | `Callable(page_payload, state) → data`. Called every page. Extracts data and/or checks API errors. Endpoint-level only. |
| `on_page` | `Callable(data, state) → None`. Side-effect hook per page (write to DB, push to queue, log progress). Fires after `on_response`, before `update_state`. Does NOT trigger buffering on its own — pages are not accumulated when `on_complete` is absent. Endpoint-level only. |
| `update_state` | `Callable(page_payload, state) → dict\|None`. Merges returned dict into page state after each page. `None` return is silently ignored. For non-paginated endpoints, shapes final state before completion hooks. Endpoint-level only. |
| `on_page_complete` | `Callable(outcome: PageCycleOutcome, state) → float\|None`. Post-cycle adaptive throttling hook. See §3.4a. Endpoint-level only. |
| `on_complete` | Mode-specific. **fetch() mode:** `Callable(results, state) → any`. All pages buffered in memory; `fetch()` returns the callback result. **stream() mode:** `Callable(StreamSummary, state)`. Pages are NOT buffered — stream stays incremental. `StreamSummary`: `.pages`, `.requests`, `.stop`, `.endpoint`, `.source`, `.retries`, `.elapsed_seconds`, `.retry_wait_seconds`, `.rate_limit_wait_seconds`, `.adaptive_wait_seconds`, `.static_wait_seconds`, `.total_wait_seconds`, `.bytes_received`, `.retry_bytes_received`. If sharing one callback across modes, branch on `isinstance(result, StreamSummary)`. Endpoint-level only. |
| `on_error` | `Callable(exc, state) → "raise"\|"skip"\|"stop"`. Returning `None` raises `CallbackError`. Overrides client-level `on_error`. |
| `on_event` | `Callable(PaginationEvent) → None`. Overrides client-level `on_event` for this endpoint only. |
| `on_event_kinds` | Overrides client-level `on_event_kinds` for this endpoint only. Same accepted shapes. |
| `on_request` | `Callable(request_kwargs, state) → dict`. Overrides client-level `on_request` for this endpoint only. Runs before auth. Set to `None` to suppress a client-level hook. |
| `response_parser` | Overrides client-level `response_parser` for this endpoint. Same 1-arg/2-arg semantics. |
| `canonical_parser` | Overrides client-level `canonical_parser` for this endpoint. Same signature. |
| `log_level` | Overrides client `log_level` for this endpoint only. |
| `debug` | bool. Logs full request/response at DEBUG level regardless of `log_level`. |
| `retry` | Retry config dict, or `None` to disable additional retry attempts for this endpoint while still honouring reactive rate-limit waits (inherits `max_retry_after` and other reactive keys from client retry config). Omitted inherits client retry. Dict values merge over client retry config. |
| `rate_limit` | Rate limit config dict (same structure as client-level). Fully replaces client-level `rate_limit` for this endpoint; not merged. Set `None` to opt out. |
| `playback` | Path string or dict `{path, mode, record_as_bytes}`. Modes: `auto`\|`save`\|`load`\|`none`. `record_as_bytes: true` forces raw-byte fixture storage without changing logical `response_format`. See §5. |
| `mock` | List of dicts (one per page), or `Callable(request_kwargs) → dict`. No real HTTP calls. |
| `response_format` | `auto` \| `json` \| `text` \| `xml` \| `csv` \| `bytes`. Overrides client-level for this endpoint. Also overridable at call time. |
| `csv_delimiter` | Single char. CSV column separator. Default `';'`. Overrides client-level. Also overridable at call time. |
| `encoding` | Text-layer codec name, e.g. `'utf-8'`, `'cp1251'`. Default `'utf-8'`. Overrides client-level. Also overridable at call time. |
| `scrub_headers` | `List[str]`. Additional header names to redact in logs and playback fixtures. Overrides (does not extend) client-level. |
| `scrub_query_params` | `List[str]`. Additional query param names to redact in recorded URLs. Overrides client-level. |
| `timeout` | Scalar seconds or `(connect, read)` tuple. Overrides client-level timeout for this endpoint only. |

### 3.3 Auth config

| type | Required keys |
|---|---|
| `bearer` | `token` (str) OR `token_callback` (callable(config) → str) |
| `basic` | `username` (str), `password` (str) |
| `oauth2` | `token_url`, `client_id`, `client_secret`. Optional: `scope`, `expiry_margin` (default 60s). |
| `oauth2_password` | `token_url`, `client_id`, `client_secret`, `username`, `password`. Optional: `scope`, `expiry_margin` (default 60s). Password grant flow for APIs requiring user identity (e.g. GLPI v2). |
| `callback` | `handler`: `callable(request_kwargs, config) → request_kwargs` |

### 3.4 Pagination config

Pagination config owns mechanics only. Lifecycle hooks live at endpoint level.

| Key | Description |
|---|---|
| `next_request` | **Required.** `Callable(parsed_body, state) → dict\|None`. Returns request overrides for next page, or `None` to stop. `parsed_body` is the canonical parsed body for the configured `response_format` (pre-`response_parser`). |
| `delay` | float. Seconds to sleep between page requests. Anti-spam. |
| `initial_params` | Dict injected into first request. Set automatically by built-in strategies. |

### 3.4a Endpoint lifecycle hooks

These hooks live at endpoint level. They do not inherit from client level. They fire for both paginated and non-paginated endpoints.

| Key | Description |
|---|---|
| `on_response` | `Callable(page_payload, state) → data`. Extracts page data from the parsed payload after `response_parser` (if configured). |
| `on_page` | `Callable(data, state) → None`. Side-effect hook per page. Fires after `on_response`, before `update_state`. Does NOT trigger buffering on its own. |
| `update_state` | `Callable(page_payload, state) → dict\|None`. Merges returned dict into page state after each page. `None` return is silently ignored. For non-paginated endpoints, shapes the final callback-visible state before completion hooks run. |
| `on_page_complete` | `Callable(outcome: PageCycleOutcome, state) → float \| None`. Post-cycle adaptive throttling hook. Fires after `on_error` resolves (not on `'raise'`), before `next_request`. Return seconds to sleep, `None`, or `0` for no delay. Negative or non-numeric return raises `CallbackError`. `bool` is explicitly rejected. Delays are additive with `pagination.delay`; callers should rely on the total delay effect, not a specific internal sleep order. |
| `on_complete` | Mode-specific — see §3.2. In `fetch()` mode: `on_complete(results, state) → final_result` where `results` is a list of page data (single-element for non-paginated). In `stream()` mode: `on_complete(summary, state)` with `StreamSummary`. |

### 3.5 Call-time keys

> Passed as keyword arguments to `fetch()` or `stream()`. Merged over schema defaults.

| Key | Description |
|---|---|
| `params` | Query parameters merged over endpoint-level params. |
| `headers` | Headers merged over endpoint-level headers. |
| `body` | Request body merged over endpoint-level body. |
| `form` | Form-encoded body dict. Mutually exclusive with `body`. |
| `path_params` | Dict of values interpolated into `{placeholder}` segments in the path template. e.g. `path="/holidays/{year}/{country}"` + `path_params={"year": 2026, "country": "US"}` → `/holidays/2026/US` |
| `max_pages` | `int \| None`. Non-destructive guardrail: run stops cleanly when page limit is reached, preserving already-fetched results. Non-paginated endpoints count as one page. Default: `None`. |
| `max_requests` | `int \| None`. Non-destructive guardrail: run stops cleanly when request limit is reached, preserving already-fetched results. Counts all HTTP attempts across all pages (first try + retries) — a page needing 3 retries costs 3. Default: `None`. |
| `time_limit` | `float \| None`. Raises `DeadlineExceeded` when `elapsed (time.monotonic()) >= limit`. Checked before each page/attempt AND after every retry sleep (Retry-After and backoff), so a `Retry-After: 60` response will not oversleep a tight deadline. Default: `None`. |
| `response_format` | Overrides endpoint/client `response_format` for this call. `auto` \| `json` \| `text` \| `xml` \| `csv` \| `bytes`. |
| `csv_delimiter` | Overrides endpoint/client `csv_delimiter` for this call. Single character. |
| `encoding` | Overrides endpoint/client `encoding` for this call. Text-layer codec name. |
| `scrub_headers` | Overrides endpoint/client `scrub_headers` for this call. `List[str]`. |
| `scrub_query_params` | Overrides endpoint/client `scrub_query_params` for this call. `List[str]`. |

> **Caps compose:** all three share one `OperationContext` per call. `max_pages` and `max_requests` are non-destructive — the run stops cleanly, preserving already-fetched data. In `fetch()` mode, `on_complete` can inspect `state['stop']`; in `stream()` mode, `on_complete` receives `StreamSummary(.stop)` instead of the page list. `time_limit` is destructive and raises `DeadlineExceeded` (`on_complete` does not run). `max_requests` and `max_pages` interact non-obviously: `max_requests` counts raw HTTP attempts including retries, so it can fire before `max_pages` if retries are frequent (e.g. `max_pages=10`, `max_requests=10`, 2 retries/page → stops after page 3). Typical pattern: one backstop (`max_pages`) plus one SLA enforcer (`time_limit`).

> **`stream_run()` accepts the same caps** (`max_pages`, `max_requests`, `time_limit`) as `stream()`. All three are popped from `**call_params` before the job is created.

### 3.6 `params_mode` in `next_request`

> `next_request` can include `params_mode: "replace"` to clear all accumulated query params instead of merging. Default is `"merge"` (sticky params carry forward between pages). Built-in helpers intentionally omit `params_mode` when they keep the default merge behavior and only emit it for `"replace"`. Invalid values now raise instead of silently falling back to merge.

```python
return {'params': {'page': 2}}                           # merge: prior params survive
return {'params': {'page': 2}, 'params_mode': 'replace'} # replace: prior params cleared
return {'params_mode': 'replace'}                         # replace with no params: all cleared
```

### 3.7 Validation behavior

`validate()` defaults to `strict=True`. Unknown keys raise `SchemaError` with all issues collected and reported together. Each message includes the full list of allowed keys and a typo suggestion when the key is close enough to a known one. Pass `strict=False` to emit `SchemaValidationWarning` instead — useful during development when adding experimental keys. `SchemaValidationWarning` can be filtered with Python's standard `warnings` module. Type errors, missing required fields, and other semantic checks always fail immediately regardless of `strict`.

### 3.8 Notes and gotchas

- `total_path` and `total_pages_path` use a built-in dot-path resolver. Stop priority: (1) resolves to numeric → compare and stop when offset ≥ total; (2) resolves but non-numeric → stop immediately with warning (no fallback); (3) not configured → short-page heuristic (`len(items) < limit`). Inject `path_resolver` for advanced traversal: `offset_pagination(total_path='pages.0.count', path_resolver=my_resolver)`. Resolver signature: `(resp, path) → Any | None`.
- `next_request` returning `None` is the only stop signal — no separate `has_next` needed.
- Lifecycle hooks (`on_response`, `on_page`, `on_complete`, `on_page_complete`, `update_state`) live at endpoint level only. Placing them inside a `pagination` dict raises `SchemaError` with a redirecting message.
- Built-in helpers include a helper-provided `on_response`. Endpoint-level `on_response` overrides it when both are present.
- `schema['state']` is one source consumed in two ways: auth reads it as read-only config, and each pagination run gets a copied `page_state` seed. Pagination callbacks receive `StateView(page_state)` only; auth runtime state stays internal to handlers.
- Pagination callbacks receive a `StateView` (read-only dict subclass). Direct mutations raise `StateViewMutationError` immediately. Use `update_state` to persist values into `page_state`. Callback order per page: `on_response` → `on_page` → `update_state` → `on_page_complete` → `next_request`. State is rebuilt after `update_state`, so `next_request` sees updated counters. Read request params via `state['_request']['params']` (captured post-auth).
- `_response_headers` is injected into `page_state` automatically each page — includes all HTTP headers plus `_status_code` (int). On `on_error`, HTTP failures expose the final error response headers in the same shape; network errors do not fabricate them.
- Empty body responses (HTTP 204, or any response with no body) are parsed to `None` rather than raising an error.
- `on_complete` is mode-specific: in `fetch()` mode all pages are buffered in memory and `on_complete` receives the aggregate result; in `stream()` mode no pages are buffered and `on_complete` receives `StreamSummary`. The accompanying `state` is cleaned final run-local state, not transient transport/control keys such as `_response_headers` or `_stop_signal`. `on_page` alone does not buffer — use it for streaming side-effects without `on_complete`. See §Stop and Completion Semantics in `semantics.md` for the full decision table.

---

## 4. Callback Reference

| Callback | Signature | Returns |
|---|---|---|
| `on_response` | `(page_payload: any, state: dict) → any` | Transformed page data. Called every page. `page_payload` is the parsed payload after `response_parser` (if configured). `None` indicates an empty-body response (e.g. HTTP 204). `state["_response_headers"]["_status_code"]` carries the HTTP status code. |
| `next_request` | `(parsed_body: any, state: dict) → dict \| None` | Request overrides for next page. `None` stops pagination. `parsed_body` is the canonical parse for `response_format` **before** `response_parser`. With `mock=...`, `parsed_body` is whatever the mock returned. |
| `update_state` | `(page_payload: any, state: dict) → dict \| None` | Merged into page state after each page. `None` return is silently ignored. Use to carry info across pages. |
| `on_page` | `(data: any, state: dict) → None` | Side effect per page. Return value ignored. Does NOT buffer on its own — only `on_complete` triggers buffering. |
| `on_complete` | **fetch():** `(results, state: dict) → any`  **stream():** `(summary: StreamSummary, state: dict) → ignored` | **fetch() mode:** called once with aggregate results; return value becomes the `fetch()` return value. All pages buffered in memory before this runs. **stream() mode:** called once with `StreamSummary(.pages, .requests, .stop, .endpoint, .source, .retries, .elapsed_seconds, .retry_wait_seconds, .rate_limit_wait_seconds, .adaptive_wait_seconds, .static_wait_seconds, .total_wait_seconds, .bytes_received, .retry_bytes_received)`; return value ignored; pages are NOT buffered. `StopSignal`: `.kind`, `.limit`, `.observed` (with `limit` / `observed` optional for non-cap stops). |
| `on_error` | `(exc: Exception, state: dict) → str \| None` | `"raise"`, `"skip"`, or `"stop"`. Returning `None` raises `CallbackError`. `"skip"` suppresses the page and continues. `"stop"` ends the run cleanly. `"raise"` re-propagates. For HTTP failures, `state['_response_headers']` contains the final error response headers plus `_status_code`; network failures do not populate it. |
| `on_page_complete` | `(outcome: PageCycleOutcome, state: dict) → float \| None` | Post-cycle adaptive pacing hook. Return seconds to sleep (positive float), `None`, or `0` for no delay. Fires after `on_error` resolves; does not fire on `'raise'`. Sleep is skipped when stop is already decided. `PageCycleOutcome` fields: `kind`, `status_code`, `error`, `stop_signal`, `attempts_for_page`, `cycle_elapsed_ms`. |
| `on_event` | `(event: PaginationEvent) → None` | Telemetry only. Exceptions are swallowed; the run continues. See §4a for event kinds. |

> **Event nuance:** `request_start` fires once per page before the retry loop and carries only `method` — no progress/timing enrichment. Use `requests_so_far` in `request_end` for per-attempt counting. `adaptive_wait_end` is not emitted when termination is already decided, even if the hook returned a positive delay.
| `on_request` | `(request_kwargs: dict, state: dict) → dict` | Pre-auth request shaping hook. Return the request dict to continue with. You may build a new dict or mutate the passed dict in place, as long as you return it. At endpoint level, `None` suppresses a client-level hook. |
| `response_parser` (1-arg) | `(response: requests.Response) → any` | Full control — library does no pre-decoding. Use when raw bytes, full response access, or custom format handling is needed. |
| `response_parser` (2-arg) | `(response, parsed: any \| None) → any` | Library pre-decodes per `response_format` (JSON value, str, Element, CSV rows, bytes, or `None` for empty body) then calls parser with both. Arity detected at construction: 2-arg if `*args` or ≥ 2 positional params (including defaulted). |
| `canonical_parser` | `(content_bytes: bytes, context: dict) → any` | Replaces built-in `response_format` parsing. Return becomes `parsed_body`. See §3.1b for `context` keys. |
| `auth handler` (callback type) | `(request_kwargs: dict, config: Mapping) → dict` | Returns modified request kwargs with auth injected. `config` is the read-only auth config view. |
| `token_callback` (bearer auth) | `(config: Mapping) → str` | Returns bearer token string. Use for dynamic/rotated tokens. |
| `mock` (callable form) | `(request_kwargs: dict, *, run_state) → dict` | Returns fake response dict. `run_state` is passed as a keyword argument — declare it explicitly (`def mock(req, *, run_state): ...`) or accept it via `**kwargs`. List form: one dict per page, no callable needed. |

### 4a. Lifecycle event kinds (`on_event`)

Every `PaginationEvent` has base fields: `kind`, `source` (`'live'`/`'playback'`), `ts`, `mono`, `endpoint`, `url`, and `data` (kind-specific dict). Correlation fields `request_index`, `attempt`, `page_index` are reserved and currently always `None`.

`PaginationEvent.to_dict()` returns a JSON-serializable dict of all fields except `mono` (process-local, meaningless outside the run). The key set is stable across all event kinds — `None` fields are included.

```python
import json

def on_event(ev):
    print(json.dumps(ev.to_dict()))   # ETL audit log, one line per event
```

| Kind | When emitted | `data` keys |
|---|---|---|
| `request_start` | Before the request cycle for a page/run path, prior to the retry loop. | `method` |
| `request_end` | After each request cycle (including retries) ends. | `status_code`, `elapsed_ms`, `bytes_received` (final response body bytes for the cycle), `retry_bytes_received` (sum of response body bytes from retry attempts in the cycle), `requests_so_far`, `elapsed_seconds_so_far`, plus all other `*_so_far` progress fields |
| `retry` | Before each retry sleep. | `reason`, `attempt`, `planned_ms` (ms until next attempt), `retries_so_far` |
| `page_parsed` | After parsing completes for a page. | `status_code`, `pages_so_far` (count including this page), `requests_so_far`, plus other progress fields |
| `stopped` | When pagination stops (cap, `next_request → None`, or `on_error → 'stop'`). | `stop_kind` (`'max_pages'`/`'max_requests'`/`'next_request_none'`/`'error_stop'`); `observed`, `limit` present for cap stops; `stop` (dict with `kind`/`limit`/`observed`), `pages_so_far`, `requests_so_far`, `elapsed_seconds_so_far` |
| `callback_error` | When a data callback raises (before re-raise). | `callback`, `exception_type`, `exception_msg` |
| `error` | When the run aborts unexpectedly. Not emitted for `RateLimitExceeded`. | `exception_type`, `exception_msg` |
| `rate_limit_wait_start` | Before a token-bucket sleep (live only). | `planned_ms`, `rps`, `burst` |
| `rate_limit_wait_end` | After a proactive or reactive rate-limit wait. | `wait_type` (`'proactive'`/`'reactive'`), `wait_ms`, `planned_ms`, `rps`, `burst` (proactive only), `cause` (`'retry_after'`/`'min_delay'` on reactive waits); cumulative wait counters for the emitted wait type |
| `adaptive_wait_end` | After an `on_page_complete` adaptive sleep (only when > 0). Not emitted when the run is already terminating and no adaptive sleep occurs. | `wait_ms`, `adaptive_waits_so_far`, `adaptive_wait_seconds_so_far`, plus standard progress fields |
| `rate_limit_exceeded` | When `on_limit='raise'` triggers. Terminal. | `rps`, `burst` |

Rate-limit events (`rate_limit_wait_*`, `rate_limit_exceeded`) are not emitted during playback.

### Using `raise_()` in callbacks

`raise_(msg)` raises `ResponseError` by default. Pass `cls=` to change exception type.

```python
'on_response': lambda resp, state: raise_(resp['error']) if 'error' in resp else resp['data']
'on_response': lambda resp, state: raise_(resp['error'], cls=AuthError) if 'error' in resp else resp['data']
```

---

## 5. Playback (Save / Replay)

| Mode | Behaviour |
|---|---|
| `auto` (default) | File exists → replay from file. File missing → fetch real API and save. Delete file to force refresh. |
| `save` | Always fetch real API and overwrite file. Use to force a fresh recording. |
| `load` | Always replay from file. Raises `PlaybackError` if file is missing. |
| `none` | Disables playback entirely — behaves as if no `playback` block was set. |

```python
'playback': 'fixtures/campaigns.json'                                             # shorthand — auto mode
'playback': {'path': 'fixtures/campaigns.json', 'mode': 'save'}                  # explicit mode
'playback': {'path': 'fixtures/campaigns.json', 'mode': 'save', 'record_as_bytes': True}  # store raw bytes
```

- Responses are stored as a JSON array of `raw_response` envelopes — one per pagination page. Each envelope records the full wire response (status, headers, body text) and is replayed through the normal parsing/callback pipeline on load.
- Playback is per-endpoint. Different endpoints can have independent fixture files.
- In `auto` mode, first run hits the real API and saves. Every subsequent run replays instantly.
- Safety caps (`max_pages`, `max_requests`, `time_limit`) are **not applied** during replay. To test cap behaviour offline, use a callable `mock=` instead (mock goes through the full pagination/retry stack).

---

## 6. Built-in Pagination Strategies

### `offset_pagination`

```python
from rest_fetcher import offset_pagination

'pagination': offset_pagination(
    offset_param='offset', limit_param='limit',
    limit=100, data_path='items', total_path='total'
)
```

`total_path` optional — if omitted, stops when returned items < limit (short-page heuristic). Legacy alias: `total_key`.

### `cursor_pagination`

```python
from rest_fetcher import cursor_pagination

'pagination': cursor_pagination(
    cursor_param='cursor',
    next_cursor_path='meta.next_cursor',
    data_path='items'
)
```

`next_cursor_path` is dot-separated, e.g. `"meta.next_cursor"` resolves `parsed_body["meta"]["next_cursor"]`.

### `page_number_pagination`

```python
from rest_fetcher import page_number_pagination

'pagination': page_number_pagination(
    page_param='page', page_size=100,
    data_path='results', total_pages_path='total_pages'
)
```

`total_pages_path` optional — if omitted, stops when returned items < `page_size` (short-page heuristic). Legacy alias: `total_pages_key`.

### `link_header_pagination`

`params_mode` does not apply here because this helper returns a full `url` override rather than parameter patch semantics.


```python
from rest_fetcher import link_header_pagination

'pagination': link_header_pagination(data_path='items')
```

Parses RFC 5988 Link header. Looks for `rel="next"`. Used by GitHub API and others.

---


### `url_header_pagination`

```python
from rest_fetcher import url_header_pagination

'pagination': url_header_pagination('X-Next-Url', data_path='items')
```

Uses a named response header whose value is already the next URL. Header lookup is case-insensitive. If the header is absent or empty, pagination stops normally. Unlike `link_header_pagination`, this helper does not parse RFC 5988 `Link` metadata; it treats the header value as the next URL directly.

---

## 7. SchemaBuilder and IDE-friendly authoring

`SchemaBuilder` produces a plain dict identical to hand-written schemas. It is a developer-experience layer — zero runtime difference. All TypedDicts in `rest_fetcher.types` are available for IDE autocompletion and mypy.

For rich key suggestions in VS Code / Pylance, choose one of:

- **`SchemaBuilder`** — fluent API with typed method signatures.
- **TypedDict annotation** — annotate a raw dict literal with `ClientSchema` for top-level config and `EndpointSchema` for individual endpoints. Untyped inline dicts usually won't provide key suggestions.

```python
from rest_fetcher.types import ClientSchema, EndpointSchema

schema: ClientSchema = {
    'base_url': 'https://api.example.com/v1',
    'endpoints': {
        'users': EndpointSchema(method='GET', path='/users'),
    },
}
```

```python
from rest_fetcher import SchemaBuilder

schema = (
    SchemaBuilder('https://api.example.com/v1')
    .bearer('my-token')       # or .basic(), .oauth2(), .oauth2_password(), .auth_callback()
    .timeout(30)              # always set — requests has no default timeout
    .retry(max_attempts=3)
    .state(api_key='secret')  # read-only auth config + initial run-state seed
    .scrub_headers('X-Tenant-Id')
    .endpoint('users', method='GET', path='/users', params={'active': True})
    .endpoint('create', method='POST', path='/users')
    .build()                  # returns plain dict
)

client = APIClient(schema)    # identical to passing a hand-written dict
```

| Method | Description |
|---|---|
| `.bearer(token \| callable)` | Bearer auth. Pass str for static token, `callable(config)→str` for dynamic. |
| `.basic(username, password)` | HTTP Basic auth. |
| `.oauth2(token_url, client_id, client_secret)` | OAuth2 client credentials. Optional: `scope`, `expiry_margin`. |
| `.oauth2_password(token_url, client_id, client_secret, username, password)` | OAuth2 password grant. Optional: `scope`, `expiry_margin`. For APIs requiring user identity alongside client credentials (e.g. GLPI v2). |
| `.auth_callback(handler)` | Fully custom auth: `handler(request_kwargs, state) → request_kwargs`. |
| `.timeout(n \| (connect, read))` | Always set. Single float or `(connect, read)` tuple. |
| `.session_config(verify, cert, proxies, max_redirects)` | Configure `requests.Session`: TLS verification, client certs, proxies, redirect limit. All params optional. |
| `.retry(max_attempts, backoff, on_codes, base_delay=None, max_delay=None, jitter=False, max_retry_after=None)` | Retry config with defaults. `max_retry_after` raises `RateLimitError` when `Retry-After` exceeds this value. Callable `backoff` remains valid, but combining it with enabled `jitter` is rejected during schema validation. |
| `.rate_limit(min_delay, respect_retry_after)` | Reactive rate-limit settings only (Retry-After and min-delay). **Does not configure token-bucket limiting** — build the `rate_limit` dict by hand for that. |
| `.headers(**kwargs)` | Default headers applied to every request. |
| `.log_level(level)` | `none` \| `error` \| `medium` \| `verbose`. |
| `.state(**kwargs)` | Seed read-only auth config and per-run initial callback state. |
| `.scrub_headers(*names)` | Extra header names to redact in logs / fixtures. |
| `.scrub_query_params(*names)` | Extra query param names to redact in recorded URLs in playback fixtures. |
| `.endpoint(name, *, method, path, **kwargs)` | Add endpoint. All `EndpointSchema` keys accepted as kwargs. |
| `.pagination(config)` | Client-level pagination defaults inherited by all endpoints. |
| `.build()` | Returns plain dict. Pass to `APIClient`. Returns independent copy. |

---

## 8. Usage Examples

### 8.1 Simple GET, bearer auth

```python
from rest_fetcher import APIClient

client = APIClient({
    'base_url': 'https://api.example.com/v1',
    'auth': {'type': 'bearer', 'token': 'my-token'},
    'endpoints': {
        'whoami': {'method': 'GET', 'path': '/me'}
    }
})

profile = client.fetch('whoami')
```

### 8.2 Paginated GET, cursor strategy

```python
client = APIClient({
    'base_url': 'https://api.example.com/v1',
    'auth': {'type': 'bearer', 'token': 'my-token'},
    'endpoints': {
        'list_events': {
            'method': 'GET', 'path': '/events',
            'params': {'page_size': 200},
            'pagination': cursor_pagination('cursor', 'meta.next_cursor', 'items')
        }
    }
})

all_events = client.fetch('list_events')          # all pages in memory
for page in client.stream('list_events'):          # or stream page by page
    save_to_warehouse(page)
```

### 8.3 POST with client-level pagination defaults

> Endpoints inherit client-level pagination. Override only what differs per endpoint.
> Set `pagination: None` on an endpoint to explicitly disable inherited pagination.

```python
client = APIClient({
    'base_url': 'https://api.direct.yandex.com/json/v5',
    'auth': {'type': 'bearer', 'token': 'yandex-token'},
    'pagination': {
        'next_request': lambda parsed_body, state: (
            {'json': {'params': {'Page': {'Offset': parsed_body['LimitedBy']}}}}
            if parsed_body.get('LimitedBy') else None
        ),
        'delay': 0.1,
    },
    'endpoints': {
        'campaigns': {
            'method': 'POST', 'path': '/campaigns',
            'body': {'method': 'get', 'params': {
                'SelectionCriteria': {'Statuses': ['ACCEPTED']},
                'FieldNames': ['Id', 'Name'], 'Page': {'Limit': 10000}
            }},
            # on_response is an endpoint-level lifecycle hook, not a pagination key
            'on_response': lambda resp, state: resp.get('Campaigns'),
        },
        'reports': {'method': 'POST', 'path': '/reports', 'pagination': None},
    }
})
```

### 8.4 Call-time overrides

```python
results  = client.fetch('search', params={'limit': 50, 'q': 'python'})
report   = client.fetch('reports', body={'date_from': '2025-01-01'})

# path_params — fills {placeholders} in path template
# schema path: '/PublicHolidays/{year}/{country}'
holidays = client.fetch('holidays', path_params={'year': 2026, 'country': 'US'})
```

### 8.5 Stateful custom pagination

> Use `update_state` to persist values into `page_state`. Direct mutation raises `StateViewMutationError`. Callback order: `on_response` → `on_page` → `update_state` → `next_request`. `next_request` sees the post-update snapshot. Read current params via `state['_request']['params']` (post-auth).

```python
'pagination': {
    'next_request': lambda parsed_body, state: (
        None if
        (state.get('_request') or {}).get('params', {}).get('offset', 0)
        + len(parsed_body.get('data', [])) >= parsed_body.get('total_rows', 0)
        else {'params': {'offset':
            (state.get('_request') or {}).get('params', {}).get('offset', 0)
            + len(parsed_body.get('data', []))
        }}
    ),
    'on_response': lambda resp, state: resp.get('data', []),
}
```

### 8.6 Context manager (recommended for Airflow tasks)

```python
with APIClient(schema) as client:
    data = client.fetch('endpoint')
    # session closed automatically even if exception fires
```

### 8.7 Playback — record then replay

```python
'endpoints': {
    'campaigns': {
        # ...
        'playback': 'fixtures/campaigns.json'  # auto: save first run, replay after
    }
}
```

### 8.8 Mock for unit tests

```python
'endpoints': {
    'list_users': {
        'pagination': cursor_pagination('cursor', 'next_cursor'),
        'mock': [
            {'items': [{'id': 1}, {'id': 2}], 'next_cursor': 'p2'},
            {'items': [{'id': 3}], 'next_cursor': None},
        ]
    }
}

pages = list(client.stream('list_users'))
# [[{'id':1},{'id':2}], [{'id':3}]] — no HTTP calls made
```

### 8.9 Retry, rate limiting, and error handling

```python
client = APIClient({
    'base_url': 'https://api.example.com',
    'retry': {
        'max_attempts': 3,
        'backoff': 'exponential',
        'on_codes': [429, 500, 502, 503],
    },
    'rate_limit': {
        # proactive: pace outgoing requests
        'requests_per_second': 5.0,
        'burst': 10,
        'on_limit': 'wait',
        # reactive: honour server throttling signals
        'respect_retry_after': True,
        'min_delay': 0.2,
    },
    'endpoints': {
        'fragile': {
            'on_error': lambda exc, state: 'skip' if exc.status_code == 404 else 'raise'
        }
    }
})
```

### 8.10 Lifecycle events and per-endpoint rate-limit override

```python
from rest_fetcher import APIClient

def on_event(ev):
    if ev.kind == 'request_end' and ev.source == 'live':
        metrics.record(ev.endpoint, ev.data['elapsed_ms'])

client = APIClient({
    'base_url': 'https://api.example.com',
    'on_event': on_event,
    'rate_limit': {
        'requests_per_second': 2.0,
        'burst': 5,
    },
    'endpoints': {
        'users': {
            'method': 'GET', 'path': '/users',
            'rate_limit': {'requests_per_second': 10.0, 'burst': 20},  # endpoint override
        },
        'health': {
            'method': 'GET', 'path': '/health',
            'rate_limit': None,   # opt out entirely
        },
    },
})
```

See `examples/events_rate_limit_example.py` for a runnable version using a fake clock/sleep.

### 8.11 `stream_run()` — inspect completion summary at the call site

Use `stream_run()` when you need the completion summary at the same level you iterate the stream, without relying on an `on_complete` callback closure. The return type is `StreamRun`, which is importable from the package root for typed callers.

- `run.summary` is a `StreamSummary` after normal exhaustion (natural end or non-destructive cap).
- `run.summary` stays `None` if you break out early or an exception aborts the run.
- If the schema already defines `on_complete`, it still runs normally; `stream_run()` captures the summary in addition to that callback running.

```python
run = client.stream_run('list_events', max_pages=50)

for page in run:
    save_to_warehouse(page)

# after exhaustion
summary = run.summary
if summary is None:
    # broke out early or an exception aborted the run
    raise RuntimeError('stream did not complete normally')

if summary.stop is not None:
    print(f'stopped: {summary.stop.kind} '
          f'(limit={summary.stop.limit}, observed={summary.stop.observed})')

print(f'pages={summary.pages}, requests={summary.requests}')
```

`StreamSummary` fields: `.pages` (int), `.requests` (int, counts actual attempts in both live and playback runs), `.stop` (`StopSignal | None`), `.endpoint` (`str | None`), `.source` (`'live' | 'playback'`), `.retries` (int), `.elapsed_seconds` (float), `.retry_wait_seconds`, `.rate_limit_wait_seconds`, `.adaptive_wait_seconds`, `.static_wait_seconds`, `.total_wait_seconds`, `.bytes_received` (int, total response body bytes processed), `.retry_bytes_received` (int, total response body bytes from retry attempts only). Fields may grow additively over time; prefer named attribute access. `StopSignal` fields: `.kind` (`'max_pages'`, `'max_requests'`, `'next_request_none'`, or `'error_stop'`), `.limit` (`int | None`), `.observed` (`int | None`).

### 8.12 `fetch_pages()` — always a list, stream semantics

`fetch_pages()` is `list(stream(...))` materialized. Unlike `fetch()`, it always returns a list regardless of page count — `fetch()` unwraps single-page results to the bare value; `fetch_pages()` never does.

```python
# fetch() returns dict for one page, list for many — shape varies with data volume
result = client.fetch('users')        # dict if one page, list[dict] if many

# fetch_pages() is always a list — predictable shape for downstream code
pages = client.fetch_pages('users')   # always list, even for a single page
```

`on_complete` in `fetch_pages()` follows stream semantics: it fires with `(StreamSummary, state)` at normal completion and its return value is ignored. There is no data-transformation via `on_complete` return value here — use `fetch()` for that.

Accepts the same safety caps as `fetch()` and `stream()`:

```python
pages = client.fetch_pages('events', max_pages=10)
pages = client.fetch_pages('events', params={'status': 'open'}, max_requests=50)
```

---

## 9. Exception Hierarchy

| Exception | When raised |
|---|---|
| `RestFetcherError` | Base class for all library exceptions. |
| `SchemaError` | Invalid or missing keys in schema dict. Unrecognised keys also raise by default (catches typos like `on_requets`). Pass `validate(schema, strict=False)` to warn instead of raising. |
| `AuthError` | Auth failure or invalid auth config. |
| `RequestError` | HTTP request failed. Has `.status_code`, `.response`, `.cause` (network errors), `.endpoint`, `.method`, `.url` (post-auth — always set for both network and HTTP errors). |
| `RateLimitError` | Reactive rate limit: `Retry-After` exceeds `max_retry_after`. Has `.retry_after`. |
| `RateLimitExceeded` | Proactive rate limit: token bucket empty and `on_limit='raise'`. Terminal — not passed to `on_error`. |
| `ResponseError` | API returned 200 but body signals an error. Has `.raw`. |
| `PaginationError` | Pagination config invalid or callbacks return unexpected types. |
| `CallbackError` | User callback raised an unexpected exception. Has `.callback_name`, `.cause`. |
| `StateViewMutationError` | Pagination callback tried to mutate the read-only state snapshot. Has `.key` (the key that was written). Wrapped in `CallbackError`. |
| `DeadlineExceeded` | Destructive deadline abort: `elapsed (time.monotonic()) >= time_limit`. Unlike `max_pages`/`max_requests`, this raises immediately with no partial result and `on_complete` does not run. Has `.limit`, `.elapsed` (seconds). |
| `PlaybackError` | Playback file missing in load mode, file I/O failure, or recorded envelope stream mismatch. |
| `FixtureFormatError` | Playback fixture contains an unrecognised or malformed envelope format. Subclass of `PlaybackError`. |

### Retry semantics

- Only `requests.RequestException` subclasses trigger retry (`ConnectionError`, `Timeout`, `TooManyRedirects`, SSL errors). Programming errors (`AttributeError`, `KeyError`, `TypeError`, etc.) bubble up immediately on the first attempt — they are not retried.
- After all retry attempts exhausted, a `RequestError` is raised with `.cause` set to the original network exception.

---

## 10. APIClient Reusability

`APIClient` is designed to be created once per API host and reused across multiple calls. The underlying `requests.Session` is shared, giving you connection pooling and keep-alive for free.

```python
with APIClient(schema) as client:
    campaigns = client.fetch('campaigns')
    reports   = client.fetch('reports')
    stats     = client.fetch('stats')
    # session closed automatically
```

| Component | Reusability notes |
|---|---|
| `requests.Session` | Shared across all calls. Connection pooling and keep-alive active. Always reuse. |
| Auth handler | Stateless for bearer, basic, callback. Safe to share across calls and endpoints. |
| `OAuth2Auth` / `OAuth2PasswordAuth` | Caches token + expiry on the handler instance. Safe for sequential (single-threaded) use. In a multithreaded context two threads could race to refresh — not a concern for typical Airflow ETL tasks. |
| `RetryHandler` | Tracks a monotonic `_last_request_tick` for `min_delay`. Stateful but safe for sequential use. |
| `_FetchJob` | Created fresh per call by `APIClient`. Never reused. `mock_idx` and per-call state are isolated. |
| `CycleRunner` | Created fresh per `_FetchJob`. State dict is local to one fetch operation. |

- For Airflow ETL — sequential tasks, single thread — `APIClient` is fully reusable without any caveats.
- Do not share one `APIClient` instance across parallel Airflow tasks if using OAuth2 auth — create one client per task instead.

---

_rest_fetcher — internal ETL library_

