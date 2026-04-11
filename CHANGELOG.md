# Changelog

## 0.5.7

- **Strict schema validation by default:** `validate()` now defaults to `strict=True`. Unknown keys raise `SchemaError` with all issues reported together. Pass `strict=False` to emit `SchemaValidationWarning` instead.

## 0.5.6

- **Client metrics sessions:** optional `metrics` client schema key accepts `True`, `False` / `None`, or an explicit `MetricsSession` instance. `client.metrics` exposes a thread-safe session accumulator with `summary()` and atomic `reset()`.
- **`MetricsSummary` / `MetricsSession` exports:** both are available from the package root for explicit injection and type annotations.
- **Session totals are recorded on every run exit:** clean completion, raised errors, and abandoned generators all contribute one terminal run summary to the metrics session. Failed runs are counted explicitly via `total_failed_runs`.
- **Stream summary single-source rule:** stream completion callbacks, `stream_run().summary`, and metrics-session recording now share the same terminal `StreamSummary` object on clean completion, avoiding duplicate summary construction paths.
- **Docs/examples pass:** add client-metrics session guidance, config rules, and reset/reporting examples.

## 0.5.5

- **Callback ownership cleanup:** lifecycle hooks (`on_response`, `on_page`, `on_complete`, `on_page_complete`, `update_state`) move from `pagination` config to endpoint level. Pagination config now owns mechanics only: `next_request`, `delay`, `initial_params`. Placing lifecycle hooks inside a `pagination` dict raises `SchemaError` with a redirecting message in both strict and non-strict modes.
- **`on_page_complete` available for non-paginated endpoints:** the single request is the cycle, and `PageCycleOutcome` carries the same fields regardless of whether pagination was involved.
- **`on_event_kinds` filter:** new config key controls which event kinds reach `on_event`. Accepts `None`, `'all'`, a single kind string, or a set of kind strings. Validated against known event kinds. Inherits from client to endpoint.
- **`bytes_received` telemetry:** `request_end` events include `bytes_received` (per-response), `bytes_received_so_far` is callback-visible in run-local state, and `StreamSummary.bytes_received` reports the run total. Measured as `len(response.content)` for each response including playback.
- **`retry_bytes_received` telemetry:** `request_end` events include `retry_bytes_received` (per-cycle bytes from retry attempts only), `retry_bytes_received_so_far` is callback-visible in run-local state, and `StreamSummary.retry_bytes_received` reports the run total. Measured as `len(response.content)` for retry attempts 2+ in each page cycle, including playback replay of recorded retries.
- **Rename `PaginationRunner` → `CycleRunner`:** reflects that the runner is the cycle execution engine for both paginated and non-paginated flows.
- **`docs/schema_guide.md` added:** authoritative guide for config ownership, inheritance rules, layering, and annotated examples. `semantics.md` trimmed and cross-referenced.
- Fix proactive `rate_limit_wait_end` reporting so `planned_ms` preserves the original planned wait while `wait_ms` continues to report actual elapsed time.
- Align public typing with runtime retry config by adding `jitter` and `max_retry_after` to `RetryConfig` and removing `max_retry_after` from `RateLimitConfig`.
- Built-in pagination helpers use transparent compatibility normalization so existing helper usage continues to work without schema changes.

## 0.5.4

- Add `url_header_pagination(...)` for APIs that return the next-page URL directly in a response header.
- Tighten strict schema validation for known nested config dicts, including variant-aware auth validation while keeping callback auth open-ended.
- Expand stream completion summaries with wait-time breakdowns and improve wait observability with richer reactive wait reporting.
- Move `max_retry_after` from `rate_limit` to `retry` and align builder/docs/examples with the cleaner config boundary.

## 0.5.3

- Add post-cycle adaptive throttling via pagination `on_page_complete(outcome, state) -> float | None`.
- Export public `PageCycleOutcome` and add adaptive wait observability via `adaptive_wait_end`, `adaptive_waits_so_far`, and `adaptive_wait_seconds_so_far`.

## 0.5.2

- Extend `StreamSummary` in place with endpoint, source, retries, and elapsed timing.
- Expand `StopKind` / `StopSignal` to cover `next_request_none` and error-driven stop.
- Add run-local observability counters and callback-visible `*_so_far` state.
- Count playback request attempts the same way as live runs for observability.

Note: Older entries may mention `csv_encoding`. That setting was removed in `0.5.0` in favor of the general text-layer `encoding`.

## 0.5.1

- Added lifecycle event hook `on_event` for pagination engine telemetry (works in live and playback runs).
- Added proactive token-bucket rate limiting (`rate_limit`) with endpoint overrides and explicit opt-out.
- Added rate limit event kinds: `rate_limit_wait_start`, `rate_limit_wait_end`, `rate_limit_exceeded`.

## 0.5.0

- Replaced `csv_encoding` with general inherited `encoding` and made it a text-layer setting rather than a CSV-only knob.
- Added `FixtureFormatError` and split playback fixture format into logical format (`format`) vs storage form (`body` / `content_b64`).
- Added raw-byte playback support for `bytes` responses and optional raw-byte recording for any logical format via `playback.record_as_bytes`.
- Standardized raw-byte storage on a single-string `content_b64` field; chunked list form is now rejected explicitly.
- Normalized textual XML fixtures to UTF-8 body storage, with exact original XML bytes preserved via `content_b64` when needed.
- Kept parsing anchored on `.content`, clarified CSV order as bytes -> text via `encoding` -> parse via `csv_delimiter`, and updated docs/examples accordingly.

## 0.4.10

- Replace exception-driven page/request cap signaling with internal `StopSignal` return values. `max_pages` and `max_requests` still stop cleanly, but runner/context no longer play catch-and-translate games for normal bounded stops.
- Bump package version for the cap-signaling refactor groundwork.

## 0.4.9

- Bugfix: `max_pages` and `max_requests` are now non-destructive local caps. Reaching either cap stops cleanly and preserves already fetched pages/results instead of raising and discarding the aggregate.
- `time_limit` remains destructive and exceptional.
- `on_complete` now runs on normal page/request-capped completion and can inspect `state['stop']` for the stop reason.
- Added callback-contract docs and bounded-stop examples/tests.
- Added `oauth2_password` auth support for password-grant APIs such as GLPI v2, plus schema/builder coverage, tests, and cautious GLPI example docs.


## 0.4.7

- Move per-run mutable fetch state into `_RunState` and thread it through request execution.
- Simplify playback capture by accumulating pages in run state and extracting `_save_playback()`.
- Remove dead pagination tuple plumbing and strictify the `run_state` callback contract in runner/tests/docs.
- Polish docs, tighten internal typing, and clean up test helpers/structure.

## 0.4.6

- Trimmed `_ResolvedJobConfig` so it no longer carries intermediate `cfg`/`parser_arity` values, and added an explicit note that the cached effective runner is safe because `CycleRunner` is stateless across runs.
- Cleaned up tiny internal leftovers: `_raise_mutation()` no longer has an unused default key parameter.

## 0.4.5

- Build the effective pagination runner once per fetch job instead of reconstructing it on every `_run()` call.
- Clarified why `on_error` stays a direct callsite and why `_execute_mock()` intentionally bypasses the HTTP/auth/retry path.
- Kept `build_cycle_runner(...)` as the explicit external seam and trimmed `StateView` mutation-method repetition.

## 0.4.4

- Simplified fetch-job config resolution: removed persistent `_resolved` shadow state and replaced repeated override chains with a small `_resolve()` helper.
- Added call-time override precedence tests for response_format, csv_delimiter/csv_encoding, and scrub lists.
- Includes 0.4.3 playback-save regression coverage.

## 0.4.3

- Added regression tests that assert playback fixture files are actually written for save-mode paths that regressed in an earlier internal change.
- Fixed playback save so per-page raw envelopes are still recorded when `on_complete` is active.

## 0.4.2

- Validated inherited endpoint fields (`response_parser`, `csv_delimiter`, `csv_encoding`, `scrub_headers`, `scrub_query_params`) at endpoint level instead of letting bad overrides fail later at runtime.
- Shared timeout validation between top-level schema and endpoint validation.
- Moved unnecessary deferred exception imports out of hot paths in `client.py` and `pagination.py`.

## 0.4.1

- Unified callback invocation through a shared `safe_call` helper so pagination callbacks now re-raise `CallbackError`, `RequestError`, and `ResponseError` unchanged instead of re-wrapping them.
- Kept `on_error` as a deliberate direct callsite so its exceptions still propagate as-is.

## 0.4.0

- Non-paginated endpoints now run `on_page` and `on_complete` through the same callback engine as paginated endpoints.
- Added explicit coverage for extra recorded responses on non-paginated raw playback endpoints.
- Removed a few obvious import leftovers while keeping the runtime model unchanged.

## 0.3.3

- Unified `_run()` around a single callback execution engine: both live and raw playback now route non-paginated endpoints through an internal single-page `CycleRunner` instead of bespoke callback paths.
- This removes the remaining duplicated mini-engines from `_run()` while preserving the existing live/raw playback model after the playback format cleanup.

## 0.3.2

- Playback fixtures use raw response envelopes.
- Playback save is no longer supported with `mock:`; raw playback requires a real HTTP response.
- Updated docs/comments and removed tests for discarded playback behavior.

## 0.3.1

- Kept only the low-risk hot-path cleanups: reuse precomputed retry kwargs, skip the second state rebuild only when `update_state` returns an empty dict, and avoid copying lists in playback save when they are already lists.
- Tightened the retry-path contract slightly by reading required `method`/`url` with direct indexing instead of silent `None` fallbacks.

## 0.3.0

- Final internal cleanup pass: renamed `_process_response()` to `_process_response()` to match its actual responsibility after the request-outcome refactor.

## 0.2.9

- Fixed the internal request-result refactor so `_process_response()` returns `_RequestOutcome` directly on all non-raising paths, including nested error-skip handling.
- Regenerated the cleanup diff against the actually fixed artifact.

## 0.2.8

- Completed the internal fetch-job cleanup: one `_RequestOutcome` contract end-to-end, explicit config-resolution inputs, `_collect()` / `_generate()` internal naming, and no `OperationContext` construction for playback load mode.
- Updated the `link_header_pagination` docstring example to use `data_path='results'`.

## 0.2.7

- Changed the built-in CSV defaults to delimiter `;` and encoding `utf-8`.
- Updated docs/examples to describe the CSV default policy explicitly.

## 0.2.6

- Added `csv_encoding` config for `response_format='csv'` on client schema, endpoint schema, and per-call overrides.
- CSV playback recording now stores CSV text using the configured encoding rather than relying on `response.text` defaults.
- Kept the built-in CSV policy intentionally narrow: only `csv_delimiter` and `csv_encoding` are supported.

## 0.2.5

- Added `csv_delimiter` config for `response_format='csv'` on client schema, endpoint schema, and per-call overrides.
- CSV playback replay now uses the same configured delimiter as live parsing.
- Kept other CSV-specific behavior intentionally out of scope; use `response_format='text'` plus a custom parser for metadata rows, encoding quirks, or nonstandard dialect needs.

## 0.2.4

- Added curated raw playback fixtures under `examples/fixtures/` for selected examples.
- Selected examples now include inert playback blocks (`mode: 'none'`) that can be switched to `'load'` for offline replay.
- Added lightweight example replay scenario tests using the same curated fixtures.

## 0.2.3

- Added playback mode `none` to disable record/replay without removing the `playback` config block.
- Added a small typed helper in `pagination.py` for `cursor_param` handling.

## 0.2.2

- Added playback-fixture scrubbing for recorded request URLs (sensitive query parameter values are redacted before fixtures are written).
- Raw playback envelopes now also store recorded request headers, scrubbed using the existing header-scrub rules plus any configured `scrub_headers` extensions.
- Added `scrub_query_params` config (client / endpoint / per-call) and `SchemaBuilder.scrub_query_params()` for query-param scrub extensions.
- Expanded docs/examples around playback scrubbing and request-side secret handling.

## 0.2.1

- Fixed duplicate function definitions that shadowed the new implementations in `client.py` and `pagination.py`.
- Fixed `link_header_pagination()` to accept `data_path`.
- Reworked playback to record raw response envelopes and replay the normal parsing/callback/pagination pipeline on load.
- Added list-index support to nested path resolution (for example `items.0.name`).
- Added regression tests for raw playback replay, nested pagination paths, and helper behavior.

## 0.2.0

### Added

- `response_format` support on client schema, endpoint schema, and per-call overrides
- built-in parsing for `auto`, `json`, `text`, `xml`, `csv`, and `bytes`
- canonical parsed-body semantics for 2-arg `response_parser(response, parsed)`
- nested-path support in built-in body-based pagination helpers
- typed playback envelopes for JSON and text-backed formats (`text`, `xml`, `csv`)

### Changed

- default response parsing is no longer JSON-only
- 2-arg response parsers now receive the canonical parsed body for the configured/inferred format
- playback files now store typed envelopes instead of assuming pages are always plain JSON payloads
- package version bumped from `0.1.0` to `0.2.0`

### Preserved

- existing JSON workflows continue to work
- retry, auth, caps, and link-header pagination behavior remain intact

### Notes

- playback supports exact raw bytes via single-string `content_b64` fixtures (`response_format='bytes'`) and can record any logical format as raw bytes via `playback.record_as_bytes`
- runtime caps are intentionally bypassed in playback load mode
- body-based pagination helpers are still primarily dict/JSON-oriented, but now support nested paths like `data_path='meta.items'`