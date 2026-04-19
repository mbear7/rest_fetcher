Contributor Contract Map
========================

This note is maintainer-facing. It records core invariants and fragile areas that are easy to damage with seemingly local cleanups.

API surface
-----------

Every exported or compatibility name should have an explicit status. When adding a new name, decide its status before merging.

| Name | Status | Notes |
|---|---|---|
| `APIClient` | **public** | Package root. Primary entry point. |
| `PaginationEvent` | **public** | Package root. Stable event dataclass. |
| `PaginationRunner` | **public** | Package root + `rest_fetcher.pagination`. Canonical engine name since 0.5.x. |
| `cursor_pagination` | **public** | Package root. |
| `link_header_pagination` | **public** | Package root. |
| `url_header_pagination` | **public** | Package root. |
| `offset_pagination` | **public** | Package root. |
| `page_number_pagination` | **public** | Package root. |
| `MetricsSession` | **public** | Package root. |
| `MetricsSummary` | **public** | Package root. |
| `EndpointMetrics` | **public** | Package root. |
| `validate` | **public** | Package root. Schema validation entry point. |
| `SchemaBuilder` | **public** | Package root. Optional fluent builder; zero runtime cost. |
| `ClientSchema`, `EndpointSchema`, `PaginationConfig`, `RateLimitConfig`, `RetryConfig`, `AuthConfig` | **public** | Package root. Optional TypedDicts for IDE/mypy. |
| `OnErrorFn`, `OnEventFn`, `OnPageCompleteFn`, `OnRequestFn` | **public** | Package root. Callback protocol types. |
| `StreamRun`, `StreamSummary`, `StopSignal`, `PageCycleOutcome` | **public** | Package root. |
| All exceptions (`RestFetcherError`, `SchemaError`, …, `raise_`) | **public** | Package root. |
| `build_pagination_runner` | **public (submodule only)** | `rest_fetcher.pagination`. Canonical factory. Intentionally not promoted to package root. |
| `CycleRunner` | **compatibility** | `rest_fetcher.pagination` only. Alias for `PaginationRunner`. Do not add new uses; do not remove until a deprecation cycle is complete. |
| `build_cycle_runner` | **compatibility** | `rest_fetcher.pagination` only. Alias for `build_pagination_runner`. Same policy as `CycleRunner`. |
| `_FetchJob` | **internal** | `rest_fetcher._fetch_job`. Not for external use. |
| `_RunState` | **internal** | `rest_fetcher._run_state`. Not for external use. |
| `_scrub`, `_scrub_recorded_url` | **internal** | `rest_fetcher.playback`. Not for external use. |
| `_UNKNOWN_ENDPOINT` | **internal** | `rest_fetcher.metrics`. Sentinel string; test via observable behavior, not by importing it. |

Core invariants
---------------

1. One terminal summary per run.
   - Each run has one canonical terminal `StreamSummary`.
   - Do not create parallel summary-construction paths for different exit modes.

2. `StreamSummary` is canonical per-run truth.
   - Session metrics aggregate terminal run summaries.
   - Do not recreate totals in a separate telemetry path.

3. Metrics record every run exit.
   - Clean completion, failure, and abandoned generators all record into `MetricsSession` when metrics are enabled.
   - User-facing completion surfaces do not necessarily mirror all terminal recording behavior.

4. Failure is control-flow-defined.
   - Do not infer failure only by inspecting summary fields after the fact.
   - The code path that ends the run determines whether the run is counted as failed.

5. `MetricsSession.reset()` is flush-and-clear.
   - It returns the pre-reset totals and clears the session state.
   - Runs that finish later record into the fresh post-reset state.

6. `by_endpoint` sum equals grand totals.
   - For every scalar field on `EndpointMetrics`, the sum across `MetricsSummary.by_endpoint.values()` equals the corresponding `total_*` field on `MetricsSummary`.
   - Runs with `endpoint=None` are bucketed under `'<unknown>'`, not silently dropped.
   - Do not record to the grand-total counters without also recording to the per-endpoint bucket, and vice versa.

7. `_FetchJob` owns per-call execution; `APIClient` owns shared state.
   - `APIClient` owns: the `requests.Session`, auth handler, retry handler, shared token bucket, playback handler, metrics session, and the four public fetch methods (`fetch`, `fetch_pages`, `stream`, `stream_run`).
   - `_FetchJob` owns: request building, retry loop, rate-limit wait, event emission, and `StreamSummary` construction for one call.
   - Per-call logic belongs in `_FetchJob`. Logic that spans calls or manages shared resources belongs in `APIClient`.

Config semantics that are easy to forget
----------------------------------------

1. Be explicit about merge vs replace.
   - `headers` and `params` are merged by scope.
   - `rate_limit` and `canonical_parser` are replacement-style overrides.
   - `retry` is merge-style: endpoint retry dictionaries override only the specified keys; `retry: None` disables inherited retry for that endpoint.
   - `pagination` is merge-style: endpoint pagination merges over client-level defaults (`merge_dicts`). Endpoint `None` explicitly disables; omitting inherits the client default.
   - Explicit `None` can be a meaningful disable, not just "missing".

2. `rate_limit` mixes proactive and reactive controls.
   - Token-bucket settings control proactive limiting before requests.
   - `respect_retry_after` and `min_delay` affect reactive waiting.
   - Endpoint/call overrides replace inherited `rate_limit` config; they do not field-merge.

3. Call-level token-bucket overrides are not shared.
   - Shared buckets exist for client-level and endpoint-level scopes.
   - Call-time overrides create per-call behavior.

Intentional mode differences
----------------------------

1. Live means network mode.
   - Mock mode uses the live execution path for accounting/events, but does not perform a real network request.

2. Playback is intentionally not identical to live.
   - Playback bypasses proactive token-bucket limiting.
   - Shared request-cycle accounting/events still apply around playback responses.

3. Event `source` values are a closed internal contract.
   - Valid values are `live` and `playback`.
   - Invalid internal values should fail loudly.

Execution-mode contracts
------------------------

1. `fetch()` vs `fetch_pages()` differ in two ways, not one.
   - Return shape: `fetch()` unwraps single-page results to the bare page value; `fetch_pages()` always returns a list, regardless of page count.
   - `on_complete` semantics: in `fetch()`, the return value of `on_complete` becomes the final return value of `fetch()` — it can transform the data. In `fetch_pages()`, `on_complete` follows stream semantics: it fires with `(StreamSummary, state)` but its return value is ignored. Do not silently widen the `on_complete` contract in one mode without auditing the other.

2. `canonical_parser` and `response_parser` hook at different pipeline points.
   - `canonical_parser(content_bytes, context)` replaces built-in format parsing entirely. Its output is what pagination callbacks (`next_request`) and `response_parser` see. Use it when the API's wire format is not natively handled or when custom parsing must influence pagination.
   - `response_parser(response)` or `response_parser(response, parsed)` runs after canonical or built-in parsing. It reshapes what downstream hooks (`on_response`, stream items) receive without affecting pagination. The 2-arg form receives the already-decoded body as `parsed`.
   - Do not swap their roles: a `canonical_parser` that only reshapes data will silently break `next_request`; a `response_parser` that tries to drive pagination will have no effect on it.

3. `validate()` strict mode controls unknown-key handling only.
   - `strict=True` (the default): unknown keys in any config dict raise `SchemaError`. All unknown-key issues are collected before raising, so the caller sees the full list at once.
   - `strict=False`: unknown keys emit `SchemaValidationWarning` instead of raising.
   - In both modes, type errors, missing required fields, and other semantic checks still fail immediately on the first problem encountered. The strict flag does not suppress those.
   - `APIClient.__init__` calls `validate()` with `strict=True` by default. Do not add per-key strict carve-outs without updating the schema guide.

Callbacks: public behavior vs internal details
----------------------------------------------

1. Public docs should describe stable callback contract only.
   - Ordering or data-shape details that are purely implementation artifacts do not belong in public docs.

2. Endpoint-level `None` can be meaningful suppression.
   - Example: `on_request=None` suppresses inherited client-level `on_request`.

3. Be careful when changing callback firing boundaries.
   - `on_complete` fires inside `PaginationRunner.run()`, after all pages are collected, before `StreamSummary` is constructed. Metrics recording happens after `on_complete` returns; a raising `on_complete` counts as a failed run.
   - Do not widen callback firing surfaces casually.

Fragile implementation seams
----------------------------

1. Error handling has two distinct raise contexts.
   - `_handle_error_response(...)` constructs an exception locally and must use `raise exc`.
   - `_handle_request_cycle_error(...)` runs inside an active `except` path and must use bare `raise` to preserve traceback.
   - `_dispatch_on_error_action(...)` handles shared action interpretation (state bookkeeping,
     outcome/signal construction, `CallbackError` for invalid returns) but intentionally does not
     raise — each caller uses its own appropriate raise form.

2. `_fetch_job.py` and `pagination.py` communicate through a private run-state contract.
   - Keep the contract aligned with what `PaginationRunner` actually consumes.
   - Small protocol/type comments are preferable to broad refactors here.

3. Shared live/playback request-cycle handling should stay focused.
   - Keep the common request-cycle envelope shared.
   - Keep source-specific response acquisition narrow.
   - Avoid reintroducing duplicated accounting/event/error logic across live and playback.

4. `_rf_pagination_helper` is a private three-way protocol.
   - Built-in strategies tag their pagination-config dicts with `'_rf_pagination_helper': True`.
   - `_FetchJob._resolve_config` in `_fetch_job.py` inspects this tag to extract strategy-provided
     lifecycle hooks without requiring separate config keys.
   - `schema.py`'s `_validate_pagination` skips unknown-key checks for tagged dicts.
   - Third-party strategies must not use this tag. It is internal to the library.

Review checklist for risky changes
----------------------------------

Before merging changes in request execution, callbacks, summaries, metrics, retry, or rate limiting, check:

- Does this preserve one terminal summary per run?
- Does metrics aggregation still consume the canonical summary rather than recomputing totals?
- Does `by_endpoint` still sum to grand totals for every scalar field?
- Did any override semantics silently change from merge to replace, or vice versa?
- Did any callback fire in a path where it previously did not?
- Did any change alter live/mock/playback parity intentionally or accidentally?
- Did a cleanup touch `raise exc` vs bare `raise` behavior?
- Is the claimed behavior covered by tests before it is documented?
