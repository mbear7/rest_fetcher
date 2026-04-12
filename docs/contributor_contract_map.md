Contributor Contract Map
========================

This note is maintainer-facing. It records core invariants and fragile areas that are easy to damage with seemingly local cleanups.

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

Config semantics that are easy to forget
----------------------------------------

1. Be explicit about merge vs replace.
   - `headers` and `params` are merged by scope.
   - `rate_limit`, `pagination`, and `canonical_parser` are replacement-style overrides.
  - `retry` is merge-style: endpoint retry dictionaries override only the specified keys, and `retry: None` disables inherited retry for that endpoint.
  - `pagination` is replacement-style: endpoint pagination replaces inherited client pagination rather than merging field-by-field.
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

Callbacks: public behavior vs internal details
----------------------------------------------

1. Public docs should describe stable callback contract only.
   - Ordering or data-shape details that are purely implementation artifacts do not belong in public docs.

2. Endpoint-level `None` can be meaningful suppression.
   - Example: `on_request=None` suppresses inherited client-level `on_request`.

3. Be careful when changing callback firing boundaries.
   - `on_complete` behavior differs between clean completion and failure paths.
   - Do not widen callback firing surfaces casually.

Fragile implementation seams
----------------------------

1. Error handling has two distinct raise contexts.
   - `_handle_error_response(...)` constructs an exception locally and must use `raise exc`.
   - `_handle_request_cycle_error(...)` runs inside an active `except` path and must use bare `raise` to preserve traceback.
   - `_dispatch_on_error_action(...)` handles shared action interpretation (state bookkeeping,
     outcome/signal construction, `CallbackError` for invalid returns) but intentionally does not
     raise — each caller uses its own appropriate raise form.

2. `client.py` and `pagination.py` communicate through a private run-state contract.
   - Keep the contract aligned with what `CycleRunner` actually consumes.
   - Small protocol/type comments are preferable to broad refactors here.

3. Shared live/playback request-cycle handling should stay focused.
   - Keep the common request-cycle envelope shared.
   - Keep source-specific response acquisition narrow.
   - Avoid reintroducing duplicated accounting/event/error logic across live and playback.

4. `_rf_pagination_helper` is a private three-way protocol.
   - Built-in strategies tag their pagination-config dicts with `'_rf_pagination_helper': True`.
   - `_FetchJob._resolve_config` in `client.py` inspects this tag to extract strategy-provided
     lifecycle hooks without requiring separate config keys.
   - `schema.py`'s `_validate_pagination` skips unknown-key checks for tagged dicts.
   - Third-party strategies must not use this tag. It is internal to the library.

Review checklist for risky changes
----------------------------------

Before merging changes in request execution, callbacks, summaries, metrics, retry, or rate limiting, check:

- Does this preserve one terminal summary per run?
- Does metrics aggregation still consume the canonical summary rather than recomputing totals?
- Did any override semantics silently change from merge to replace, or vice versa?
- Did any callback fire in a path where it previously did not?
- Did any change alter live/mock/playback parity intentionally or accidentally?
- Did a cleanup touch `raise exc` vs bare `raise` behavior?
- Is the claimed behavior covered by tests before it is documented?
