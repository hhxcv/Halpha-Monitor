---
name: develop-halpha-monitor
description: Implement, review, refactor, test, and operate the standalone Halpha Monitor service. Use for collectors, public-source adapters, rate limits, persistence, retention, FastAPI/UI projection, configuration, packaging, runtime checks, dependencies, or CI changes in Halpha-Monitor.
---

# Develop Halpha Monitor

## Establish the result

Read `../../../docs/PROJECT-PRINCIPLES.md`, the relevant parts of `../../../README.md`, and the target code and tests before changing anything.

State the smallest result in concrete terms:

- the current user decision, risk, or operational failure being served;
- the visible or machine-verifiable result;
- the source, time meaning, validity cutoff, and failure/unknown behavior;
- what is deliberately excluded;
- the checks that will establish completion.

Do not add a capability with no current consumer or no plausible contribution to better risk-adjusted net profitability. Prefer deletion, an existing view, an external tool, or a manual step when it has lower whole-life cost.

## Choose the implementation

Use this order:

1. Existing registered monitor, store, web projection, configuration path, and locked dependency;
2. Maintained mature library, official SDK, or official public API;
3. Supported composition or a thin adapter for source-specific semantics;
4. Minimal custom code for a concrete uncovered gap.

Do not introduce a framework, service, database, generic abstraction, or second representation unless it reduces total implementation, operation, diagnosis, migration, and deletion cost. Keep one owner for each fact and reuse it through storage, API, and UI.

## Implement source and lifecycle behavior

- Use public data only; never load product secrets, account state, or exchange-changing capability. Apply the network-egress boundary owned by `docs/PROJECT-PRINCIPLES.md`: do not attach host-private values or add telemetry, diagnostics or uploads to public-source requests.
- Keep collectors independently fail-able. Attach source identity, observation time, source time, cutoff, units, and quality state where the fact requires them.
- Preserve upstream timeout, rate-limit, and `Retry-After` semantics. Retry only temporary failures with bounded exponential backoff and jitter; surface permanent or contract errors.
- Never fabricate freshness. Empty, stale, missing, malformed, and partial results remain distinguishable from valid current data.
- Bound configuration values, transactions, retention, caches, threads, work queues, retries, and log growth. On restart, resume current-value collection rather than replaying obsolete work without limit.
- Reuse the single-process FastAPI/SQLite topology and explicit built-in registration. Do not build a plugin platform or distributed scheduler for a monitor addition.
- Ensure shutdown stops scheduling, resolves current writes safely, and releases owned processes and resources.

## Validate by impact

Select the smallest relevant set and expand when failures could be hidden or persistent:

- source parsing: valid, empty, malformed, stale, partial, throttled, timeout, network loss, and recovery;
- persistence: atomic writes, schema compatibility, restart, retention, and bounded growth;
- projection: API semantics, source/time labels, unknown states, and localhost UI behavior;
- operation: install/package, startup, health, one real public read when appropriate, clean shutdown, and no owned process leak.

Keep CI short, deterministic, and network-free. Run broad, live-network, long-duration, or expensive checks locally and report exactly what ran.

## Integrate cleanly

Review the full diff, remove scaffolding and unused paths, preserve unrelated owner work, and run `python .githooks/check_local_privacy.py --all` before any Git publication. Report changed behavior, validation, and residual risk. Do not infer authority to commit, push, publish, control another repository, or perform a trading action.
