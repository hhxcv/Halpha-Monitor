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

For user-facing projections, apply the UI fact contract in `docs/PROJECT-PRINCIPLES.md`: render contract-satisfying facts without routine validity badges, columns, or filters; keep candidates and incomplete records out of the primary decision view; label only concrete exceptions; keep source health and collection diagnostics secondary; and make human input an optional correction path rather than a prerequisite for usable results. Review every changed visible string for plain, concrete meaning: replace internal state names, metaphors, workflow jargon, and ambiguous parenthetical qualifiers; state the condition and its visible impact; and explain any unavoidable technical term in place. Remove persistent banners or method notes that merely repeat source provenance, baseline quality behavior, generic disclaimers, obvious control behavior, or other default design expectations; put a necessary rule next to the field or category it explains and make it available on hover and keyboard focus. Add projection or UI checks that prevent a regression when this distinction is material to the change.

### Deliver the running web result

For any new or changed page, API, or web projection:

- Distinguish the temporary QA endpoint from the normal delivery endpoint before starting either one. A temporary port, `TestClient`, screenshot, or passing browser script proves behavior but is not the delivered web result.
- Before claiming completion, run the current code on the user-agreed normal service endpoint, preserve its existing externalized database configuration, and ensure there is exactly one intended scheduler/service instance. Do not leave an old process serving the normal URL or run a second collector against the real database.
- Verify the delivered URL itself: health succeeds, the changed monitor or route is registered, the direct user URL loads, and the visible page is the current version. Give that exact URL to the user.
- If authority, tooling, or the runtime prevents updating the normal service, report the work as blocked on that concrete action. State which temporary endpoint was used for QA and do not describe the web change as delivered.

### Release temporary resources

Treat every validation server, listener, process, thread, browser session, database, log, download, screenshot, cache, and temporary directory as an owned resource:

- Record its purpose, exact path or identifier, PID/session when applicable, port, and intended lifetime. Use a unique task directory and synthetic database for destructive or browser acceptance tests; never repurpose the real monitor database for fixtures.
- Put shutdown and cleanup in `finally` behavior where code controls the lifecycle. Otherwise close browser sessions, stop the exact validated process, wait for the port to clear, release files, and delete or recycle task-owned artifacts before handoff.
- Recheck the process list, listening ports, browser sessions, and exact paths after cleanup. A successful stop/delete command without this absence check is insufficient.
- Do not delete shared caches, user evidence, normal service data, or resources whose ownership is uncertain. Any retained validation artifact must be an explicit durable deliverable; otherwise remove it.
- In the final report, name the verified delivery URL, the persistent service intentionally left running, the temporary resources removed, and any cleanup or runtime blocker that remains.

At the current project stage, do not add GitHub CI or GitHub Actions. Run deterministic tests, static checks, privacy gates, broad regressions, live-network, startup, restart, UI, long-duration, and expensive checks locally in proportion to impact, and report exactly what ran. Reconsider remote CI only after the recurring-failure threshold in `docs/PROJECT-PRINCIPLES.md` is demonstrably met; if it is reintroduced, keep it short, deterministic, and network-free.

## Integrate cleanly

Review the full diff, remove scaffolding and unused paths, preserve unrelated owner work, and run `python .githooks/check_local_privacy.py --all` before any Git publication. Report changed behavior, validation, and residual risk. Do not infer authority to commit, push, publish, control another repository, or perform a trading action.
