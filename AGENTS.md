# Halpha Monitor AI Entry Point

## Authority

- Stable value, boundary, reuse, and complexity rules come from `docs/PROJECT-PRINCIPLES.md`.
- `README.md` owns current implemented behavior and operating instructions. If code, README, and principles disagree, report the inconsistency instead of inventing a current fact.
- For implementation, review, refactoring, tests, dependencies, runtime configuration, or operational validation, use `.agents/skills/develop-halpha-monitor/SKILL.md`.

## Repository boundary

- Keep this repository independent from the Halpha product runtime. Do not import product packages, credentials, account data, databases, control registries, or trading APIs.
- Read public market endpoints only. Never infer authority to send an order, change an exchange account, or control a Halpha trading process.
- Require a named current consumer and decision value for new capability. Prefer an existing component, maintained third-party library, official API, or thin adapter before custom code.
- Preserve the current single-process topology unless a measured need and lower whole-life complexity justify a change.

## Work baseline

- Preserve unrelated work and obtain explicit authority for commit, push, release, or external state changes.
- Make the smallest reversible change that produces a user-visible or operationally verifiable result; remove temporary and unused paths before handoff.
- Keep external facts source- and time-aware, represent unknown or stale states honestly, isolate partial failures, and bound retries, threads, caches, retention, and shutdown.
- Keep CI short, deterministic, and network-free. Run broader public-source, startup, restart, UI, and long-running checks locally when the change warrants them.
