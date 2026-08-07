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
- The local-privacy and network-egress boundary is owned by `docs/PROJECT-PRINCIPLES.md`. Before commit or push, run `python .githooks/check_local_privacy.py --all`; never bypass the repository hooks or publish a rejected revision.
- Make the smallest reversible change that produces a user-visible or operationally verifiable result. For Web delivery and temporary-resource ownership, follow the explicit gates in the development Skill; never substitute a temporary QA endpoint for the current service or hand off owned temporary resources.
- Keep external facts source- and time-aware, represent unknown or stale states honestly, isolate partial failures, and bound retries, threads, caches, retention, and shutdown.
- Do not add GitHub CI while `docs/PROJECT-PRINCIPLES.md` keeps validation local-only. Run tests, static checks, privacy gates, public-source, startup, restart, UI, and long-running checks locally in proportion to the change; reconsider CI only after the observed recurring-failure threshold in the principles is met.
