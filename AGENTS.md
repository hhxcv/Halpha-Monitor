# Halpha Monitor AI Entry Point

- `README.md` owns the current service boundary and operating semantics.
- This repository is independent from the Halpha product runtime. Do not import Halpha product packages, credentials, account data, databases, control registries, or trading APIs.
- The service may read only public market endpoints and may mutate only its own local configuration, cache, logs, and SQLite database.
- Preserve one process, one FastAPI surface, one SQLite database, explicit built-in monitor registration, bounded retention, independent scheduler threads, and local-loopback binding unless a verified requirement needs otherwise.
- Respect upstream timeout and rate-limit semantics. HTTP 418/429 and temporary network errors must remain bounded to one monitor, use backoff where supported, and must not fabricate current samples.
- Prefer mature libraries and standard library clients. Add monitor-specific code only where no maintained framework provides the endpoint or field semantics.
- Validate source logic, partial failures, persistence, retention, API projection, package installation, and a real localhost startup before release.
- Never infer authority to send an order, change an exchange account, or start/stop Halpha product processes.

