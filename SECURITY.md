# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.x     | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

To report a security issue, email [sandip.dey1988@yahoo.com](mailto:sandip.dey1988@yahoo.com) with the subject line `[SECURITY] Atomic — <brief description>`.

Include:
- A description of the vulnerability and its potential impact
- Steps to reproduce or proof-of-concept code
- Any suggested mitigations if you have them

You should receive an acknowledgement within **48 hours** and a more detailed response within **7 days** indicating the next steps. After the initial response you will be kept informed of the progress toward a fix and disclosure.

## Scope

The following components are in scope:

- `atomic-compute`, `atomic-data`, `atomic-scheduler` — core Rust execution engine
- `atomic-worker` — standalone worker binary (TCP listener, task execution)
- `atomic-cli` — SSH/SFTP binary distribution tool
- `atomic-py`, `atomic-js` — language binding crates
- `atomic-sql` — SQL/DataFrame layer
- CI/CD workflows and release pipelines

The following are **out of scope** for the security program:

- Issues in third-party dependencies (please report to the respective upstream project)
- Theoretical vulnerabilities without a practical attack path
- Denial of service from running untrusted user-provided RDD tasks (workers execute arbitrary user code by design; isolate worker machines accordingly)

## Security Considerations for Operators

- **Worker isolation**: Workers execute arbitrary Rust `#[task]` functions linked into the binary at compile time and Python/JavaScript tasks at runtime. Run workers on isolated machines or containers with appropriate resource limits.
- **TLS**: Enable mTLS for worker communication in production (`--features tls`, `Config::tls_*` fields or `ATOMIC_TLS_*` env vars).
- **S3 credentials**: Use IAM instance roles or environment variables; never embed credentials in source code.
- **SSH key management**: `atomic ship` reads SSH private keys from the default agent or key path; ensure key permissions are `0600`.
