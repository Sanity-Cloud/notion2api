# Credentialed Integration Sandbox

## Boundary

The sandbox is separate from the live checkout and service. It uses:

- localhost port `18120`
- an external virtual environment
- a separate database and logs
- an ACL-restricted credential copy under `%LOCALAPPDATA%\SanityCloud\notion2api-sandbox\secrets`
- explicit opt-in before any remote Notion inference

Secret values are not committed, copied into images, printed by the launcher, or stored in the workspace library.

## Local runtime

```powershell
.\scripts\sandbox-runtime.ps1 Initialize
.\scripts\sandbox-runtime.ps1 Install
.\scripts\sandbox-runtime.ps1 Start
.\scripts\sandbox-runtime.ps1 Test
```

A credentialed remote test must include either:

- header `X-Sandbox-Allow-Remote: true`, or
- request metadata `sandbox_allow_remote: true`

Without that opt-in, `/v1/chat/completions` returns `SANDBOX_REMOTE_AUTH_REQUIRED`. Local `OK`/`pong` probes remain available without remote provider calls.

## Container runtime

`docker-compose.sandbox.yml` is ready for Docker Desktop. Supply external paths through `SANDBOX_ACCOUNTS_FILE`, `SANDBOX_API_KEY_FILE`, `SANDBOX_DATA_DIR`, and `SANDBOX_LOG_DIR`. The current machine must have a running Docker engine and Compose plugin before it can be launched.
