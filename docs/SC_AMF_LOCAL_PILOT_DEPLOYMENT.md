# SC-AMF R1 local deployment contract

This is a bounded **synthetic smoke deployment**, not production configuration.

## Invariants

- Core image: `agentmemory/memory-core@sha256:f9b286246d0e5020a7f0cb011b7074703d10b76b424a834a117482392f7bd424`
- Hub image: `agentmemory/memory-hub@sha256:99c234f606be6e0496e78cddf220a9ebf12248863276991f2132d2a1b7d9a95f`
- Core is published only as `127.0.0.1:8420`.
- Panel is published only as `127.0.0.1:8125`.
- Knowledge is published only as `127.0.0.1:8424`.
- Proxy (`8096`) is not created.
- No real LLM/provider credential is accepted by the start script.
- The Hub receives only explicit synthetic placeholder values needed to prove startup wiring.
- No admin user/key is created by these scripts.
- No private/live SanityCloud history is ingested.
- Named volumes are preserved by normal stop; `-Purge` is a separate destructive action.

## Host prerequisite defect discovered during R1

The CodingTools execution environment omitted `ProgramData`, `USERPROFILE`, `APPDATA`, and `LOCALAPPDATA`. Docker Desktop exited with `unable to get 'ProgramData'`. The start script restores standard Windows values before starting Docker Desktop if the engine is unavailable. It does not delete Docker state.

## Proven versus deliberately unproven

The smoke test proves service health, Hub-to-Core network reachability, loopback-only host publication, named-volume persistence, and deterministic operator stop/start recovery. It does **not** claim real LLM-backed extraction/knowledge behavior, live authenticated Agent Memory calls, automatic crash-restart semantics, or production suitability.

Those remaining behaviors require Session Broker-authorized credentials/capabilities and independent governance/validation before expansion.
