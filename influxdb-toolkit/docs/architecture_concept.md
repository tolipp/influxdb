# Architecture Concept

## Goals
- Provide a consistent API for InfluxDB v1 (InfluxQL) and v2 (Flux).
- Keep write/delete/admin operations safe by default.
- Make v3 integration minimal by isolating version-specific logic.

## Design

- Factory pattern in `influxdb_toolkit.client.InfluxDBClientFactory` selects v1/v2 based on config.
- Strategy pattern via `InfluxDBClientBase` and versioned implementations under `v1/` and `v2/`.
- Query builders (`v1/query_builder.py`, `v2/query_builder.py`) isolate language-specific query assembly.
- Shared error model in `exceptions.py`.

## Module Boundaries

- `base.py` defines the public API contract and shared helpers.
- `v1/client.py` uses `influxdb` (InfluxQL) and implements exploration and query methods.
- `v2/client.py` uses `influxdb-client` (Flux) and adds InfluxQL compatibility for v2 where available.
- `config.py` loads env and normalizes configuration into `V1Config` / `V2Config`.

## Safety

- `allow_write` defaults to `False`.
- Write/delete/admin methods raise `UnsafeOperationError` unless explicitly enabled.
- This aligns with the current project safety rules for read-only DB usage.

## Extension to v3

- Add `v3/` with `client.py` and `query_builder.py`.
- Extend factory to recognize v3 config.
- Reuse base interface and shared models.

## Testing

- Unit tests use mocks only.
- Integration tests are placeholders and should be wired to Docker when approved.