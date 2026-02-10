# influxdb-toolkit

Unified Python API for InfluxDB v1 and v2 with extension points for v3.

Key goals:
- Consistent read/query/exploration API across v1 (InfluxQL) and v2 (Flux).
- Safe-by-default operations: write/delete/admin require explicit enablement.
- Clean boundaries for future v3 support.

## Install

```bash
pip install -e .
```

On this Windows setup, prefer:

```powershell
py -m pip install -e .
```

## Quickstart

### Auto-detect version (recommended)

```python
from datetime import UTC, datetime, timedelta
from influxdb_toolkit import InfluxDBClientFactory

# Pass either a v1-style config...
# config = {
#     "host": "localhost",
#     "port": 8086,
#     "username": "user",
#     "password": "pass",
#     "database": "mydb",
#     "ssl": False,
# }
#
# ...or a v2-style config:
config = {
    "url": "http://localhost:8086",
    "token": "my-token",
    "org": "my-org",
    "bucket": "my-bucket",
}

with InfluxDBClientFactory.get_client(config=config) as client:
    df = client.get_timeseries(
        measurement="temperature",
        fields=["value"],
        start=datetime.now(UTC) - timedelta(hours=24),
        end=datetime.now(UTC),
        interval="5m",
        aggregation="mean",
    )
    print(df.head())
```

The factory determines runtime version from input keys:

- v1 keys: `host`, `database`, `username/password` (or `user/pwd`)
- v2 keys: `url`, `token`, `org`

If both sets are present, the factory raises a clear error (`Ambiguous config`) instead of guessing.

### Explicit version (optional)

You can still force a version when needed:

- `InfluxDBClientFactory.get_client(version=1, config=...)`
- `InfluxDBClientFactory.get_client(version=2, config=...)`

## Configuration

Environment variables are supported (see `.env.example`). Explicit kwargs override env values.

## Safety

Write/delete/admin operations are disabled by default. To enable, set:

```bash
INFLUXDB_ALLOW_WRITE=true
```

Or pass `allow_write=True` in config. Use with care.

Write APIs accept optional `batch_size` for chunked writes:

```python
result = client.write_points(points, measurement="m", batch_size=5000)
```

## Docs

- `docs/architecture_concept.md`
- `docs/usage_setup.md`
- `docs/influxdb_v1_vs_v2_overview.md`
- `docs/internal_package_inventory.md`
- `docs/data_structure_analysis.md`
- `docs/python_package_comparison.md`
- `docs/auth_config_comparison.md`
- `docs/week_status.md`

## Development

```bash
pip install -e .[dev]
pytest
```

## Run Locally

`influxdb-toolkit` is a Python library, not a web server. "Run locally" means installing it and executing tests or a smoke script.

### 1) Install + tests

```powershell
cd c:\Users\talippun\Documents\influxdb\influxdb-toolkit
py -m pip install -e .[dev]
py -m pytest -q -o cache_dir="$env:USERPROFILE\.pytest_cache_influxdb_toolkit"
```

### 2) Read-only smoke test against configured InfluxDB

Set environment variables (`.env` or shell), then run:

```powershell
py scripts/smoke_read.py --version 1
py scripts/smoke_read.py --version 2
```

### 3) Use named connection profiles (recommended)

List available profiles:

```powershell
py scripts/smoke_read.py --list-profiles
```

Run by profile (credentials still come from env vars):

```powershell
py scripts/smoke_read.py --profile v1_flimatec
py scripts/smoke_read.py --profile v1_meteo
py scripts/smoke_read.py --profile v1_wattsup
py scripts/smoke_read.py --profile v1_mdb_connection_test
py scripts/smoke_read.py --profile v2_lcm_kwh_legionellen
py scripts/smoke_read.py --profile v2_meteo
```

To extend targets, add a new profile entry in `src/influxdb_toolkit/profiles.py`.

### 4) Re-generate schema analysis (Task 3)

Generate `docs/data_structure_analysis.md` from read-only live metadata queries:

```powershell
py scripts/schema_report.py --list-profiles
py scripts/schema_report.py --profile v1_flimatec --profile v2_meteo
py scripts/schema_report.py
```

The last command runs all profiles and overwrites `docs/data_structure_analysis.md`.
