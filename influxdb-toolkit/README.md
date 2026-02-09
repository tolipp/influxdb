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

## Quickstart

### v1

```python
from datetime import datetime, timedelta
from influxdb_toolkit import InfluxDBClientFactory

config = {
    "host": "localhost",
    "port": 8086,
    "username": "user",
    "password": "pass",
    "database": "mydb",
    "ssl": False,
}

with InfluxDBClientFactory.get_client(version=1, config=config) as client:
    df = client.get_timeseries(
        measurement="temperature",
        fields=["value"],
        start=datetime.utcnow() - timedelta(hours=24),
        end=datetime.utcnow(),
        interval="5m",
        aggregation="mean",
    )
    print(df.head())
```

### v2

```python
from datetime import datetime, timedelta
from influxdb_toolkit import InfluxDBClientFactory

config = {
    "url": "http://localhost:8086",
    "token": "my-token",
    "org": "my-org",
    "bucket": "my-bucket",
}

with InfluxDBClientFactory.get_client(version=2, config=config) as client:
    df = client.get_timeseries(
        measurement="temperature",
        fields=["value"],
        start=datetime.utcnow() - timedelta(hours=24),
        end=datetime.utcnow(),
        interval="5m",
        aggregation="mean",
    )
    print(df.head())
```

## Configuration

Environment variables are supported (see `.env.example`). Explicit kwargs override env values.

## Safety

Write/delete/admin operations are disabled by default. To enable, set:

```bash
INFLUXDB_ALLOW_WRITE=true
```

Or pass `allow_write=True` in config. Use with care.

## Docs

- `docs/architecture_concept.md`
- `docs/influxdb_v1_vs_v2_overview.md`
- `docs/internal_package_inventory.md`
- `docs/data_structure_analysis.md`
- `docs/python_package_comparison.md`

## Development

```bash
pip install -e .[dev]
pytest
```
