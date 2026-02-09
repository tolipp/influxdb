# Agent Instructions for InfluxDB Toolkit Development

## Project Context

You are assisting Tobias Lippuner in developing **influxdb-toolkit**, a unified Python package that provides a consistent API for both InfluxDB v1.x (InfluxQL) and v2.x (Flux) databases.

**Project Details:**
- **Timeline:** KW 07 (10-14 February 2026)
- **Client:** Reto Marek (HSLU)
- **SAP:** 11.51.00371 - Feldtest Low-Cost Monitoring
- **Goal:** Create a production-ready Python package with Factory pattern architecture

---

## Critical Safety Rules

### 🚨 DO NOT Execute These Operations (Wait for Reto's Return):

1. **NO WRITING** to any InfluxDB database
2. **NO DELETING** data from any database
3. **NO ADMIN OPERATIONS** (create/delete databases, users, buckets)

You may **prepare and test the code** for these operations, but:
- Use mock data and unit tests only
- Do not connect to real databases for write/delete/admin operations
- Comment code clearly: `# TODO: Test with real DB after Reto approves`

### ✅ Safe Operations (Allowed):

- **READ** queries from existing databases
- **EXPLORATION** (list measurements, tags, fields, databases/buckets)
- **UNIT TESTS** with dummy data (no DB connection)
- **CODE DEVELOPMENT** for all features
- **DOCUMENTATION** and examples

---

## Available InfluxDB Test Databases

### InfluxDB v1 (Read-Only Access):

| Host | User | Password | Database | Description |
|------|------|----------|----------|-------------|
| https://influxdbv1.mdb.ige-hslu.io:8086 | tobias | influxdb4ever! | flimatec-langnau-am-albis_v2 | LoRaWAN devices (new structure) |
| https://influxdbv1.mdb.ige-hslu.io:8086 | tobias | influxdb4ever! | meteoSwiss | MeteoSwiss station data |
| https://influxdbv1.mdb.ige-hslu.io:8086 | tobias | influxdb4ever! | wattsup | Counter data for diff(max()) tests |
| http://10.180.26.130 | - | - | mdb-connection-test | Old schema with tags like temp_debrC |

**Note:** v1 Chronograf UI available on port 8888

### InfluxDB v2 (Read-Only Access):

| Host | Token | Bucket | Description |
|------|-------|--------|-------------|
| https://influxdbv2.mdb.ige-hslu.io | sZeVm2YrjjZZvI6D4czmdtNoI5mnvGYk2dtDfkhr17i5HWoGqP97k2c_5ARl4gQsed2atx0xMPe5p3Bh-11icA== | lcm-kwh-legionellen | New DB structure |
| https://influxdbv2.mdb.ige-hslu.io | sZeVm2YrjjZZvI6D4czmdtNoI5mnvGYk2dtDfkhr17i5HWoGqP97k2c_5ARl4gQsed2atx0xMPe5p3Bh-11icA== | meteoSwiss | MeteoSwiss data |

**Note:** v2 UI available on standard port (8086)

---

## Architecture Overview (Based on Analysis)

### Recommended Structure:

New folder next to /estierende Packages called influxdb-toolkit. 


```
influxdb-toolkit/
├── src/
│   └── influxdb_toolkit/
│       ├── __init__.py
│       ├── client.py              # Factory & main entry point
│       ├── base.py                # Abstract base class (ABC)
│       ├── v1/
│       │   ├── __init__.py
│       │   ├── client.py          # InfluxDBClientV1 (InfluxQL)
│       │   └── query_builder.py   # InfluxQL query construction
│       ├── v2/
│       │   ├── __init__.py
│       │   ├── client.py          # InfluxDBClientV2 (Flux)
│       │   └── query_builder.py   # Flux query construction
│       ├── models.py              # Data models (TimeseriesResult, etc.)
│       ├── config.py              # Configuration management
│       └── exceptions.py          # Custom exceptions
├── tests/
│   ├── unit/                      # Mock-based tests (no DB)
│   └── integration/               # Docker-based tests (future)
├── docs/
│   ├── architecture_concept.md
│   ├── influxdb_v1_vs_v2_overview.md
│   └── examples/
│       ├── 01_quickstart.ipynb
│       └── 02_advanced_queries.ipynb
├── .env.example
├── .gitignore
├── pyproject.toml
├── README.md
└── CHANGELOG.md



Always remember that there are some useful examples in /estierende Packages

# Overview of Existing Packages

| Folder | Language | InfluxDB | Description & Key Learnings |
|---|---|---|---|
| 01_pyinfluxdb | Python | v1.x (InfluxQL) | Simple wrapper around `influxdb-python`. Functions: `get_timeseries()`, `get_multiple_timeseries()`, `get_measurements()`, `get_databases()`, `write_timeseries()`. Supports aggregation (`mean`, `median`, `min`, `max`, `diffMax`) and various intervals. Authentication via `credentials.py` (username/password). |
| 02_influxdbpy_github_reto | Python | v1.x + v2.x | Advanced architecture using the Factory Pattern and an abstract base class. `InfluxDBClientFactory.get_client(version, config)` returns the appropriate client. Supports InfluxQL (v1) and Flux (v2). Includes tests. Authentication: user/pass (v1), token (v2) via config dictionary. **THIS PACKAGE IS THE BEST STARTING POINT FOR THE NEW ARCHITECTURE.** |
| 03_influxdbr_wrapper | R | v1.x (InfluxQL) | R wrapper based on the `influxdbr` package. Functions: `influxdbCon()`, `influxdbGetDatabases()`, `influxdbGetMeasurements()`, `influxdbGetFieldKeys()`, `influxdbGetTimeseries()`, `influxdbWriteDf()`. Includes solid aggregation logic and timezone handling (`Europe/Zurich`). Authentication via function parameters. |
| 04_lcm_r_influxdb2 | R | v2.x (Flux) | Native InfluxDB v2 client in R using direct REST API calls (`httr`). Includes Flux query execution, CSV parsing, and a data layer with field-to-unit mappings (Temperature→°C, Humidity→%, CO2→ppm, etc.). Authentication via environment variables (`INFLUXDB_URL`, `INFLUXDB_ORG`, `INFLUXDB_TOKEN`). **GOOD APPROACH FOR AUTH AND FIELD-UNIT MAPPING.** |
| 05_monitoringDB_v1_v2 | Python | v1.x + v2.x | Copy/variant of `02_influxdbpy_github_reto`, adapted for the Monitoring DB. Same architecture (Factory, Base, v1/v2 Clients). Demonstrates that the pattern has been reused successfully in practice. |
| 06_Siemens_BX | Python | v1.x (SSL) | Variant of `01_` with SSL/TLS support (`ssl=True`). Optimized for Siemens Building Automation data. Includes tag filtering and enhanced error handling. Authentication: username/password with SSL via `credentials.py`. |
| 07_miniDataCloudClient | Python | v2.x (Cloud) | Most developed package, includes `setup.py` and README. Uses the Decorator Pattern (`@_database()`) for automatic client lifecycle management. Configuration via `.ini` file. GZIP compression, timezone support (`Europe/Zurich`), and daily chunking for large queries. Functions: `list_measurements()`, `read_measurement()`, `write()`, `delete_measurement()`. **GOOD APPROACH FOR CONFIG MANAGEMENT AND PACKAGING.** |


```

### Design Patterns:

1. **Factory Pattern** (`InfluxDBClientFactory.get_client(version, config)`)
   - Auto-detects v1 vs v2 based on config
   - Returns appropriate client instance

2. **Strategy Pattern** (v1/v2 implementations of abstract base)
   - Abstract base class defines interface
   - v1 and v2 clients implement version-specific logic

3. **Context Manager** (`with` statement support)
   - Ensures proper connection cleanup

---

## Key Learnings from Existing Packages

### Best Architecture: `02_influxdbpy_github_reto`
- Factory pattern with abstract base class
- Clean separation of v1 and v2 logic
- Config-dict based initialization

### Best Configuration: `07_miniDataCloudClient`
- `.ini` file for configuration
- Decorator pattern for connection lifecycle
- GZIP compression support

### Best Authentication: `04_lcm_r_influxdb2`
- Environment variables (INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_TOKEN)
- No hardcoded credentials

Why env vars are a good fit
Security: You avoid hardcoding tokens/URLs in the code or config files that might be committed to Git.

Portability: The same package can run in dev, CI, and production by just changing the environment, not the code.

Convention: Many Python packages and cloud tools (Docker, Kubernetes, CI/CD) expect config via env vars (e.g., DATABASE_URL, API_KEY).

So yes: env vars are a solid default for a PyPI package.

How to make it “best practice”‑grade
To keep it clean and user‑friendly, combine env vars with:

Optional fallbacks / defaults

Read from env, but allow explicit kwargs in the client constructor:

python
url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
org = os.getenv("INFLUXDB_ORG", "my-org")
token = os.getenv("INFLUXDB_TOKEN")
This way users can still pass credentials directly in scripts or tests without polluting their shell.

.env‑file support for local dev

Use python‑dotenv in your config module so that load_dotenv() reads INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_TOKEN from a .env file.

Document that .env should be in .gitignore and not committed.

Clear error messages

If a required env var (e.g., INFLUXDB_TOKEN) is missing and no explicit token is passed, raise a clear ValueError explaining what the user must set.

Documentation

List the supported env vars in README.md and docstrings, including examples:

bash
export INFLUXDB_URL="https://cloud.influxdata.com"
export INFLUXDB_ORG="my-org"
export INFLUXDB_TOKEN="your-token"

When you might want more than env vars
For a serious PyPI package, you can still layer env vars with:

A small config file (influxdb_config.yaml or pyproject.toml section) for non‑secret defaults (e.g., default bucket, default org

### Best Features to Adopt:
- **Field-Unit Mapping** (Temperature→°C, Humidity→%, CO2→ppm)
- **Timezone Handling** (Europe/Zurich)
- **Chunking** for large queries (daily chunks)
- **Aggregation Functions** (mean, median, min, max, diff_max)
- **SSL/TLS Support** (from `06_Siemens_BX`)

---

## Common Functions to Implement

### Priority 1 (This Week):

#### Query Functions:
```python
get_timeseries(
    measurement: str,
    fields: List[str],
    tags: Optional[Dict[str, str]] = None,
    start: datetime,
    end: datetime,
    interval: Optional[str] = None,  # e.g., "5m", "1h", "1d"
    aggregation: Optional[str] = None  # "mean", "median", "min", "max"
) -> pd.DataFrame

query_raw(query: str) -> pd.DataFrame
```

#### Exploration Functions:
```python
# v1: databases, v2: buckets
list_databases() -> List[str]  # v1
list_buckets() -> List[str]    # v2

list_measurements(database: str = None) -> List[str]

get_tags(measurement: str) -> List[str]

get_fields(measurement: str) -> Dict[str, str]  # {field_name: field_type}

Connection helper: influxdbCon(host, port, user, pwd) → context-manager connect() / InfluxDBClient.__enter__/__exit__
List databases / buckets: influxdbGetDatabases() → list_databases() / list_buckets()
List measurements: influxdbGetMeasurements(database) → list_measurements(database=None)
List field keys / types: influxdbGetFieldKeys(measurement) → get_fields(measurement) -> {field: type}
List tag keys/values: get_tags/get_tag_values(measurement, tag) → list_tags(measurement) and list_tag_values(measurement, tag)
Read timeseries (full feature parity): influxdbGetTimeseries(...) with:
valueType (counter vs gauge) handling
valueFactor scaling
func including raw, diffMax, mean, median, etc.
agg chunking logic (daily chunking + re-aggregation)
timezone handling / timeShift
flexible start/stop parsing (support now() - ...)
Write DataFrame: influxdbWriteDf(df, measurement, time_col) → write_dataframe(df, measurement, tag_cols=None, field_cols=None, time_col='time')
Low-level write points: write_points(points, measurement) → write_points(points)
CSV/Flux response parser: parse_influx_csv(csv_content) → robust Flux CSV → DataFrame parser
Range-string parser: parse_range_string(range_string) → canonical datetime or Influx expression
Query raw / execute: influxdb_query(flux_query) → query_raw(query: str) that auto-handles v1/v2
Helpers for query building: get_fieldkey(), get_groupby(), get_tags() → shared query-builder utilities for v1 (InfluxQL) + v2 (Flux)
Chunking utilities: functions to split large time ranges into daily chunks and recombine results
Config / credential helpers: .ini/.env loader + open_config_file_folder() behavior → load_config(), open_config_folder()
Decorator / context injection: R-style decorator that injects client, bucket, org into functions → Python decorator or dependency-injection helper
Counter/consumption helpers: compute_diff_max() and common transforms for counters
Timezone utilities: convert between UTC and local TZ, and apply tz to queries/results
CSV → wide reshape helpers: pivot/reshape functions that R code uses to transform measurements to wide format
Safety stubs for destructive ops: implementations for delete_range, create_bucket/database, create_user but gated (no-op / raise unless test mode)
Admin/exploration extras: get_retention_policies(), get_db/bucket_metadata(), measurement schema introspection


```

### Priority 2 (Prepare, Don't Execute):

#### Write Functions:
```python
write_dataframe(
    df: pd.DataFrame,
    measurement: str,
    tag_columns: Optional[List[str]] = None,
    field_columns: Optional[List[str]] = None,
    time_column: str = "time"
) -> bool

write_points(
    points: List[Dict],
    measurement: str
) -> bool
```

#### Delete Functions (DO NOT EXECUTE):
```python
delete_range(
    measurement: str,
    start: datetime,
    end: datetime,
    tags: Optional[Dict[str, str]] = None
) -> bool
```

#### Admin Functions (DO NOT EXECUTE):
```python
create_database(name: str) -> bool  # v1
create_bucket(name: str, retention: str = "0s") -> bool  # v2

create_user(username: str, password: str) -> bool
grant_privileges(user: str, database: str) -> bool
```

---

## Authentication & Configuration Strategy

### Recommended Hybrid Approach:

1. **Priority Order:**
   - Environment variables (highest)
   - `.env` file
   - Config file (`.ini` or `.yaml`)
   - Defaults (lowest)

2. **Environment Variables:**
   ```bash
   # v1
   INFLUXDB_V1_HOST=https://influxdbv1.mdb.ige-hslu.io
   INFLUXDB_V1_PORT=8086
   INFLUXDB_V1_USER=tobias
   INFLUXDB_V1_PASSWORD=influxdb4ever!
   INFLUXDB_V1_DATABASE=flimatec-langnau-am-albis_v2
   INFLUXDB_V1_SSL=true
   
   # v2
   INFLUXDB_V2_URL=https://influxdbv2.mdb.ige-hslu.io
   INFLUXDB_V2_TOKEN=sZeVm2YrjjZZvI6D4czmdtNoI5mnvGYk2dtDfkhr17i5HWoGqP97k2c_5ARl4gQsed2atx0xMPe5p3Bh-11icA==
   INFLUXDB_V2_ORG=hslu
   INFLUXDB_V2_BUCKET=lcm-kwh-legionellen
   ```

3. **`.env.example` Template:**
   ```ini
   # InfluxDB v1 Configuration
   INFLUXDB_V1_HOST=localhost
   INFLUXDB_V1_PORT=8086
   INFLUXDB_V1_USER=your_username
   INFLUXDB_V1_PASSWORD=your_password
   INFLUXDB_V1_DATABASE=your_database
   INFLUXDB_V1_SSL=false
   
   # InfluxDB v2 Configuration
   INFLUXDB_V2_URL=http://localhost:8086
   INFLUXDB_V2_TOKEN=your_token_here
   INFLUXDB_V2_ORG=your_org
   INFLUXDB_V2_BUCKET=your_bucket
   ```

---

## InfluxDB v1 vs v2 Key Differences

### Conceptual:
| Aspect | v1 | v2 |
|--------|----|----|
| **Data Organization** | Databases → Retention Policies → Measurements | Buckets → Measurements |
| **Query Language** | InfluxQL (SQL-like) | Flux (functional) |
| **Authentication** | Username/Password | Token-based |
| **Time Precision** | Configurable (ns, µs, ms, s) | Nanoseconds (default) |
| **API** | Multiple endpoints | Unified REST API |

### Query Syntax Examples:

**v1 (InfluxQL):**
```sql
SELECT mean("temperature") 
FROM "sensors" 
WHERE "location" = 'room1' 
  AND time >= '2026-02-01T00:00:00Z' 
  AND time < '2026-02-02T00:00:00Z'
GROUP BY time(5m)
TZ('Europe/Zurich')
```

**v2 (Flux):**
```flux
from(bucket: "sensors")
  |> range(start: 2026-02-01T00:00:00Z, stop: 2026-02-02T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "temperature")
  |> filter(fn: (r) => r.location == "room1")
  |> aggregateWindow(every: 5m, fn: mean)
  |> timeShift(duration: 1h, columns: ["_time"])  // Europe/Zurich = UTC+1
```

---

## Code Quality Requirements

### 1. Type Hints (PEP 484):
```python
from typing import List, Dict, Optional, Union
from datetime import datetime
import pandas as pd

def get_timeseries(
    measurement: str,
    fields: List[str],
    start: datetime,
    end: datetime
) -> pd.DataFrame:
    """Fetch time series data."""
    pass
```

### 2. Docstrings (Google Style):
```python
def get_timeseries(measurement, fields, start, end):
    """Fetch time series data from InfluxDB.
    
    Args:
        measurement: Name of the measurement to query
        fields: List of field names to retrieve
        start: Start time (inclusive)
        end: End time (exclusive)
    
    Returns:
        pandas.DataFrame with columns: time, field1, field2, ...
    
    Raises:
        InfluxDBConnectionError: If connection fails
        InfluxDBQueryError: If query is invalid
    
    Example:
        >>> client = InfluxDBClient(version=1, config=config)
        >>> df = client.get_timeseries(
        ...     measurement="temperature",
        ...     fields=["value"],
        ...     start=datetime(2026, 2, 1),
        ...     end=datetime(2026, 2, 2)
        ... )
    """
```

### 3. Error Handling:
```python
class InfluxDBError(Exception):
    """Base exception for influxdb-toolkit."""
    pass

class InfluxDBConnectionError(InfluxDBError):
    """Connection to InfluxDB failed."""
    pass

class InfluxDBQueryError(InfluxDBError):
    """Query execution failed."""
    pass

class InfluxDBAuthenticationError(InfluxDBError):
    """Authentication failed."""
    pass
```

### 4. Logging (not print):
```python
import logging

logger = logging.getLogger(__name__)

def query(self, query_string: str):
    logger.debug(f"Executing query: {query_string}")
    try:
        result = self._execute(query_string)
        logger.info(f"Query returned {len(result)} rows")
        return result
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise InfluxDBQueryError(str(e))
```

### 5. Context Manager:
```python
class InfluxDBClient:
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# Usage:
with InfluxDBClient(version=1, config=config) as client:
    df = client.get_timeseries(...)
```

---

## Testing Strategy

### Unit Tests (Priority 1):
```python
# tests/unit/test_client_v1.py
import pytest
from unittest.mock import Mock, patch
from influxdb_toolkit.v1.client import InfluxDBClientV1

@patch('influxdb_toolkit.v1.client.InfluxDBClient')
def test_get_timeseries_basic(mock_influxdb):
    """Test basic timeseries query."""
    # Mock data
    mock_result = [
        {'time': '2026-02-01T00:00:00Z', 'value': 23.5},
        {'time': '2026-02-01T00:05:00Z', 'value': 23.7}
    ]
    mock_influxdb.return_value.query.return_value.get_points.return_value = mock_result
    
    # Test
    client = InfluxDBClientV1(config={...})
    df = client.get_timeseries(...)
    
    assert len(df) == 2
    assert 'time' in df.columns
    assert 'value' in df.columns


    Testing & mocks: utilities to mock client responses and fixtures equivalent to R test workflows

```

### Integration Tests (Priority 2 - Prepare Only):
```python
# tests/integration/test_influxdb_v1.py
import pytest
import docker

@pytest.fixture(scope="module")
def influxdb_v1_container():
    """Start InfluxDB v1 container for testing."""
    client = docker.from_env()
    container = client.containers.run(
        "influxdb:1.8",
        detach=True,
        ports={'8086/tcp': 8086},
        environment={
            'INFLUXDB_DB': 'testdb',
            'INFLUXDB_ADMIN_USER': 'admin',
            'INFLUXDB_ADMIN_PASSWORD': 'admin'
        }
    )
    yield container
    container.stop()
    container.remove()

def test_query_real_database(influxdb_v1_container):
    """Test against real InfluxDB instance."""
    # Wait for container to be ready
    time.sleep(5)
    
    # Test query
    config = {...}
    client = InfluxDBClientV1(config)
    # ... test code
```

---

## Dependencies

### Core:
```toml
[project]
dependencies = [
    "influxdb>=5.3.1",           # v1 client
    "influxdb-client>=1.36.0",   # v2 client
    "pandas>=2.0.0",
    "python-dotenv>=1.0.0",
]
```

### Development:
```toml
[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "pytest-cov>=4.1.0",
    "black>=23.0.0",
    "flake8>=6.1.0",
    "mypy>=1.5.0",
    "tenacity>=8.2.0",  # Retry logic
]
```

---

## Git Workflow

### Branch Strategy:
- `main` - stable releases
- `develop` - integration branch
- `feature/query-api` - feature branches
- `docs/readme` - documentation branches

### Commit Messages:
```
feat: Add v1 query builder with aggregation support
fix: Handle timezone conversion in v2 client
docs: Update README with authentication examples
test: Add unit tests for get_timeseries
refactor: Extract common query logic to base class
```

---

## Documentation Checklist

### README.md Must Include:
- [ ] Installation instructions (`pip install influxdb-toolkit`)
- [ ] Quick start example (v1 and v2)
- [ ] Configuration guide (.env, env vars)
- [ ] API overview (key methods)
- [ ] Example use cases
- [ ] Link to full documentation
- [ ] Contributing guidelines
- [ ] License information

### API Documentation:
- [ ] All public methods have docstrings
- [ ] Examples in docstrings
- [ ] Type hints for all parameters
- [ ] Return type documentation
- [ ] Exception documentation

---

## Interaction Guidelines

### When I Ask You To:

**"Analyze existing code":**
- Read files from `existierende Packages/` directory
- Identify patterns, best practices, and anti-patterns
- Suggest improvements based on modern Python standards

**"Implement feature X":**
- Ask clarifying questions if needed
- Propose architecture/approach first
- Write clean, well-documented code
- Include unit tests
- Update relevant documentation

**"Test this code":**
- Use mock data for unit tests
- Do NOT connect to real databases for write/delete operations
- Clearly indicate what can be safely tested vs what needs approval

**"Review my code":**
- Check type hints, docstrings, error handling
- Suggest PEP 8 improvements
- Identify potential bugs or edge cases
- Recommend additional tests

### What You Should Do Proactively:

✅ Suggest better approaches when you see opportunities
✅ Ask questions when requirements are unclear
✅ Point out potential issues early
✅ Recommend testing strategies
✅ Suggest documentation improvements

❌ Do NOT execute write/delete/admin operations on real databases
❌ Do NOT skip error handling
❌ Do NOT hardcode credentials
❌ Do NOT use `print()` for logging

---

## Success Criteria (End of Week)

### Must Have:
- [x] Working v1 and v2 query clients
- [x] Factory pattern implementation
- [x] Exploration functions (list_*, get_*)
- [x] Unit tests (>80% coverage)
- [x] README with quick start
- [x] .env.example template
- [x] Type hints and docstrings

### Nice to Have:
- [ ] Jupyter notebook examples
- [ ] CI/CD pipeline (GitHub Actions)
- [ ] Integration test setup (not activated)
- [ ] PyPI-ready pyproject.toml

### Explicitly NOT This Week:
- [ ] Write functions execution on real DB
- [ ] Delete functions execution on real DB
- [ ] Admin operations on real DB
- [ ] PyPI publication

---

## Quick Reference

### Example Usage Pattern:
```python
from influxdb_toolkit import InfluxDBClientFactory
from datetime import datetime, timedelta

# v1 Example
config_v1 = {
    'host': 'influxdbv1.mdb.ige-hslu.io',
    'port': 8086,
    'username': 'tobias',
    'password': 'influxdb4ever!',
    'database': 'flimatec-langnau-am-albis_v2',
    'ssl': True
}

with InfluxDBClientFactory.get_client(version=1, config=config_v1) as client:
    df = client.get_timeseries(
        measurement='temperature',
        fields=['value'],
        start=datetime.now() - timedelta(hours=24),
        end=datetime.now(),
        interval='5m',
        aggregation='mean'
    )
    print(df.head())

# v2 Example
config_v2 = {
    'url': 'https://influxdbv2.mdb.ige-hslu.io',
    'token': 'sZeVm2YrjjZZvI6D4czmdtNoI5mnvGYk2dtDfkhr17i5HWoGqP97k2c_5ARl4gQsed2atx0xMPe5p3Bh-11icA==',
    'org': 'hslu',
    'bucket': 'lcm-kwh-legionellen'
}

with InfluxDBClientFactory.get_client(version=2, config=config_v2) as client:
    measurements = client.list_measurements()
    print(f"Available measurements: {measurements}")
```

---

## Contact & Escalation


**Escalate If:**
- Unclear requirements
- Need approval for DB operations
- Architectural decisions
- Scope changes

---

## Remember:

🎯 **Focus:** Query + Exploration first, Write/Delete/Admin later  
🔒 **Safety:** Read-only operations on real databases  
📚 **Quality:** Type hints, docstrings, tests, documentation  
🏗️ **Architecture:** Factory + Strategy pattern for extensibility  
🔑 **Security:** No hardcoded credentials, use .env + env vars  

**Good luck, and ask questions when in doubt!**