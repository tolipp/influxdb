# Work Order: Python Package — InfluxDB Unified Wrapper

**Date:** 6 February 2026  
**Employee:** Tobias Lippuner  
**Period:** 10–14 February 2026 (CW 07)  
**Client / Requestor:** Reto Marek  
**Project:** `influxdb-toolkit` — Unified Python Package for InfluxDB v1.x & v2.x  
**SAP Number:** 11.51.00371  
**SAP Name:** Field Test Low-Cost Monitoring  

Source: :contentReference[oaicite:0]{index=0}

---

## 1. Objective

Develop a unified Python package (working title: **`influxdb-toolkit`**) that acts as a wrapper/abstraction layer supporting both **InfluxDB v1.x** and **InfluxDB v2.x**.

The package should provide data analysts and developers at HSLU with a **consistent API** to:

- query time series data,
- explore structures/metadata,
- write time series data,

…independent of the underlying InfluxDB version.

The architecture must be designed so that a future extension to **InfluxDB v3.x** is possible with **minimal effort**. :contentReference[oaicite:1]{index=1}

---

## 2. Tasks (Prioritized)

### Task 1: Inventory analysis of existing InfluxDB packages
Analyze the existing internal InfluxDB packages and identify:

- commonalities,
- differences,
- redundancies,

with the goal of defining a harmonized basis for the new unified package.

> Tip from the document: Claude.ai can help as an analysis tool. :contentReference[oaicite:2]{index=2}

---

### Task 2: InfluxDB v1 vs. v2 overview — architecture & query languages
Build a compact overview of:

- database structures in **InfluxDB v1 (InfluxQL)** vs **InfluxDB v2 (Flux)**,
- conceptual differences (e.g., *Databases vs Buckets*).

Timebox: **1–2 hours** (aim for a fast high-level understanding).

> Tip from the document: Claude.ai can help as a research tool. :contentReference[oaicite:3]{index=3}

---

### Task 3: Analyze existing data structures (from example databases)
Inspect how data is stored in real projects in v1 and v2:

- measurements,
- tags,
- fields,

and document differences. The new wrapper must **not** assume a single fixed schema; it must remain flexible.

Timebox: **max. 1 hour**. :contentReference[oaicite:4]{index=4}

---

### Task 4: Research existing Python packages
Research existing Python packages for InfluxDB (examples mentioned):

- `influxdb`
- `influxdb-client`
- `influxdb3-python`

Evaluate for each package:

- Who created it? (official vs community)
- Is it actively maintained?
- Which features does it offer?
- What limitations does it have?

Create a short comparison matrix.

Timebox: **max. 2 hours**.

Goal: avoid reinventing something that already exists; past research reportedly didn’t find anything usable, but that may have changed. :contentReference[oaicite:5]{index=5}

---

### Task 5: Concept & architecture
Create a concept document .md  including:

- package file/folder structure
- architectural decision: class-based vs function-based
- API design: which methods/functions the package should provide (derived from Task 1)
- requirements/dependencies (derived from Task 1)
- versioning and compatibility strategy


---

### Task 6: Authentication & configuration management
Evaluate approaches for storing authentication data:

- `.env` file (with `python-dotenv`)
- `.ini` / `.yaml` config file
- direct environment variables
- Python file with constants

Create a short pros/cons comparison.

Recommendation from the document:  
`.env` for local development + environment variables for production/CI is a common standard.

Timebox: **max. 2 hours** — but the decision should be well-founded, not just copied from existing implementations. :contentReference[oaicite:7]{index=7}

---

### Task 7: Package implementation (in order)

#### (a) Query data (getTimeserie, getTimeseries, etc.)
- Query abstraction for **InfluxQL** and **Flux**
- Return format: **pandas DataFrame**

#### (b) Explore databases/buckets
- `getMeasurements()`
- `getTags()`
- `getFields()`
- `getDatabases()` / `getBuckets()`

#### (c) Write data
- batch writing with configurable batch size
- support DataFrames and dictionaries as input
- **Important:** do not perform real writes to a DB until the requestor returns from vacation

#### (d) Delete data
- time-range-based deletion
- **Important:** do not perform real deletions until the requestor returns from vacation

#### (e) Admin functions
- create/delete DBs or buckets
- create/delete users / grant DB access
- **Important:** do not run real admin actions until the requestor returns from vacation :contentReference[oaicite:8]{index=8}

---

## 3. Existing examples & reference material

A handover folder contains a subfolder **“existing packages/”** with **7 implementations** in Python and R.

These were never consolidated into a proper package — consolidating them is the assignment.

Instructions from the document:

- analyze all folders thoroughly,
- use CLI tooling (mentioned: Claude Code in terminal) to identify best patterns,
- **also analyze the R examples** — they contain valuable logic to be transferred into Python.

Example CLI usage suggested in the document:
- `claude --dangerously-skip-permissions`

Example prompts suggested:
- “Analyze all Python and R files in this folder and summarize common functions, patterns, and auth approaches.”
- “Which functions exist in all packages and how do they differ?”
- “Create an architecture proposal for a unified package based on these examples.” :contentReference[oaicite:9]{index=9}

---

## 4. Overview of existing packages

| Folder | Language | InfluxDB | Description & key learnings |
|---|---|---|---|
| `01_pyinfluxdb` | Python | v1.x (InfluxQL) | Simple wrapper around `influxdb-python`. Functions: `get_timeseries()`, `get_multiple_timeseries()`, `get_measurements()`, `get_databases()`, `write_timeseries()`. Supports aggregation (`mean`, `median`, `min`, `max`, `diffMax`) and multiple intervals. Auth via `credentials.py` (username/password). |
| `02_influxdbpy_github_reto` | Python | v1.x + v2.x | Advanced architecture using **Factory Pattern** + abstract base class. `InfluxDBClientFactory.get_client(version, config)` returns the correct client. Supports InfluxQL (v1) and Flux (v2). Includes tests. Auth: user/pass (v1), token (v2) via config dict. **Best starting point for the new architecture.** |
| `03_influxdbr_wrapper` | R | v1.x (InfluxQL) | R wrapper based on `influxdbr`. Functions: `influxdbCon()`, `influxdbGetDatabases()`, `influxdbGetMeasurements()`, `influxdbGetFieldKeys()`, `influxdbGetTimeseries()`, `influxdbWriteDf()`. Strong aggregation logic and timezone handling (`Europe/Zurich`). Auth via function parameters. |
| `04_lcm_r_influxdb2` | R | v2.x (Flux) | Native InfluxDB v2 client in R using direct REST API calls (`httr`). Includes Flux execution, CSV parsing, and a data layer with **field-to-unit mappings** (e.g., temperature→°C, humidity→%, CO2→ppm). Auth via environment variables (`INFLUXDB_URL`, `INFLUXDB_ORG`, `INFLUXDB_TOKEN`). Good approach for auth and field-unit mapping. |
| `05_monitoringDB_v1_v2` | Python | v1.x + v2.x | Copy/variant of `02_influxdbpy_github_reto`, adapted for Monitoring DB. Same architecture (Factory, Base, v1/v2 clients). Demonstrates reusability of the pattern in practice. |
| `06_Siemens_BX` | Python | v1.x (SSL) | Variant of `01_` with SSL/TLS support (`ssl=True`). Optimized for Siemens Building Automation data. Includes tag filtering and extended error handling. Auth: username/password with SSL via `credentials.py`. |
| `07_miniDataCloudClient` | Python | v2.x (Cloud) | Most mature package: includes `setup.py` and README. Uses decorator pattern (`@_database()`) for automatic client lifecycle management. Config via `.ini` file. Includes GZIP compression, timezone support (`Europe/Zurich`), daily chunking for large queries. Functions: `list_measurements()`, `read_measurement()`, `write()`, `delete_measurement()`. Good approach for config management and packaging. |

:contentReference[oaicite:10]{index=10}

---

## 5. Authentication approaches found in the examples

| Method | Used in | Description |
|---|---|---|
| `credentials.py` | `01_`, `06_` | Hardcoded Python file with constants |
| Config dict | `02_`, `05_` | Dictionary passed when creating the client |
| Environment variables | `04_` | `INFLUXDB_URL`, `INFLUXDB_ORG`, `INFLUXDB_TOKEN` |
| `.ini` file | `07_` | `influxdb_config.ini` with a default template |
| Function parameters | `03_` | host/port/user passed directly as parameters |

:contentReference[oaicite:11]{index=11}

---
