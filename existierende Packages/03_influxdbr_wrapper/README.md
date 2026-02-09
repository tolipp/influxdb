# influxdbpy

This package provides a unified interface to interact with **InfluxDB** versions 1.x and 2.x, allowing users to retrieve, write, and query time-series data in a flexible way.
It supports InfluxQL for v1.x and Flux for v2.x, with built-in functionality to handle time-series queries, tags, fieldKeys, and more.

## Features

- **Support for InfluxDB v1.x and v2.x** via a factory-based approach.
- **Query multiple time series** using measurements, tags, aggregation, and time range.
- **Write data points** from Pandas DataFrames to InfluxDB with optional tags and custom field keys.
- **Flexibility** in specifying global defaults or overriding them on a per-query basis.

## Installation

To install the package, clone the repository and install it via `pip`:

```bash
git clone https://github.com/hslu-ige-laes/influxdbpy.git
cd influxdbpy
pip install .
```

## Usage
### Initializing the Client
You can initialize the client for either InfluxDB v1.x or v2.x using the `InfluxDBClientFactory`

```python
from influxdb_multiversion.factory import InfluxDBClientFactory

# Configuration for InfluxDB v1
config_v1 = {
    'host': 'localhost',
    'port': 8086,
    'user': 'username',
    'password': 'password',
    'database': 'your_database'
}

# Configuration for InfluxDB v2
config_v2 = {
    'url': 'http://localhost:8086',
    'token': 'your_token',
    'org': 'your_org',
    'bucket': 'your_bucket'
}

# Initialize the client for v1
client = InfluxDBClientFactory.get_client(version=1, config=config_v1)

# Initialize the client for v2
# client = InfluxDBClientFactory.get_client(version=2, config=config_v2)
```

You can manage your credentials as well in a separate credentials.py file and import them into your project.

Here’s an example of how you can set it up with a `credentials.py` file:

```python
# InfluxDB v1 Credentials
INFLUXDB_HOST = 'localhost'
INFLUXDB_PORT = 8086
INFLUXDB_USER = 'your_username'
INFLUXDB_PWD = 'your_password'
INFLUXDB_DB = 'your_database'

# InfluxDB v2 Credentials
INFLUXDB_V2_URL = 'http://localhost:8086'
INFLUXDB_V2_TOKEN = 'your_token'
INFLUXDB_V2_ORG = 'your_org'
INFLUXDB_V2_BUCKET = 'your_bucket'

```

For enhanced security, it's recommended to manage credentials using environment variables stored in a `.env` file. This keeps sensitive information out of the codebase.

Create a `.env` file in your project root with the following content:

```python
# InfluxDB v1 Credentials
INFLUXDB_HOST=localhost
INFLUXDB_PORT=8086
INFLUXDB_USER=your_username
INFLUXDB_PWD=your_password
INFLUXDB_DB=your_database

# InfluxDB v2 Credentials
INFLUXDB_V2_URL=http://localhost:8086
INFLUXDB_V2_TOKEN=your_token
INFLUXDB_V2_ORG=your_org
INFLUXDB_V2_BUCKET=your_bucket
```

To use environment variables in your code load them at the beginning of your script:

```python
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Access the environment variables
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_PORT = os.getenv("INFLUXDB_PORT")
INFLUXDB_USER = os.getenv("INFLUXDB_USER")
INFLUXDB_PWD = os.getenv("INFLUXDB_PWD")
INFLUXDB_DB = os.getenv("INFLUXDB_DB")

# InfluxDB v2
INFLUXDB_V2_URL = os.getenv("INFLUXDB_V2_URL")
INFLUXDB_V2_TOKEN = os.getenv("INFLUXDB_V2_TOKEN")
INFLUXDB_V2_ORG = os.getenv("INFLUXDB_V2_ORG")
INFLUXDB_V2_BUCKET = os.getenv("INFLUXDB_V2_BUCKET")
```

### Retrieving Time Series Data

1. Single Time Series

You can retrieve a single time series from a measurement by specifying the tags, field key, aggregation function, and time range.

```python
df = client.get_timeseries(
    measurement="01-TR-R01-S1", 
    datetimeStart='2023-01-01T00:00:00Z', 
    datetimeEnd='2023-01-02T00:00:00Z', 
    tags={'sensor': 'tempExternal1_degrC'}, 
    fieldKey='value', 
    func='mean'
)

print(df)
```

2. Multiple Time Series

You can query multiple time series at once, with the ability to specify individual query parameters or rely on global defaults.

```python
queries = [
    {
        'measurement': '01-TR-R01-S1',
        'tags': {'sensor': 'tempExternal1_degrC'},
        'fieldKey': 'value',
        'func': 'mean'
    },
    {
        'measurement': '02-TR-R02-S3',
        'tags': {'sensor': 'tempExternal1_degrC'},
        'fieldKey': 'value',
        'func': 'mean'
    }
]

df = client.get_multiple_timeseries(
    queries, 
    datetimeStart='2023-01-01T00:00:00Z', 
    datetimeEnd='2025-01-02T00:00:00Z', 
    agg="5m", 
    fill="null", 
    tags={'sensor': 'defaultSensor'}, 
    fieldKey='value', 
    func='mean'
)

print(df)
```

### Writing Data Points
The package also allows you to write data points from a Pandas DataFrame to InfluxDB, with options for setting tags and custom field keys.

1. Basic Write

Write a DataFrame to a specific measurement in InfluxDB.

```python
import pandas as pd

df = pd.DataFrame({
    'time': ['2023-09-01T00:00:00Z', '2023-09-01T01:00:00Z'],
    'temperature': [22.5, 23.0]
})
df['time'] = pd.to_datetime(df['time'])

client.write_points(df, measurement='environment', fieldKey='temperature')
```

2. Writing Points with Tags

You can also write points with associated tags.

```python
tags = {
    'location': 'Room1',
    'device': 'sensor1'
}

client.write_points(df, measurement='environment', tags=tags, fieldKey='temperature')
```

### Custom Queries

You can execute custom queries using the get_results_from_qry method.

```python
qry = "SELECT mean(\"value\") FROM \"01-TR-R01-S1\" WHERE time >= '2023-01-01T00:00:00Z' AND time <= '2023-01-02T00:00:00Z' GROUP BY time(5m)"
df = client.get_results_from_qry(qry)
print(df)
```

### General functions

1. Get Available Measurements

Retrieve the list of available measurements in the database.

```python
measurements = client.get_measurements()
print(measurements)
```

2. Get Available Databases

Retrieve the list of available databases in the InfluxDB instance (for InfluxDB v1.x).

```python
databases = client.get_databases()
print(databases)
```

## Advanced Usage

### Global Defaults and Query-Specific Overrides

The package allows setting global defaults for parameters like tags, fieldKey, and func, while individual queries can override these defaults as needed.

Example with Global Defaults:

```python
queries = [
    {'measurement': '01-TR-R01-S1'},
    {'measurement': '02-TR-R02-S3'}
]

# Global defaults for tags, fieldKey, and func
df = client.get_multiple_timeseries(
    queries, 
    datetimeStart='2023-01-01T00:00:00Z', 
    datetimeEnd='2025-01-02T00:00:00Z',
    tags={'sensor': 'defaultSensor'}, 
    fieldKey='value', 
    func='mean'
)

print(df)
```

Example with Query-Specific Overrides:

```python
queries = [
    {'measurement': '01-TR-R01-S1', 'tags': {'sensor': 'sensorA'}, 'fieldKey': 'temperature', 'func': 'max'},
    {'measurement': '02-TR-R02-S3'}
]

# Query-specific overrides for tags, fieldKey, and func
df = client.get_multiple_timeseries(
    queries, 
    datetimeStart='2023-01-01T00:00:00Z', 
    datetimeEnd='2025-01-02T00:00:00Z'
)

print(df)
```