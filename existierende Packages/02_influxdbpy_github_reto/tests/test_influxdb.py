import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from dotenv import load_dotenv
from influxdbpy.factory import InfluxDBClientFactory

# Load environment variables from the .env file
load_dotenv()

# Mock configurations for both InfluxDB v1 and v2
config_v1 = {
    'host': os.getenv("INFLUXDB_HOST"),
    'port': int(os.getenv("INFLUXDB_PORT")),
    'user': os.getenv("INFLUXDB_USER"),
    'password': os.getenv("INFLUXDB_PWD"),
    'database': os.getenv("INFLUXDB_DB")
}

config_v2 = {
    'url': os.getenv("INFLUXDB_V2_URL"),
    'token': os.getenv("INFLUXDB_V2_TOKEN"),
    'org': os.getenv("INFLUXDB_V2_ORG"),
    'bucket': os.getenv("INFLUXDB_V2_BUCKET")
}

@pytest.fixture
def mock_client_v1():
    """Fixture for mocking InfluxDB v1 client."""
    with patch('influxdb.InfluxDBClient') as mock:
        yield mock

@pytest.fixture
def mock_client_v2():
    """Fixture for mocking InfluxDB v2 client."""
    with patch('influxdb_client.InfluxDBClient') as mock:
        yield mock

def test_initialize_client_v1(mock_client_v1):
    """Test initializing InfluxDB v1 client."""
    client = InfluxDBClientFactory.get_client(version=1, config=config_v1)
    assert client is not None
    mock_client_v1.assert_called_with(
        host=config_v1['host'], 
        port=config_v1['port'], 
        username=config_v1['user'], 
        password=config_v1['password'], 
        database=config_v1['database'], 
        ssl=True, 
        verify_ssl=False
    )

def test_initialize_client_v2(mock_client_v2):
    """Test initializing InfluxDB v2 client."""
    client = InfluxDBClientFactory.get_client(version=2, config=config_v2)
    assert client is not None
    mock_client_v2.assert_called_with(
        url=config_v2['url'], 
        token=config_v2['token'], 
        org=config_v2['org']
    )

def test_get_timeseries_v1(mock_client_v1):
    """Test getting single time series data from InfluxDB v1."""
    client = InfluxDBClientFactory.get_client(version=1, config=config_v1)
    
    mock_client_v1.return_value.query.return_value.get_points.return_value = [
        {"time": "2023-01-01T00:00:00Z", "value": 23.5},
        {"time": "2023-01-01T01:00:00Z", "value": 24.0}
    ]

    df = client.get_timeseries(
        measurement="01-TR-R01-S1", 
        datetimeStart="2023-01-01T00:00:00Z", 
        datetimeEnd="2023-01-02T00:00:00Z", 
        tags={"sensor": "tempExternal1_degrC"}, 
        fieldKey="value", 
        func="mean"
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.iloc[0]["value"] == 23.5

def test_get_multiple_timeseries_v1(mock_client_v1):
    """Test getting multiple time series data from InfluxDB v1."""
    client = InfluxDBClientFactory.get_client(version=1, config=config_v1)
    
    mock_client_v1.return_value.query.return_value.get_points.return_value = [
        {"time": "2023-01-01T00:00:00Z", "value": 23.5},
        {"time": "2023-01-01T01:00:00Z", "value": 24.0}
    ]

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
        datetimeStart="2023-01-01T00:00:00Z", 
        datetimeEnd="2023-01-02T00:00:00Z", 
        agg="5m", 
        fill="null"
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0

def test_write_points(mock_client_v1):
    """Test writing points to InfluxDB v1."""
    client = InfluxDBClientFactory.get_client(version=1, config=config_v1)
    
    # Mock the write_points method to return True
    mock_client_v1.return_value.write_points.return_value = True

    df = pd.DataFrame({
        'time': ['2023-09-01T00:00:00Z', '2023-09-01T01:00:00Z'],
        'temperature': [22.5, 23.0]
    })
    df['time'] = pd.to_datetime(df['time'])

    success = client.write_points(df, measurement='environment', fieldKey='temperature')

    assert success is True
    mock_client_v1.return_value.write_points.assert_called()

def test_custom_query(mock_client_v1):
    """Test executing custom InfluxQL queries."""
    client = InfluxDBClientFactory.get_client(version=1, config=config_v1)
    
    mock_client_v1.return_value.query.return_value.get_points.return_value = [
        {"time": "2023-01-01T00:00:00Z", "mean": 23.5}
    ]

    qry = "SELECT mean(\"value\") FROM \"01-TR-R01-S1\" WHERE time >= '2023-01-01T00:00:00Z' AND time <= '2023-01-02T00:00:00Z' GROUP BY time(5m)"
    df = client.get_results_from_qry(qry)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["mean"] == 23.5
