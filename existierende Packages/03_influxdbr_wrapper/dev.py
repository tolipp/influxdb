# -*- coding: utf-8 -*-
"""
Created on Tue Sep 17 01:29:48 2024

@author: retom
"""
from influxdbpy.factory import InfluxDBClientFactory

# Configuration for InfluxDB v1
config_v1 = {
    'host': '10.180.26.130',
    'port': 8086,
    'user': '',
    'password': '',
    'database': 'zentralbahn',
    'ssl':False,
    	'verify_ssl':False 
}

# Configuration for InfluxDB v2
config_v2 = {
    'url': '10.180.26.175:8086',
    'token': 'IVB5YKgW_JNookEgXL6UIWNvHZ73XSlyP1AcGcXo3ejlBdKLyy7KFbTliVnJhVHgcY33OvXDmfgzpXaSb1S2iA==',
    'org': 'hslu',
    'bucket': '_monitoring'
}

# Dynamically select client based on version
version = 2  # Change to 2 for v2
client = InfluxDBClientFactory.get_client(version, config_v1 if version == 1 else config_v2)

client.get_databases()

client.get_measurements()

# Example query
queries = [
    {
        'measurement': '01-TR-R01-S1',
        'tags': {'sensor': 'tempExternal_top_degrC'},
        'fieldKey': 'value',
        'func': 'mean'
    },
    {
        'measurement': '02-TR-R02-S3',
        'tags': {'sensor': 'tempExternal_top_degrC'},
        'fieldKey': 'value',
        'func': 'mean'
    }
]

df = client.get_multiple_timeseries(queries, datetimeStart='2023-01-01T00:00:00Z', datetimeEnd='2025-01-02T00:00:00Z')
print(df)
