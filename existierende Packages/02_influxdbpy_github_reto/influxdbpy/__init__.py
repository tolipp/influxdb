"""
influxdb_multiversion

A package for interacting with both InfluxDB v1 (InfluxQL) and InfluxDB v2 (Flux).
"""

from .factory import InfluxDBClientFactory

__all__ = ['InfluxDBClientFactory']
