"""Local read-only smoke test for influxdb-toolkit.

Usage:
    py scripts/smoke_read.py --version 1
    py scripts/smoke_read.py --version 2
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import os
import sys

from influxdb_toolkit import InfluxDBClientFactory


def _v1_config_from_env() -> dict:
    host = os.getenv("INFLUXDB_V1_HOST", os.getenv("INFLUXDB_HOST", "localhost"))
    port = int(os.getenv("INFLUXDB_V1_PORT", os.getenv("INFLUXDB_PORT", "8086")))
    username = os.getenv("INFLUXDB_V1_USER", os.getenv("INFLUXDB_USER", ""))
    password = os.getenv("INFLUXDB_V1_PASSWORD", os.getenv("INFLUXDB_PWD", ""))
    database = os.getenv("INFLUXDB_V1_DATABASE", os.getenv("INFLUXDB_DB", ""))
    ssl = os.getenv("INFLUXDB_V1_SSL", "false").lower() in {"1", "true", "yes", "on"}

    if not database:
        raise ValueError("INFLUXDB_V1_DATABASE (or INFLUXDB_DB) is required for v1 smoke test")

    return {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "database": database,
        "ssl": ssl,
        "allow_write": False,
    }


def _v2_config_from_env() -> dict:
    url = os.getenv("INFLUXDB_V2_URL", os.getenv("INFLUXDB_URL", "http://localhost:8086"))
    token = os.getenv("INFLUXDB_V2_TOKEN", os.getenv("INFLUXDB_TOKEN", ""))
    org = os.getenv("INFLUXDB_V2_ORG", os.getenv("INFLUXDB_ORG", ""))
    bucket = os.getenv("INFLUXDB_V2_BUCKET", os.getenv("INFLUXDB_BUCKET", ""))

    if not token or not org or not bucket:
        raise ValueError("INFLUXDB_V2_TOKEN, INFLUXDB_V2_ORG, and INFLUXDB_V2_BUCKET are required for v2 smoke test")

    return {
        "url": url,
        "token": token,
        "org": org,
        "bucket": bucket,
        "allow_write": False,
    }


def run(version: int) -> int:
    config = _v1_config_from_env() if version == 1 else _v2_config_from_env()
    _suppress_v2_pivot_warnings(version)

    # Intentionally avoid context-manager connect() here because v1 ping can fail
    # in environments where /ping is blocked but read queries are still allowed.
    client = InfluxDBClientFactory.get_client(version=version, config=config)
    try:
        measurements = client.list_measurements()
        print(f"version={version} measurements={len(measurements)}")
        if not measurements:
            print("No measurements found.")
            return 0

        measurement = measurements[0]
        print(f"sample measurement: {measurement}")

        tags = client.get_tags(measurement)
        fields = client.get_fields(measurement)
        print(f"tag keys: {tags[:10]}")
        print(f"field keys: {list(fields.keys())[:10]}")

        if fields:
            first_field = list(fields.keys())[0]
            end = datetime.now(UTC)
            start = end - timedelta(hours=24)
            df = client.get_timeseries(
                measurement=measurement,
                fields=[first_field],
                start=start,
                end=end,
                interval="1h",
                aggregation="mean",
                timezone="UTC",
            )
            print(f"timeseries rows: {len(df)}")
            if not df.empty:
                print(df.head(3).to_string(index=False))
    finally:
        try:
            client.close()
        except Exception:
            pass

    return 0


def _suppress_v2_pivot_warnings(version: int) -> None:
    if version != 2:
        return
    try:
        import warnings
        from influxdb_client.client.warnings import MissingPivotFunction

        warnings.simplefilter("ignore", MissingPivotFunction)
    except Exception:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Run read-only smoke checks for influxdb-toolkit")
    parser.add_argument("--version", type=int, choices=[1, 2], required=True, help="InfluxDB major version")
    args = parser.parse_args()
    try:
        return run(args.version)
    except Exception as exc:
        print(f"Smoke test failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


