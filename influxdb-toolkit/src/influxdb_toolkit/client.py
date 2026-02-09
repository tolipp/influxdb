"""Factory and entry point for influxdb_toolkit clients."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from .config import resolve_v1_config, resolve_v2_config
from .v1.client import InfluxDBClientV1
from .v2.client import InfluxDBClientV2


class InfluxDBClientFactory:
    """Factory for selecting the correct client implementation."""

    @staticmethod
    def get_client(
        version: Optional[int] = None,
        config: Optional[Mapping[str, Any]] = None,
    ):
        if config is None:
            raise ValueError("config is required")
        client_override = config.get("client") if isinstance(config, Mapping) else None

        if version is None:
            # Auto-detect: v2 uses url/token/org
            if any(k in config for k in ("url", "token", "org")):
                version = 2
            else:
                version = 1

        if version == 1:
            cfg = resolve_v1_config(config)
            return InfluxDBClientV1(
                host=cfg.host,
                port=cfg.port,
                username=cfg.username,
                password=cfg.password,
                database=cfg.database,
                ssl=cfg.ssl,
                verify_ssl=cfg.verify_ssl,
                allow_write=cfg.allow_write,
                client=client_override,
            )
        if version == 2:
            cfg = resolve_v2_config(config)
            return InfluxDBClientV2(
                url=cfg.url,
                token=cfg.token,
                org=cfg.org,
                bucket=cfg.bucket,
                allow_write=cfg.allow_write,
                client=client_override,
            )

        raise ValueError(f"Unsupported InfluxDB version: {version}")
