from influxdb_toolkit.client import InfluxDBClientFactory


class Dummy:
    pass


def test_factory_v1_with_client_override():
    config = {
        "host": "localhost",
        "port": 8086,
        "username": "u",
        "password": "p",
        "database": "db",
        "client": Dummy(),
    }
    client = InfluxDBClientFactory.get_client(version=1, config=config)
    assert client is not None


def test_factory_v2_with_client_override():
    config = {
        "url": "http://localhost:8086",
        "token": "t",
        "org": "o",
        "bucket": "b",
        "client": Dummy(),
    }
    client = InfluxDBClientFactory.get_client(version=2, config=config)
    assert client is not None


def test_factory_auto_detect_v2():
    config = {
        "url": "http://localhost:8086",
        "token": "t",
        "org": "o",
        "bucket": "b",
        "client": Dummy(),
    }
    client = InfluxDBClientFactory.get_client(config=config)
    assert client is not None