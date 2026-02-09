"""
Factory class to select the correct InfluxDB client (v1 or v2).
"""

from .client_v1 import InfluxDBClientV1
from .client_v2 import InfluxDBClientV2

class InfluxDBClientFactory:
    """
    Factory class for selecting the correct InfluxDB client (v1 or v2).
    
    Methods:
    --------
    get_client(version, config):
        Return an instance of the appropriate InfluxDB client.
    """

    @staticmethod
    def get_client(version, config):
        """
        Return an instance of the appropriate InfluxDB client based on the version.

        Parameters:
        ----------
        version : int
            The version of InfluxDB (1 for v1, 2 for v2).
        config : dict
            Configuration parameters for the InfluxDB client.

        Returns:
        -------
        InfluxDBClientBase
            An instance of InfluxDBClientV1 or InfluxDBClientV2.
        """
        if version == 1:
            return InfluxDBClientV1(config['host'], config['port'], config['user'], config['password'], config['database'])
        elif version == 2:
            return InfluxDBClientV2(config['url'], config['token'], config['org'], config['bucket'])
        else:
            raise ValueError(f"Unsupported InfluxDB version: {version}")
