"""
Base class and abstract methods for InfluxDB clients (v1 and v2).
"""

from abc import ABC, abstractmethod
import pandas as pd

class InfluxDBClientBase(ABC):
    """
    Abstract base class for InfluxDB clients.
    
    Methods:
    --------
    get_timeseries(measurement, tags, fieldKey, func, datetimeStart, datetimeEnd, agg, fill, locTimeZone):
        Abstract method to retrieve a single time series.

    get_multiple_timeseries(queries, datetimeStart, datetimeEnd, agg, fill, locTimeZone):
        Abstract method to retrieve multiple time series.
        
    get_results_from_qry(qry, locTimeZone):
        Abstract method to execute custom queries.
    """
    
    @abstractmethod
    def get_timeseries(self, measurement, tags=None, fieldKey="value", func="mean", datetimeStart=None, datetimeEnd=None, agg="5m", fill=None, locTimeZone="UTC"):
        """
        Retrieve a single time series from InfluxDB.

        Parameters:
        ----------
        measurement : str
            The name of the measurement.
        tags : dict, optional
            A dictionary of tags for filtering.
        fieldKey : str, optional
            The field key to aggregate (default is "value").
        func : str, optional
            The aggregation function to use (default is "mean").
        datetimeStart : str, optional
            The start time for the query.
        datetimeEnd : str, optional
            The end time for the query.
        agg : str, optional
            The time aggregation interval (default is "5m").
        fill : str, optional
            The fill method for missing data (default is None).
        locTimeZone : str, optional
            The timezone for the query (default is "UTC").
        
        Returns:
        -------
        pd.DataFrame
            DataFrame with the time series data.
        """
        pass

    @abstractmethod
    def get_multiple_timeseries(self, queries, datetimeStart=None, datetimeEnd=None, agg="5m", fill=None, locTimeZone="UTC"):
        """
        Retrieve multiple time series from InfluxDB.

        Parameters:
        ----------
        queries : list of dict
            List of query parameters for different time series.
        datetimeStart : str, optional
            The start time for the query.
        datetimeEnd : str, optional
            The end time for the query.
        agg : str, optional
            The time aggregation interval (default is "5m").
        fill : str, optional
            The fill method for missing data (default is None).
        locTimeZone : str, optional
            The timezone for the query (default is "UTC").
        
        Returns:
        -------
        pd.DataFrame
            DataFrame with the multiple time series data.
        """
        pass

    @abstractmethod
    def get_results_from_qry(self, qry, locTimeZone="UTC"):
        """
        Execute a custom query on InfluxDB.

        Parameters:
        ----------
        qry : str
            The InfluxDB query (either InfluxQL or Flux, depending on version).
        locTimeZone : str, optional
            The timezone for the query (default is "UTC").
        
        Returns:
        -------
        pd.DataFrame
            DataFrame with the query results.
        """
        pass

    @abstractmethod
    def get_measurements(self) -> pd.DataFrame:
        """
        Retrieve the list of available measurements from the InfluxDB database.

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the measurement names.
        """
        pass

    @abstractmethod
    def get_databases(self) -> pd.DataFrame:
        """
        Retrieve the list of available databases from the InfluxDB instance.

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the database names.
        """
        pass

    @abstractmethod
    def create_database(self, name):
        """
        Create a new database (v1) or bucket (v2).

        Parameters:
        ----------
        name : str
            The name of the database (v1) or bucket (v2) to be created.

        Returns:
        -------
        bool
            True if the database was created successfully, False otherwise.
        """
        pass