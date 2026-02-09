"""
InfluxDB v2 client implementation using Flux.
"""

from influxdb_client import InfluxDBClient as InfluxDBClientV2Lib
import pandas as pd
from .base import InfluxDBClientBase

class InfluxDBClientV2(InfluxDBClientBase):
    """
    Client implementation for InfluxDB v2 using Flux.
    
    Methods:
    --------
    get_timeseries(measurement, tags, fieldKey, func, datetimeStart, datetimeEnd, agg, fill, locTimeZone):
        Retrieve a single time series using Flux.
    
    get_multiple_timeseries(queries, datetimeStart, datetimeEnd, agg, fill, locTimeZone):
        Retrieve multiple time series using Flux.
        
    get_results_from_qry(qry, locTimeZone):
        Execute a custom Flux query.
    """

    def __init__(self, url, token, org, bucket):
        """
        Initialize the InfluxDB v2 client.

        Parameters:
        ----------
        url : str
            The URL of the InfluxDB instance.
        token : str
            The authentication token.
        org : str
            The organization name.
        bucket : str
            The bucket name.
        """
        self.client = InfluxDBClientV2Lib(url=url, token=token)
        self.org = org
        self.bucket = bucket

    def get_timeseries(self, measurement, tags=None, fieldKey="value", func="mean", datetimeStart=None, datetimeEnd=None, agg="5m", fill=None, locTimeZone="UTC"):
        """
        Retrieve a single time series using Flux.

        Parameters:
        ----------
        See InfluxDBClientBase.get_timeseries()
        """
        qry = f'''
        from(bucket: "{self.bucket}")
        |> range(start: {datetimeStart}, stop: {datetimeEnd})
        |> filter(fn: (r) => r._measurement == "{measurement}")
        |> filter(fn: (r) => r._field == "{fieldKey}")
        '''
        if tags:
            for k, v in tags.items():
                qry += f'|> filter(fn: (r) => r["{k}"] == "{v}")\n'
        qry += f'|> aggregateWindow(every: {agg}, fn: {func}, createEmpty: {fill is not None})\n'
        result = self.client.query_api().query_data_frame(qry, org=self.org)
        return result

    def get_multiple_timeseries(self, queries, datetimeStart=None, datetimeEnd=None, agg="5m", fill=None, locTimeZone="UTC"):
        """
        Retrieve multiple time series using Flux.

        Parameters:
        ----------
        See InfluxDBClientBase.get_multiple_timeseries()
        """
        df = pd.DataFrame(columns=['_time'])
        for query in queries:
            dfNew = self.get_timeseries(query['measurement'], query.get('tags'), query.get('fieldKey'), query.get('func'), datetimeStart, datetimeEnd, agg, fill, locTimeZone)
            series_name = f"{query['measurement']}_" + "_".join([f"{k}={v}" for k, v in query.get('tags', {}).items()]) + f"_{query.get('fieldKey', 'value')}"
            if dfNew.empty:
                df[series_name] = float('nan')
            else:
                dfNew.rename(columns={dfNew.columns[1]: series_name}, inplace=True)
                df = df.merge(dfNew, on='_time', how='outer')
        return df

    def get_results_from_qry(self, qry, locTimeZone="UTC"):
        """
        Execute a custom Flux query.

        Parameters:
        ----------
        See InfluxDBClientBase.get_results_from_qry()
        """
        df = self.client.query_api().query_data_frame(qry, org=self.org)
        return df

    def get_measurements(self):
        """
        Retrieve the list of available measurements in the bucket.
    
        Returns:
        -------
        pd.DataFrame:
            DataFrame containing available measurements.
        """
        qry = f'''
        from(bucket: "{self.bucket}")
        |> range(start: -1d)  // Query data from the last day
        |> keep(columns: ["_measurement"])
        |> distinct(column: "_measurement")
        |> pivot(rowKey:["_time"], columnKey: ["_measurement"], valueColumn: "_value")
        '''
        df = self.client.query_api().query_data_frame(qry, org=self.org)
        return df.drop_duplicates()

    def get_databases(self):
        """
        Retrieve the list of available buckets (databases) in the InfluxDB instance.

        Returns:
        -------
        pd.DataFrame:
            DataFrame containing available buckets (databases).
        """
        buckets = self.client.buckets_api().find_buckets().buckets
        df = pd.DataFrame([{'bucket_name': b.name, 'bucket_id': b.id} for b in buckets])
        return df