"""
InfluxDB v2 client implementation using Flux.
Supports InfluxQL queries via the v1 compatibility API.
"""

from influxdb_client import InfluxDBClient as InfluxDBClientV2Lib
import pandas as pd
import requests
from .base import InfluxDBClientBase

class InfluxDBClientV2(InfluxDBClientBase):
    """
    Client implementation for InfluxDB v2 using Flux.
    Also supports InfluxQL queries via the v1 compatibility API.

    Methods:
    --------
    get_timeseries(measurement, tags, fieldKey, func, datetimeStart, datetimeEnd, agg, fill, locTimeZone):
        Retrieve a single time series using Flux.

    get_multiple_timeseries(queries, datetimeStart, datetimeEnd, agg, fill, locTimeZone):
        Retrieve multiple time series using Flux.

    get_results_from_qry(qry, locTimeZone):
        Execute a custom query (auto-detects InfluxQL vs Flux).
    """

    def __init__(self, url, token, org, bucket=None, retention_seconds=0):
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
        bucket : str, optional
            The default bucket name (default is None).
        retention_seconds : int, optional
            Retention period in seconds (0 = infinite).
        """
        self.client = InfluxDBClientV2Lib(url=url, token=token, org=org)
        self.url = url.rstrip('/')
        self.token = token
        self.org = org
        self.bucket = bucket
        self.retention_seconds = retention_seconds

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

    def _is_influxql(self, qry):
        """
        Detect if a query is InfluxQL (v1 style) or Flux (v2 style).

        InfluxQL queries typically start with SELECT, SHOW, CREATE, DROP, etc.
        Flux queries typically start with from( or import.
        """
        qry_stripped = qry.strip().upper()
        influxql_keywords = ['SELECT', 'SHOW', 'CREATE', 'DROP', 'DELETE', 'ALTER', 'GRANT', 'REVOKE']
        return any(qry_stripped.startswith(kw) for kw in influxql_keywords)

    def _influx_grouped_query_to_df(self, result):
        """
        Convert InfluxQL grouped query result to DataFrame.
        """
        rows = []
        for series in result.get('results', [{}])[0].get('series', []):
            columns = series.get('columns', [])
            tags = series.get('tags', {})
            for value_row in series.get('values', []):
                row = dict(zip(columns, value_row))
                row.update(tags)
                rows.append(row)
        return pd.DataFrame(rows)

    def _execute_influxql(self, qry, locTimeZone="UTC"):
        """
        Execute an InfluxQL query via the v1 compatibility API.

        Parameters:
        ----------
        qry : str
            The InfluxQL query string.
        locTimeZone : str, optional
            The timezone for the query (default is "UTC").

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the query results.
        """
        qry_with_tz = f"{qry} tz('{locTimeZone}')"

        headers = {
            'Authorization': f'Token {self.token}',
            'Accept': 'application/json'
        }

        params = {
            'q': qry_with_tz,
            'db': self.bucket
        }

        response = requests.get(
            f"{self.url}/query",
            headers=headers,
            params=params
        )

        if response.status_code != 200:
            raise Exception(f"InfluxQL query failed: {response.status_code} - {response.text}")

        result = response.json()

        if 'error' in result.get('results', [{}])[0]:
            raise Exception(f"InfluxQL query error: {result['results'][0]['error']}")

        return self._influx_grouped_query_to_df(result)

    def get_results_from_qry(self, qry, locTimeZone="UTC"):
        """
        Execute a custom query (auto-detects InfluxQL vs Flux).

        Parameters:
        ----------
        qry : str
            The query string (InfluxQL or Flux).
        locTimeZone : str, optional
            The timezone for the query (default is "UTC").

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the query results.
        """
        if self._is_influxql(qry):
            return self._execute_influxql(qry, locTimeZone)
        else:
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
    
    def create_database(self, name):
        """
        Create a new bucket in InfluxDB v2.

        Parameters:
        ----------
        name : str
            The name of the new bucket.

        Returns:
        -------
        bool
            True if the bucket was created successfully, False otherwise.
        """
        try:
            buckets_api = self.client.buckets_api()

            # Check if the bucket already exists
            existing_buckets = buckets_api.find_buckets().buckets
            if any(bucket.name == name for bucket in existing_buckets):
                print(f"Bucket '{name}' already exists.")
                return False

            # Create a new bucket
            retention_rule = None
            if self.retention_seconds > 0:
                retention_rule = [{"type": "expire", "everySeconds": self.retention_seconds}]

            buckets_api.create_bucket(bucket_name=name, org=self.org, retention_rules=retention_rule)
            print(f"Bucket '{name}' created successfully.")
            return True
        except Exception as e:
            print(f"Error creating bucket '{name}': {e}")
            return False