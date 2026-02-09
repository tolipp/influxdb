"""
InfluxDB v1 client implementation using InfluxQL.
"""

from influxdb import InfluxDBClient
import pandas as pd
import logging  # Ensure logging is imported
from .base import InfluxDBClientBase
from .utils import get_fieldkey, get_groupby, get_tags, build_time_condition

class InfluxDBClientV1(InfluxDBClientBase):
    """
    Client implementation for InfluxDB v1 using InfluxQL.
    
    Methods:
    --------
    get_timeseries(measurement, tags, fieldKey, func, datetimeStart, datetimeEnd, agg, fill, locTimeZone):
        Retrieve a single time series using InfluxQL.
    
    get_multiple_timeseries(queries, datetimeStart, datetimeEnd, agg, fill, locTimeZone):
        Retrieve multiple time series using InfluxQL.
        
    get_results_from_qry(qry, locTimeZone):
        Execute a custom InfluxQL query.
    
    write_points(df, measurement):
        Write a pandas DataFrame to InfluxDB as points.
    
    get_measurements():
        Retrieve the list of available measurements from the database.
    
    get_databases():
        Retrieve the list of available databases from the InfluxDB instance.

    create_database():
        Creates a new database.
    """

    def __init__(self, host, port, user, pwd, database=None, ssl=False, verify_ssl=False):
        """
        Initialize the InfluxDB v1 client.

        Parameters:
        ----------
        host : str
            The InfluxDB host.
        port : int
            The port number.
        user : str
            The username for authentication.
        pwd : str
            The password for authentication.
        database : str
            The database name (default is None).
        ssl : bool, optional
            Whether to use SSL for the connection (default is False).
        verify_ssl : bool, optional
            Whether to verify SSL certificates (default is False).
        """
        self.client = InfluxDBClient(
            host=host, 
            port=port, 
            username=user, 
            password=pwd, 
            database=database, 
            ssl=ssl,
            verify_ssl=verify_ssl
        )

    def get_timeseries(self, measurement, datetimeStart=None, datetimeEnd=None, tags=None, fieldKey="value", func="mean", agg="5m", fill="null", locTimeZone="UTC"):
        """
        Retrieve a single time series using InfluxQL.
        
        Parameters:
        ----------
        measurement : str
            The measurement name.
        datetimeStart : str, optional
            Start time for the query.
        datetimeEnd : str, optional
            End time for the query.
        tags : dict, optional
            Dictionary of tags to filter the query (default is None).
        fieldKey : str, optional
            Field key to aggregate (default is "value").
        func : str, optional
            Aggregation function (default is "mean").
        agg : str, optional
            Aggregation interval (default is "5m").
        fill : str, optional
            Fill method for missing data (default is "null").
        locTimeZone : str, optional
            Timezone for the query (default is "UTC").

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the time series data.
        """
        # Ensure valid fill argument
        if not fill or fill.lower() in ["none", "null", ""]:
            fill = "null"  # Default to null if not specified or invalid
        
        # Build the query based on provided parameters
        qry = f"SELECT {get_fieldkey(func, fieldKey)} FROM \"{measurement}\""
        
        # Add conditions for time range
        if datetimeStart and datetimeEnd:
            qry += f" WHERE time >= '{datetimeStart}' AND time <= '{datetimeEnd}'"
        elif datetimeStart:
            qry += f" WHERE time >= '{datetimeStart}'"
        elif datetimeEnd:
            qry += f" WHERE time <= '{datetimeEnd}'"
        
        # Add tags filtering
        if tags:
            qry += f" {get_tags(tags)}"
        
        # Add GROUP BY clause, fill method, and timezone
        qry += f" GROUP BY {get_groupby(func, agg)}"
        qry += f" FILL({fill})"
        qry += f" TZ('{locTimeZone}')"
        
        # Execute the query and return the DataFrame
        df = pd.DataFrame(self.client.query(qry).get_points())
        
        # Ensure the correct ordering of columns (if 'time' is not the first column)
        if not df.empty and df.columns[0] != 'time':
            new_order = ['time'] + [col for col in df.columns if col != 'time']
            df = df.reindex(columns=new_order)
        
        return df


    def get_multiple_timeseries(self, queries, datetimeStart=None, datetimeEnd=None, agg="5m", fill="null", tags=None, fieldKey="value", func="mean", locTimeZone="UTC"):
        """
        Retrieve multiple time series using InfluxQL.

        Parameters:
        ----------
        queries : list of dict
            List of dictionaries specifying each query's measurement, tags, fieldKey, and func. 
            The possible keys for each query dict are:
                - measurement (str): Required. Name of the measurement.
                - tags (dict, optional): Specific tags for this query. If not provided, global tags will be used.
                - fieldKey (str, optional): Field key to aggregate. If not provided, global fieldKey will be used.
                - func (str, optional): Aggregation function. If not provided, global func will be used.
                - datetimeStart (str, optional): Specific start time for this query. If not provided, global datetimeStart will be used.
                - datetimeEnd (str, optional): Specific end time for this query. If not provided, global datetimeEnd will be used.
                - agg (str, optional): Specific aggregation interval for this query. If not provided, global agg will be used.
                - fill (str, optional): Specific fill method for this query. If not provided, global fill will be used.
        datetimeStart : str, optional
            Start time for the query. This acts as a global default and is applied if individual queries do not specify their own datetimeStart.
        datetimeEnd : str, optional
            End time for the query. This acts as a global default and is applied if individual queries do not specify their own datetimeEnd.
        agg : str, optional
            Aggregation interval (default is "5m"). This acts as a global default and is applied if individual queries do not specify their own agg.
        fill : str, optional
            Fill method for missing data (default is "null"). This acts as a global default and is applied if individual queries do not specify their own fill method.
        tags : dict, optional
            Default tags to filter the query. This acts as a global default and is applied if individual queries do not specify their own tags.
        fieldKey : str, optional
            Default field key to aggregate (default is "value"). This acts as a global default and is applied if individual queries do not specify their own fieldKey.
        func : str, optional
            Default aggregation function (default is "mean"). This acts as a global default and is applied if individual queries do not specify their own func.
        locTimeZone : str, optional
            Timezone for the query (default is "UTC").

        Returns:
        -------
        pd.DataFrame
            DataFrame with the combined time series data.

        Notes:
        ------
        - Global defaults can be overridden by individual queries by specifying the relevant values (tags, fieldKey, func, etc.) in the query dict.
        - If a query does not provide its own datetimeStart, datetimeEnd, agg, or fill, the global values passed to the function will be used.
        """
        df = pd.DataFrame(columns=['time'])
        
        for query in queries:
            # Apply default values if they are not provided in the query
            measurement = query.get('measurement')
            query_tags = query.get('tags', tags)  # Use query-level tags if present, otherwise global tags
            query_fieldKey = query.get('fieldKey', fieldKey)  # Use query-level fieldKey if present, otherwise global fieldKey
            query_func = query.get('func', func)  # Use query-level func if present, otherwise global func

            # Use the global datetimeStart, datetimeEnd, agg, and fill if not provided at query level
            query_datetimeStart = query.get('datetimeStart', datetimeStart)
            query_datetimeEnd = query.get('datetimeEnd', datetimeEnd)
            query_agg = query.get('agg', agg)
            query_fill = query.get('fill', fill)

            # Call get_timeseries for each query
            dfNew = self.get_timeseries(
                measurement=measurement,
                tags=query_tags,
                fieldKey=query_fieldKey,
                func=query_func,
                datetimeStart=query_datetimeStart,
                datetimeEnd=query_datetimeEnd,
                agg=query_agg,
                fill=query_fill,
                locTimeZone=locTimeZone
            )
            
            # Construct a unique series name based on measurement and tags
            series_name = f"{measurement}_" + "_".join([f"{k}={v}" for k, v in query_tags.items()]) + f"_{query_fieldKey}"
            
            if dfNew.empty:
                df[series_name] = float('nan')
            else:
                dfNew.rename(columns={dfNew.columns[1]: series_name}, inplace=True)
                df = df.merge(dfNew, on='time', how='outer')
        
        return df

    def influx_grouped_query_to_df(self, result):
        rows = []
        for (measurement, tags), value_list in result.items():
            for entry in value_list:
                row = dict(entry)  # all fields
                row.update(tags)   # add all tags as columns
                rows.append(row)
        return pd.DataFrame(rows)

    def get_results_from_qry(self, qry, locTimeZone="UTC"):
        """
        Execute a custom InfluxQL query.

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
        qry = f"{qry} tz('{locTimeZone}')"
        # print(f"query: {qry}")
        result = self.client.query(qry)
        # print(f"result: {result}")
        # print("Number of groups:", len(result))
        df = self.influx_grouped_query_to_df(result)
        return df

    def write_points(self, df, measurement, tags=None, fieldKey="value"):
        """
        Write a pandas DataFrame to InfluxDB as points.

        Parameters:
        ----------
        df : pd.DataFrame
            DataFrame containing the data to write. The DataFrame should have a `time` column.
        measurement : str
            The measurement name.
        tags : dict, optional
            Dictionary of tags to include with each point (default is None).
        fieldKey : str, optional
            The field key under which to store the values in the DataFrame (default is "value").

        Returns:
        -------
        bool
            True if points were written successfully, False otherwise.
        """
        points = []

        for _, row in df.iterrows():
            point = {
                "measurement": measurement,
                "time": row['time'],
                "fields": {fieldKey: row[fieldKey] if fieldKey in row else row.drop('time').to_dict()}  # Handle custom fieldKey or all fields
            }

            # Add tags if provided
            if tags:
                point["tags"] = tags

            points.append(point)

        return self.client.write_points(points)

    def get_measurements(self):
        """
        Retrieve the list of available measurements from the InfluxDB database.

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the measurement names.
        """
        try:
            measurements = self.client.get_list_measurements()
            return pd.DataFrame(measurements)
        except Exception as e:
            logging.error(f"Error fetching measurements: {str(e)}")
            return pd.DataFrame()

    def get_databases(self):
        """
        Retrieve the list of available databases from the InfluxDB instance.

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the database names.
        """
        try:
            databases = self.client.get_list_database()
            return pd.DataFrame(databases)
        except Exception as e:
            logging.error(f"Error fetching databases: {str(e)}")
            return pd.DataFrame()
        
    def create_database(self, name):
        """
        Create a new database in InfluxDB v1.

        Parameters:
        ----------
        name : str
            The name of the new database.

        Returns:
        -------
        bool
            True if the database was created successfully, False otherwise.
        """
        try:
            # Check if the database already exists
            existing_dbs = self.client.get_list_database()
            if any(db["name"] == name for db in existing_dbs):
                print(f"Database '{name}' already exists.")
                return False

            # Create a new database
            self.client.create_database(name)
            print(f"Database '{name}' created successfully.")
            return True
        except Exception as e:
            print(f"Error creating database '{name}': {e}")
            return False