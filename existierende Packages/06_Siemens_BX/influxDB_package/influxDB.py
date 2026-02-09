# -*- coding: utf-8 -*-

# https://influxdb-python.readthedocs.io/en/latest/api-documentation.html#influxdb.InfluxDBClient.query

from influxdb import InfluxDBClient, DataFrameClient
from .credentials import INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USER, INFLUXDB_PWD
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateutil.parser
import re
import sys

# disable warnings according to https://github.com/influxdata/influxdb-python/issues/240
import requests
requests.packages.urllib3.disable_warnings() 

#%%
def get_multiple_timeseries(dfMeasurements,
                            database,
                            datetimeStart = None, 
                            datetimeEnd = None,
                            tags = None,
                            fieldKey = "value",
                            func = "mean", 
                            agg = "5m",
                            fill = None,
                            locTimeZone = "UTC"):
    
    # create empty dataframe for time series
    df = pd.DataFrame(columns=['time']) 

    for index, row in dfMeasurements.iterrows():
        
        measurement = row.values[0]
        
        # print("get influxDB time series: ", measurement)
        dfNew = get_timeseries(measurement, database, datetimeStart, datetimeEnd, tags = tags, fieldKey = fieldKey, func = func, agg = agg, fill=fill, locTimeZone=locTimeZone)
        # print(dfNew)
        
        if(dfNew.empty):
            df[measurement] = float('nan')
        else:
            dfNew.rename(columns={ dfNew.columns[1]: measurement }, inplace = True)
            df = df.merge(dfNew, on='time', how='outer')

    return(df)

#%%
def get_timeseries(measurement,
                   database,
                   datetimeStart = None, 
                   datetimeEnd = None,
                   tags = None,
                   fieldKey = "value",
                   func = "mean", 
                   agg = "5m",
                   fill = None,
                   locTimeZone = "UTC"):

    influxDbClient = InfluxDBClient(host = INFLUXDB_HOST,
                                    port = INFLUXDB_PORT,
                                    database = database,
                                    username = INFLUXDB_USER,
                                    password = INFLUXDB_PWD,
                                    ssl = True,
                                    verify_ssl = False)
    
    if((fill is None) or (fill == "NULL") or (fill == "null") or (fill == "none") or (fill == "None")):
        fill = "null"
    
    if((datetimeStart is None) and (datetimeEnd is None)):
        qry = "SELECT " + get_fieldkey(func, fieldKey) + \
            " FROM " + '"' + measurement + '"'
        if tags != None:
            qry += get_tags(tags)
        
        qry += " GROUP BY " + get_groupby(func, agg) + \
            " Fill(" + str(fill) + ")" + " tz(\'" + \
            locTimeZone + "\')"
        
            
    elif(datetimeStart is None):
        qry = "SELECT " + get_fieldkey(func, fieldKey) + \
            " FROM " + '"' + measurement + '"' + \
            " WHERE time <= \'" + datetimeEnd + "\'" + \
            get_tags(tags) + \
            " GROUP BY " + get_groupby(func, agg) + \
            " Fill(" + str(fill) + ")" + " tz(\'" + \
            locTimeZone + "\')"

    elif(datetimeEnd is None):
        qry = "SELECT " + get_fieldkey(func, fieldKey) + \
            " FROM " + '"' + measurement + '"' + \
            " WHERE time >= \'" + datetimeStart + "\'" + \
            get_tags(tags) + \
            " GROUP BY " + get_groupby(func, agg) + \
            " Fill(" + str(fill) + ")" + " tz(\'" + \
            locTimeZone + "\')"
            
    else:
        qry = "SELECT " + get_fieldkey(func, fieldKey) + \
            " FROM " + '"' + measurement + '"' + \
            " WHERE time >= \'" + datetimeStart + "\'" + \
            " AND time <= \'" + datetimeEnd + "\'" + \
            get_tags(tags) + \
            " GROUP BY " + get_groupby(func, agg) + \
            " Fill(" + str(fill) + ")" + " tz(\'" + \
            locTimeZone + "\')"
    
    df = pd.DataFrame(influxDbClient.query(qry).get_points())
    
    influxDbClient.close()
    
    if(df.empty == False):
        if df.columns[0] != 'time':
            new_order=[df.columns[1],df.columns[0]]
            df = df.reindex(columns=new_order)
     
    return(df)

#%%
def get_results_from_qry( qry,
                          database,
                          locTimeZone = "UTC"):
    # enables to write a custom query 
    
    influxDbClient = InfluxDBClient(host = INFLUXDB_HOST,
                                    port = INFLUXDB_PORT,
                                    database = database,
                                    username = INFLUXDB_USER,
                                    password = INFLUXDB_PWD,
                                    ssl = True,
                                    verify_ssl = False)
    # add timezone
    qry = qry + " tz(\'" + locTimeZone + "\')"
    
    df = pd.DataFrame(influxDbClient.query(qry).get_points())
    return df

#%%
def get_measurements(database):

    influxDbClient = InfluxDBClient(host = INFLUXDB_HOST,
                                    port = INFLUXDB_PORT,
                                    database = database,
                                    username = INFLUXDB_USER,
                                    password = INFLUXDB_PWD,
                                    ssl = True,
                                    verify_ssl = False)
    
    df = pd.DataFrame(influxDbClient.get_list_measurements())
    
    influxDbClient.close()
     
    return(df)

#%%
def get_databases():

    influxDbClient = InfluxDBClient(host = INFLUXDB_HOST,
                                    port = INFLUXDB_PORT,
                                    username = INFLUXDB_USER,
                                    password = INFLUXDB_PWD,
                                    ssl = True,
                                    verify_ssl = False)
    
    df = pd.DataFrame(influxDbClient.get_list_database())
    
    influxDbClient.close()
     
    return(df)

#%%    
def get_fieldkey(func, fieldKey = "value"):
    return {
        'raw': "\"" + fieldKey + "\"",
        'diffMax': "\"difference(max(" + fieldKey + "))\"",
        'mean':  "mean(\"" + fieldKey + "\")",
        'median': "median(\"" + fieldKey + "\")",
        'min': "min(\"" + fieldKey + "\")",
        'max': "max(\"" + fieldKey + "\")",
        'percentile_5': "percentile(\"" + fieldKey + "\",5)",
    }.get(func, "\"" + fieldKey + "\"")

#%%
def get_groupby(func, agg = "1d"):
    return {
        'raw': "NaN",
    }.get(func, "time(" + agg + ")")

#%%
def get_tags(tags):
    tag_string = ""
    if tags != None:
        if len(tags) > 0:
            tag_string = " AND "
            for index, elem in enumerate(tags):
                if index == 0:
                    tag_string += "("
                tag_string += f'\"{elem}\"='
                tag_string += f"'{tags[elem]}'"
                if index != (len(tags)-1):
                    tag_string += " OR "
            tag_string += ") "
    return tag_string
    
#tags = {"key1":"value1", "key2":"value2"}

#%%    
def parse_range_string(range_string, datetime_now = None):
    datetimeParsed = ""
    try:
        if "now()" in range_string : # check if strings contains now()
            
            if datetime_now is None:
                datetimeParsed = datetime.utcnow()
            else:
                datetimeParsed = dateutil.parser.parse(datetime_now)
                
        if " - " in range_string:
            if(range_string.split(" - ")[0] != "now()"):
                datetimeParsed = dateutil.parser.parse(range_string.split(" - ")[0])
            
            diff_list = range_string.split(" - ")[1:]
            
            for diff in diff_list:
                if "minute" in diff:
                    datetimeParsed = datetimeParsed + relativedelta(minutes=-int(re.findall(r'\d+', diff)[0]))
                if "hour" in diff:
                    datetimeParsed = datetimeParsed + relativedelta(hours=-int(re.findall(r'\d+', diff)[0]))         
                if "day" in diff:
                    datetimeParsed = datetimeParsed + relativedelta(days=-int(re.findall(r'\d+', diff)[0]))
                if "month" in diff:
                    datetimeParsed = datetimeParsed + relativedelta(months=-int(re.findall(r'\d+', diff)[0]))  
                if "year" in diff:
                    datetimeParsed = datetimeParsed + relativedelta(years=-int(re.findall(r'\d+', diff)[0]))
        
        else: # datetime expected
            if range_string != "now()":
                # parse datetime
                datetimeParsed = dateutil.parser.parse(range_string)
    except:
        sys.exit(" -> incorrect date string format for influxDB. It should be either 'YYYY-MM-DD hh:mm:ss' or e.g. 'now() - 2 years - 1 month - 1 day - 5 minutes'")
    
    return datetimeParsed    

#%%    
def write_df_to_influxdb(df, labelname, database):
    # writes a dataframe to the influxDB
    influxDbClient = DataFrameClient(host = INFLUXDB_HOST,
                                    port = INFLUXDB_PORT,
                                    database = database,
                                    username = INFLUXDB_USER,
                                    password = INFLUXDB_PWD,
                                    ssl = True,
                                    verify_ssl = False)
    
    
    influxDbClient.write_points(df,labelname)
    return 0

# for testing    
# parse_range_string("now() - a month - 1 day") # should give an error
# parse_range_string("now()")
# parse_range_string("now() - 12 months")
# parse_range_string("now() - 1 month - 1 day")
# parse_range_string("now() - 2 years - 1 month - 1 day - 5 minutes")
# parse_range_string("2022-05-03")
# parse_range_string("2022-05-03 15:01:30")
# parse_range_string("2022-08-16 15:30:45 - 31 days")

#%%

  

