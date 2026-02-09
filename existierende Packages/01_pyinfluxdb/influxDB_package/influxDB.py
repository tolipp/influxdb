# -*- coding: utf-8 -*-

# https://influxdb-python.readthedocs.io/en/latest/api-documentation.html#influxdb.InfluxDBClient.query

from influxdb import InfluxDBClient, DataFrameClient
from .credentials import INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USER, INFLUXDB_PWD
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateutil.parser
import re

#%%
def write_timeseries(df, database, measurement, tags=None, protocol="line"):

    influxDbClient = DataFrameClient(host = INFLUXDB_HOST,
                                     port = INFLUXDB_PORT,
                                     database = database,
                                     username = INFLUXDB_USER,
                                     password = INFLUXDB_PWD)
    
    influxDbClient.write_points(dataframe = df, measurement = measurement, tags = tags, database = database, protocol=protocol)
    
    influxDbClient.close()


#%% 
def get_multiple_timeseries(dfMeasurements,
                            database,
                            datetimeStart = None, 
                            datetimeEnd = None,
                            tag = None,
                            tagVal=None,
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
        dfNew = get_timeseries(measurement, database, datetimeStart, datetimeEnd, tag = tag, tagVal = tagVal, fieldKey = fieldKey, func = func, agg = agg, fill=fill, locTimeZone=locTimeZone)
        # print(dfNew)
        
        if(dfNew.empty):
            dfNew = pd.DataFrame(columns=['time', measurement])
        else:
            dfNew.rename(columns={ dfNew.columns[1]: measurement }, inplace = True)
        
        df = df.merge(dfNew, on='time', how='outer')
    
    return(df)

#%%

def get_timeseries(measurement,
                   database,
                   datetimeStart = None, 
                   datetimeEnd = None,
                   tag = None,
                   tagVal=None,
                   fieldKey = "value",
                   func = "mean", 
                   agg = "5m",
                   fill = None,
                   locTimeZone = "UTC"):

    influxDbClient = InfluxDBClient(host = INFLUXDB_HOST,
                                    port = INFLUXDB_PORT,
                                    database = database,
                                    username = INFLUXDB_USER,
                                    password = INFLUXDB_PWD)
    
    if((fill is None) or (fill == "NULL") or (fill == "null") or (fill == "none") or (fill == "None")):
        fill = "null"
    
    tagString = ""
    if ((tag != None) and (tagVal != None)):
        tagString= " AND \"" + tag + "\" = \'" + tagVal + "\'"
    
    if((datetimeStart is None) and (datetimeEnd is None)):
        qry = "SELECT " + get_fieldkey(func, fieldKey) + \
            " FROM " + '"' + measurement + '"' + \
            tagString.replace("AND ", "") + \
            " GROUP BY " + get_groupby(func, agg) + \
            " Fill(" + str(fill) + ")" + " tz(\'" + \
            locTimeZone + "\')"
            
    elif(datetimeStart is None):
        qry = "SELECT " + get_fieldkey(func, fieldKey) + \
            " FROM " + '"' + measurement + '"' + \
            " WHERE time <= \'" + datetimeEnd + "\'" + \
            tagString + \
            " GROUP BY " + get_groupby(func, agg) + \
            " Fill(" + str(fill) + ")" + " tz(\'" + \
            locTimeZone + "\')"

    elif(datetimeEnd is None):
        qry = "SELECT " + get_fieldkey(func, fieldKey) + \
            " FROM " + '"' + measurement + '"' + \
            " WHERE time >= \'" + datetimeStart + "\'" + \
            tagString + \
            " GROUP BY " + get_groupby(func, agg) + \
            " Fill(" + str(fill) + ")" + " tz(\'" + \
            locTimeZone + "\')"
            
    else:
        qry = "SELECT " + get_fieldkey(func, fieldKey) + \
            " FROM " + '"' + measurement + '"' + \
            " WHERE time >= \'" + datetimeStart + "\'" + \
            " AND time <= \'" + datetimeEnd + "\'" + \
            tagString + \
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
def get_measurements(database):

    influxDbClient = InfluxDBClient(host = INFLUXDB_HOST,
                                    port = INFLUXDB_PORT,
                                    database = database,
                                    username = INFLUXDB_USER,
                                    password = INFLUXDB_PWD)
    
    df = pd.DataFrame(influxDbClient.get_list_measurements())
    
    influxDbClient.close()
     
    return(df)

#%%
def get_databases():

    influxDbClient = InfluxDBClient(host = INFLUXDB_HOST,
                                    port = INFLUXDB_PORT,
                                    username = INFLUXDB_USER,
                                    password = INFLUXDB_PWD)
    
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
    }.get(func, "\"" + fieldKey + "\"")

#%%
def get_groupby(func, agg = "1d"):
    return {
        'raw': "NaN",
    }.get(func, "time(" + agg + ")")
    
#%%    
def parse_range_string(range_string, datetime_now = None):
    datetimeParsed = ""
    try:
        if "now()" in range_string : # check if strings contains now()
            
            if datetime_now is None:
                datetimeParsed = datetime.utcnow()
            else:
                datetimeParsed = dateutil.parser.parse(datetime_now)
            
            if range_string != "now()" : # check if not only now()
                diff_list = range_string.split("-")[1:]
                
                for diff in diff_list :
                    if "minute" in diff :
                        datetimeParsed = datetimeParsed + relativedelta(minutes=-int(re.findall(r'\d+', diff)[0]))
                    if "hour" in diff :
                        datetimeParsed = datetimeParsed + relativedelta(hours=-int(re.findall(r'\d+', diff)[0]))         
                    if "day" in diff :
                        datetimeParsed = datetimeParsed + relativedelta(days=-int(re.findall(r'\d+', diff)[0]))
                    if "month" in diff :
                        datetimeParsed = datetimeParsed + relativedelta(months=-int(re.findall(r'\d+', diff)[0]))  
                    if "year" in diff :
                        datetimeParsed = datetimeParsed + relativedelta(years=-int(re.findall(r'\d+', diff)[0]))
                        
        else: # datetime expected
            # parse datetime
            datetimeParsed = dateutil.parser.parse(range_string)
    except:
        print(" -> incorrect date string format for influxDB. It should be either 'YYYY-MM-DD hh:mm:ss' or e.g. 'now() - 2 years - 1 month - 1 day - 5 minutes'")
    
    return datetimeParsed    

# # for testing    
# parse_range_string("now() - a month - 1 day")
# parse_range_string("now()")
# parse_range_string("now() - 12 months")
# parse_range_string("now() - 1 month - 1 day")
# parse_range_string("now() - 2 years - 1 month - 1 day - 5 minutes")
# parse_range_string("2022-05-03")
# parse_range_string("2022-05-03 15:01:30")

#%%

  

