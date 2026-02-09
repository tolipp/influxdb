
# Install required libraries
install.packages("devtools")
library(devtools)

# do not use influxdbtr from CRAN because that is not working anymore...
install_github("hslu-ige-laes/influxdbr")

library(influxdbr)


setwd("C:/Repositories/github/hslu-ige-laes/influxdbr_wrapper")

# This is a handy wrapper around influxdbr with additional functionality for querying datapoints
source("influxdbr_wrapper.R")


# set up influxdb settings
ip = "10.180.26.130"
db = "c312"

# query databases of infuxdb instance
dbs=influxdbGetDatabases(host=ip)

# show measurements of specific database
measurements = influxdbGetMeasurements(host=ip, database=db)
measurement = measurements[1,1]

# get values of specific measurement
datetimeStart = "2022-01-01 00:00:00"
datetimeEnd = NULL
func = "mean"
agg = "1h"
fieldKey = "value"
ts <- influxdbGetTimeseries(host = ip, database = db, measurement = measurement, datetimeStart = datetimeStart, datetimeEnd = datetimeEnd, func = func, agg = agg, fieldKey = fieldKey)


# write values to influxdb
db = "test"
res = influxdbWriteDf(host=ip, database = db, df=ts, measurement = "my_measurement", time_col = "time")

print(influxdbGetMeasurements(host=ip, database=db))
