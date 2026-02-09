# -*- coding: utf-8 -*-
"""Tutorial on using the InfluxDB client."""
import requests
requests.packages.urllib3.disable_warnings() 
from influxdb import InfluxDBClient

from credentials import INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USER, INFLUXDB_PWD

def main(host=INFLUXDB_HOST, port=INFLUXDB_PORT, user=INFLUXDB_USER, password=INFLUXDB_PWD):
    """Instantiate a connection to the InfluxDB."""
    dbname = 'ZugTH1a'
    host = host
    client = InfluxDBClient(host, port, user, password, dbname, ssl = True, verify_ssl = False)
    dbs = client.get_list_database()
    
if __name__ == '__main__':
    main()
    