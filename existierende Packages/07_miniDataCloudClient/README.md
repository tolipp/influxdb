# Python MDCclient

Module to access InfluxDB in the Mini Data Cloud (MDC) and get measurements as Pandas DataFrame for post-processing. This module is based on 
https://github.com/influxdata/influxdb-client-python and helps with additional support for the MDC and timestamp conversion to and from UTC.

Functionality:
+ list available measurements
+ get measurement
+ write additional data as Pandas DataFrame to InfluxDB (e.g. calculated signals from post processing)
+ delete measurement

Note: Importing measurements from InfluxDB is relatively slow because reading must be done in daily chunks due to the limited resources of the VM.

## Install

1.  Install module

    + using GIT
      ```bash
      python -m pip install git+https://gitlab.switch.ch/hslu/research/research-engineering-and-architecture/ime/cctev/mdcclient.git --upgrade
      ```
    
    + using Spyder 
      + Download MDCclient repository as ZIP, extract on local disc
      + Add local path in Spyder using: Tools / PYTHONPATH-Manager

    + Additional Module to install
      ```bash
      pip install 'influxdb-client[ciso]'
      ```

    + Add InfluxDB access token to "influxdb_config.ini" on first use. Open containing folder with:
      ```bash
      import mdcclient as mdc
      mdc.open_config_file_folder()
      ```

## Example

+ Print list of available measurements
  ```bash
  mdc.list_measurements()
  >>> measurements in records : [MeteoSchweiz, VMmonitor, 24-001, 24-002, ...]
  ```

+ Read hourly averages of a measurement as Pandas DataFrame (standard resolution: 10s). Use condition to load measurement only if not already loaded.
  ```bash
  if not 'df' in globals():
      df =mdc.read_measurement(measurement='25-000', start='2025-01-01', stop='2025-01-10', dt='1h', meteo='LUZ')
  ```

+ Write Pandas DataFrame
  ```bash
  import pandas as pd
  df = pd.DataFrame(range(5), columns=['count'], index=[pd.Timestamp('now').round('10s') + pd.timedelta_range(end='0s', periods=5, freq='10s')])
  df['ID'] = 'CALC'
  mdc.write(df, measurement='24-001')
  ```


## Authors

adrian.lauber@hslu.ch
