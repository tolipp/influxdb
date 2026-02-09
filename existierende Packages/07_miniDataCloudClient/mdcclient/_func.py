# -*- coding: utf-8 -*-
"""
Created on 2024-08-19
@author: Adrian Lauber
"""

import pandas as _pd
import numpy as _np
import os as _os
import sys as _sys
import shutil as _shutil
import platform as _platform
from functools import wraps as _wraps
import subprocess as _subprocess
from influxdb_client import InfluxDBClient as _InfluxDBClient
from influxdb_client import WriteOptions as _WriteOptions
import warnings

# https://github.com/influxdata/influxdb-client-python


def _database(func, *args, **kwargs):
    """
    Decorator that injects InfluxDB configuration and client into a function.

    Behavior
    --------
    - Looks for `influxdb_config.ini` in the module directory.
    - If present: reads `bucket`, `org`, `tz` and creates an InfluxDBClient.
      The client and metadata are injected into the wrapped function via **kwargs
      as `client`, `bucket`, `org`, `tz`. Client is closed after the call.
    - If missing: copies `_default_influxdb_config.ini` to `influxdb_config.ini`,
      prints a hint to open the folder, and exits the interpreter.

    Parameters
    ----------
    func : callable
        Function to decorate.

    Returns
    -------
    callable
        Wrapped function with InfluxDB context injected.

    Notes
    -----
    The wrapped function should accept arbitrary `**kwargs`.
    """
    @_wraps(func)
    def wrapper(*args, **kwargs):
        p = _os.path.dirname(_os.path.abspath(__file__))
        name = 'influxdb_config.ini'
        p += _os.sep + name
        if _os.path.exists(p):
            df = _pd.read_csv(p, sep='=')
            client = _InfluxDBClient.from_config_file(p, enable_gzip=True)
            kwargs.update({'bucket': df.loc['bucket', '[influx2]'],
                            'org': df.loc['org', '[influx2]'],
                            'tz': df.loc['tz', '[influx2]'],
                            'client': client})
            ret = func(*args, **kwargs)
            client.close()
            return ret
        else:
            _shutil.copyfile(p.replace(name, '_default_' + name), p)
            print('\n***************** influxdb_config.ini **************\n' +
                  'use mdc.open_config_file_folder() and set token\n' +
                  '****************************************************\n')
            _sys.exit()
    return wrapper

    
def open_config_file_folder():
    """
    Open the folder that contains this module and the `influxdb_config.ini`.

    Purpose
    -------
    Convenience helper to quickly locate and edit `influxdb_config.ini`
    (Token, URL, bucket, org, tz).

    Behavior
    --------
    - On Linux: uses `xdg-open` to open the folder.
    - On Windows: opens Explorer and selects `__init__.py`.

    Returns
    -------
    None
    """
    p = _os.path.dirname(_os.path.abspath(__file__)) 
    if _platform.uname().sysname == 'Linux':
        _subprocess.Popen(['xdg-open', p])
    else:
        _subprocess.Popen(r'explorer /select, ' + p + _os.sep + 
                          '__init__.py')


def read_measurement(measurement, start=None, stop=None, meteo=None, 
                     long_name=False, **kwargs):
    """
    Read and reshape a measurement into wide format (IDs as columns).

    Parameters
    ----------
    measurement : str
        Measurement name (e.g., "24-000", "MeteoSchweiz", "VMmonitor").
    start : str or None, optional
        Start date/time (e.g., "2024-12-12"). If None, earliest available is used.
    stop : str or None, optional
        Stop date/time. If None, latest available is used.
    meteo : str or None, optional
        MeteoSchweiz station code (e.g., "LUZ"). If given, station data are
        aligned to the measurement cadence and merged.
    long_name : bool, optional
        If True, column names become e.g. `T01-1_WÜ1`; otherwise `T_WÜ1`.
    **kwargs : dict
        Passed down to `read_raw` (usually injected by @_database).

    Returns
    -------
    pandas.DataFrame
        Time-indexed wide table with numeric signals (e.g., T, V, Q) per ID/Position.

    Notes
    -----
    - Uses an internal `keep` list to retain essential columns.
    - If `meteo` provided, resamples and merges by median timestep; fills
      with `bfill` for high-resolution (<=10 min) else `mean`.

    Examples
    --------
    >>> df = read_measurement("24-000", start="2024-01-01", stop="2024-01-31")
    >>> df.columns[:5]
    Index(['T_WÜ1', 'T_WÜ2', 'V_EG', 'Q_Leitung', ...], dtype='object')
    """
    k = '|> keep(columns:["_time", "ID", "Position", "T", "V", "Q"])'
    df = read_raw(measurement, start, stop, KEEP=k, **kwargs)
    position = df.pop('Position').replace('nan', '-')
    if long_name:
        df['ID'] += ['_' + n if len(n) > 1 else '' for n in position]
    else:
        df['ID'] = _np.where(position.str.len() > 1, 
                             df['ID'].str[:1] + '_' + position, df['ID'])
        
    signals = df.select_dtypes(include='float').columns
    
    df_tot = _pd.DataFrame()
    for n in signals:
        dfn = df[[n, 'ID']].dropna().pivot(columns='ID')
        dfn.columns = dfn.columns.droplevel(0)
        df_tot = df_tot.combine_first(dfn)
    df = df_tot
    df.columns.name = ''
    
    if not meteo == None:
        dt = df.index.diff().median()
        start = df.index.min().date()
        stop = df.index.max().date() + _pd.Timedelta('1d')
        dfx = read_meteoschweiz(meteo, start, stop).add_suffix('_' +  meteo)
        dfx = dfx.resample(dt)
        if dt <= _pd.Timedelta('10m'):
            df = df.combine_first(dfx.bfill())
        else:
            df = df.combine_first(dfx.mean())
    return df


def read_measurement_v0(measurement, start=None, stop=None, meteo=None, **kwargs):
    """
    Reader für Messungen (Q/V/T), ohne Spaltenänderung oder Namens-Erweiterung.
    Die Spaltennamen bleiben exakt wie in der Datenbank: ID + ggf. Position.

    Beispiel:
      ID = "Q04-2", Position = "Coater_3"  → Spalte: "Q04-2_Coater_3"
      ID = "Q01",   Position = "-"         → Spalte: "Q01"
    """
    import pandas as _pd
    import warnings

    k = '|> keep(columns: ["_time","ID","Position","1","2","Position1","Position2","T","Q","V"])'
    df_raw = read_raw(measurement, start, stop, KEEP=k, **kwargs)

    if df_raw is None or df_raw.empty:
        warnings.warn(f"[read_measurement_v0] No data found for '{measurement}'.")
        return _pd.DataFrame()

    if "_time" in df_raw.columns:
        df_raw = df_raw.set_index("_time")

    colmap: dict[str, _pd.Series] = {}
    numeric_fields = [f for f in ("1", "2", "T", "Q", "V") if f in df_raw.columns]

    if "Position" not in df_raw.columns:
        df_raw["Position"] = "-"

    for fld in numeric_fields:
        for id_, df_id in df_raw.groupby("ID"):
            tmp = df_id[[fld, "Position"]].dropna(subset=[fld])
            if tmp.empty:
                continue
            for pos, g in tmp.groupby("Position"):
                s = g[fld].copy()
                name = f"{id_}_{pos}" if pos != "-" else f"{id_}"
                s.name = name
                # NEW: Jede Series auf eindeutige Zeitstempel reduzieren
                if s.index.has_duplicates:
                    s = s.groupby(level=0).first()   # alternativ: .mean()/.last()

                if name in colmap:
                    colmap[name] = colmap[name].combine_first(s)
                else:
                    colmap[name] = s

    if not colmap:
        warnings.warn("[read_measurement_v0] No valid numeric data after grouping.")
        return _pd.DataFrame()

    df = _pd.concat(colmap.values(), axis=1)
    df.columns = list(colmap.keys())

    if df.index.has_duplicates:
        df = df.groupby(df.index).first()
    df = df.sort_index()

    # ---- Meteo-Merge optional ----
    if meteo is not None and not df.empty:
        dt = _pd.to_timedelta(_pd.Series(df.index).diff().dropna().median())
        if _pd.isna(dt) or dt <= _pd.Timedelta(0):
            warnings.warn("[read_measurement_v0] Cannot determine median interval for meteo merge.")
            return df

        start_m = df.index.min().floor("D")
        stop_m = df.index.max().floor("D") + _pd.Timedelta(days=1)

        try:
            dfx = read_meteoschweiz(meteo, start_m, stop_m)
        except Exception as exc:
            warnings.warn(f"[read_measurement_v0] Meteo read failed: {exc}")
            return df

        if dfx is None or dfx.empty:
            warnings.warn("[read_measurement_v0] Meteo data empty; skipping merge.")
            return df

        dfx = dfx.resample(dt)
        dfx = dfx.bfill() if dt <= _pd.Timedelta("10min") else dfx.mean()
        df = df.combine_first(dfx)

    return df



def read_meteoschweiz(Station: str = "LUZ", start: str | None = None, stop: str | None = None) -> _pd.DataFrame:
    """
    Read MeteoSchweiz data for a given station.

    Parameters
    ----------
    Station : str, optional
        Station code (e.g., "LUZ"). Default is "LUZ".
    start, stop : str or None, optional
        Time bounds; if None, determined by underlying data limits.

    Returns
    -------
    pandas.DataFrame
        Time-indexed DataFrame of meteo variables.
        - If data exist: 'Station' column is dropped.
        - If no data: returns empty DataFrame with warning.

    Examples
    --------
    >>> dfm = read_meteoschweiz("LUZ", "2024-04-01", "2024-04-30")
    """
    FILTER = f'|> filter(fn: (r) => r.Station == "{Station}")'
    df = read_raw("MeteoSchweiz", start, stop, FILTER=FILTER)

    if df is None or df.empty:
        warnings.warn(
            f"[read_meteoschweiz] No data for station '{Station}' between {start} and {stop}."
        )
        # Gib leeres DF mit Zeitindex zurück, statt None
        return _pd.DataFrame()

    if "Station" in df.columns:
        df = df.drop(columns=["Station"])

    return df



def read_sensor_metadata(measurement, ID, start=None, stop=None):
    """
    Read raw records for a specific sensor ID within a measurement.

    Parameters
    ----------
    measurement : str
        Measurement name (e.g., "24-000").
    ID : str
        Sensor identifier (e.g., "T01").
    start, stop : str or None, optional
        Time bounds; if None, inferred as in `read_raw`.

    Returns
    -------
    pandas.DataFrame
        Raw, pivoted data (time-indexed) filtered to the given sensor ID.

    Examples
    --------
    >>> df_meta = read_sensor_metadata("24-000", "T01",
    ...                                start="2024-06-01", stop="2024-06-07")
    """
    FILTER = f'|> filter(fn: (r) => r.ID == "{ID}")'
    return read_raw(measurement, start, stop, FILTER=FILTER)


@_database
def _get_limit(measurement, timestamp='first', **kwargs):
    """
    Get earliest or latest date where data exist for a measurement.

    Parameters
    ----------
    measurement : str
        Measurement name.
    timestamp : {'first', 'last'}, optional
        Which bound to return. 'first' returns the earliest date.
        'last' returns (latest date + 1 day) for convenience in slicing.
    **kwargs : dict
        Injected by @_database: client, tz, bucket, org.

    Returns
    -------
    datetime.date
        Earliest date (if 'first') or (latest date + 1 day) if 'last'.

    Notes
    -----
    Queries the last 365 days.
    """
    query_api = kwargs['client'].query_api()
    df = query_api.query_data_frame(f'''
              from(bucket:"records")
                |> range(start: -365d, stop: 0d)
                |> filter(fn: (r) => r._measurement == "{measurement}")
                |> {timestamp}(column: "_time")
                |> pivot(rowKey:["_time"], columnKey: ["_field"],
                        valueColumn: "_value")
                ''')
    if type(df) == list:
        df = _pd.concat(df)
    df.index = _pd.to_datetime(df.pop('_time'))
    df.index = (df.index.tz_convert(kwargs['tz'])
                .tz_localize(None).round('1s'))
    df = df.sort_index()
    if timestamp == 'first':
        return df.index.min().date()
    else:
        return df.index.max().date() + _pd.Timedelta('1d')

@_database
def read_raw(measurement, start=None, stop=None, FILTER='', KEEP='', **kwargs):
    """
    Low-level reader that queries day-by-day and pivots fields to columns.

    Strategy
    --------
    - Iterates daily ranges to avoid overloading low-resourced VMs.
    - Applies optional Flux `FILTER` and `KEEP` snippets.
    - Pivots by `_field` into columns, converts timestamps to local `tz`,
      drops aux columns, combines daily chunks via `combine_first`, and
      slices to [start:stop] at the end.

    Parameters
    ----------
    measurement : str
        Name of the measurement to read.
    start, stop : str or None, optional
        Time bounds; if None, inferred via `_get_limit('first'/'last')`.
    FILTER : str, optional
        Additional Flux filter clause (e.g., '|> filter(fn: (r) => r._field =~ /T_/ )').
    KEEP : str, optional
        Flux keep clause to retain specific columns after pivot.
    **kwargs : dict
        Injected by @_database: client, tz, bucket, org.

    Returns
    -------
    pandas.DataFrame
        Time-indexed DataFrame of raw values and metadata columns.

    Raises
    ------
    ValueError
        If `client` or `tz` are not provided.

    Warnings
    --------
    Emits warnings on daily query failures or index conversion issues.

    Examples
    --------
    >>> df = read_raw("24-000", start="2024-01-01", stop="2024-01-02",
    ...               FILTER='|> filter(fn: (r) => r.ID == "T01")',
    ...               KEEP='|> keep(columns: ["_time","ID","T","Position"])')
    """
    client = kwargs.get('client')
    if client is None:
        raise ValueError("[read_raw] 'client' must be provided in kwargs.")

    tz = kwargs.get('tz')
    if tz is None:
        raise ValueError("[read_raw] 'tz' must be provided in kwargs.")

    query_api = client.query_api()

    # Start/stop Limits bestimmen
    start = _get_limit(measurement, 'first') if start is None else start
    stop = _get_limit(measurement, 'last') if stop is None else stop

    start_days = -(_pd.Timestamp('now') - _pd.Timestamp(start)).ceil('d').days
    stop_days = -(_pd.Timestamp('now') - _pd.Timestamp(stop)).ceil('d').days + 1

    df_tot = _pd.DataFrame()

    for i in range(start_days, stop_days):
        query = f'''
            from(bucket:"records")
              |> range(start: {i}d, stop: {i+1}d)
              |> filter(fn: (r) => r._measurement == "{measurement}")
              {FILTER}
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> drop(columns: ["_start", "_stop"])
              {KEEP}
        '''
        try:
            df = query_api.query_data_frame(query)
        except Exception as e:
            warnings.warn(f"[read_raw] Query failed for day offset {i}: {e}")
            continue

        if isinstance(df, list):
            df = _pd.concat(df, ignore_index=True)

        if not df.empty:
            try:
                df.index = _pd.to_datetime(df.pop('_time'))
                df.index = (df.index
                            .tz_convert(tz)
                            .tz_localize(None)
                            .round('1s'))
                df.index.name = 'time'
            except Exception as e:
                warnings.warn(f"[read_raw] Failed to process index for day offset {i}: {e}")
                continue

            # Drop auxiliary columns if present
            for n in ['result', 'table', '_measurement']:
                if n in df.columns:
                    df.pop(n)

            if df_tot.empty:
                df_tot = df
            else:
                df_tot = df_tot.combine_first(df)

    if df_tot.empty:
        warnings.warn(f"[read_raw] No data collected for measurement '{measurement}' between {start} and {stop}.")
        return df_tot

    # Slicing nur wenn sinnvoll
    try:
        df_tot = df_tot.loc[start:stop]
    except Exception as e:
        warnings.warn(f"[read_raw] Failed to slice final DataFrame: {e}")

    return df_tot

def measurement2excel(path, measurement, start=None, stop=None, meteo=None):
    """
    Export a single measurement to two Excel files (processed + raw by ID).

    Files
    -----
    1) `backup_<date>_<measurement>_<meteo>.xlsx`
       Wide, processed signals merged with optional meteo data.
    2) `backup_raw_<date>_<measurement>_<meteo>.xlsx`
       Raw data split into sheets per `ID` (columns with only NaN dropped).

    Parameters
    ----------
    path : str
        Target directory for Excel files (created if necessary).
    measurement : str
        Measurement name (e.g., "24-000").
    start, stop : str or None, optional
        Time bounds for export.
    meteo : str or None, optional
        MeteoSchweiz station (e.g., "LUZ").

    Returns
    -------
    None

    Examples
    --------
    >>> measurement2excel("D:/Backups", "24-000", "2024-01-01", "2024-01-31", meteo="LUZ")
    """
    date = str(_pd.Timestamp.now().date())
    path = path.rstrip(_os.sep) + _os.sep
    p = (path + f'backup_{date}_{measurement}_{meteo}.xlsx')
    
    df = read_measurement(measurement, start, stop, meteo)
    df.to_excel(p)
    
    p = (path + 
         f'backup_raw_{date}_{measurement}_{meteo}.xlsx')
    df = read_raw(measurement, start, stop)
    with _pd.ExcelWriter(p) as writer:  
        for ID in df['ID'].unique():
            df_id = df[df['ID'] == ID].dropna(axis=1, how='all')
            df_id.to_excel(writer, sheet_name=ID)

def _sanitize_filename(name: str) -> str:
    """
    Make a string safe for use as a filename on common OSes.

    Parameters
    ----------
    name : str

    Returns
    -------
    str
        Sanitized filename (forbidden characters replaced with '_', trimmed).
    """
    if name is None:
        return "Unknown"
    s = str(name)
    for bad in '<>:"/\\|?*[]:*?/\\':
        s = s.replace(bad, '_')
    s = s.strip().rstrip('.')
    return s[:120] or "Unknown"

def measurement2csv(path, measurement, start=None, stop=None, meteo=None):
    """
    Export a single measurement into CSV files.

    Output structure
    ----------------
    <path>/
      backup_<YYYY-MM-DD>_<measurement>[_<meteo>]/
        processed.csv              # wide, processed signals (optionally merged with meteo)
        raw_by_id/
          <ID>.csv                 # raw, one CSV per sensor ID (metadata cols kept; empty-only cols dropped)

    Parameters
    ----------
    path : str
        Base directory for the export; subfolders will be created if needed.
    measurement : str
        Measurement name (e.g., "24-000", "MeteoSchweiz", "VMmonitor").
    start, stop : str or None, optional
        Time bounds (e.g., "2024-01-01", "2024-12-31"). If None, limits are inferred by `read_raw`.
    meteo : str or None, optional
        MeteoSchweiz station code (e.g., "LUZ") to merge into `processed.csv`.

    Returns
    -------
    None

    Notes
    -----
    - CSV ist flach (keine Sheets). Daher werden die Rohdaten pro ID in einzelne Dateien geschrieben.
    - Index (Zeit) wird als erste CSV-Spalte geschrieben.
    """
    date = str(_pd.Timestamp.now().date())
    base = path.rstrip(_os.sep) + _os.sep + f'backup_{date}_{_sanitize_filename(measurement)}'
    if meteo:
        base += f'_{_sanitize_filename(meteo)}'
    if not _os.path.exists(base):
        _os.makedirs(base)

    # Processed (wide) CSV
    df_proc = read_measurement(measurement, start=start, stop=stop, meteo=meteo)
    if df_proc is not None and not df_proc.empty:
        df_proc.to_csv(base + _os.sep + "processed.csv", index=True, encoding="utf-8")

    # Raw by ID (one CSV per ID)
    df_raw = read_raw(measurement, start=start, stop=stop)
    if df_raw is not None and not df_raw.empty:
        raw_dir = base + _os.sep + "raw_by_id" + _os.sep
        _os.makedirs(raw_dir, exist_ok=True)
        if 'ID' in df_raw.columns:
            for ID, df_id in df_raw.dropna(subset=['ID']).groupby('ID', dropna=True):
                df_id = df_id.dropna(axis=1, how='all')
                fn = raw_dir + f"{_sanitize_filename(ID)}.csv"
                df_id.to_csv(fn, index=True, encoding="utf-8")
        else:
            # Kein ID-Feld vorhanden → komplette Tabelle als eine Datei
            df_raw.to_csv(raw_dir + "data.csv", index=True, encoding="utf-8")

def _pivot_no_agg_by_id(dfx: _pd.DataFrame, value_col: str) -> _pd.DataFrame:
    """
    Pivot ohne Aggregation (Rohdaten erhalten):
    Bei mehreren Rohpunkten je (Zeit, ID) wird eine Sequenz 'seq' (0,1,2,…) vergeben,
    damit alle Punkte erhalten bleiben (Index wird (time, seq)).

    Parameters
    ----------
    dfx : pandas.DataFrame
        Index = Zeit, Spalten enthalten mindestens ['ID', value_col].
    value_col : str
        Numerische Spalte, die pivotiert werden soll (z. B. 'T').

    Returns
    -------
    pandas.DataFrame
        Wide-Table mit MultiIndex ('time','seq') und Spalten = IDs.
    """
    if 'ID' not in dfx.columns:
        return _pd.DataFrame(index=dfx.index.unique()).sort_index()

    # Nur benötigte Spalten, ungültige IDs verwerfen
    tmp = dfx[['ID', value_col]].dropna(subset=['ID']).copy()
    if tmp.empty:
        return _pd.DataFrame(index=dfx.index.unique()).sort_index()

    # Index (Zeit) zu Spalte machen – eindeutiger Name 'time'
    tmp = tmp.reset_index()
    # Der Name der ersten Spalte ist der ehemalige Indexname (oft 'time' oder None)
    time_col = tmp.columns[0]
    if time_col != 'time':
        tmp = tmp.rename(columns={time_col: 'time'})

    # Sequenz je (time, ID) → 0,1,2,... für Duplikate
    tmp['seq'] = tmp.groupby(['time', 'ID']).cumcount()

    # Pivot ohne Aggregation – jede (time, seq) Zeile repräsentiert einen Rohpunkt
    piv = tmp.pivot(index=['time', 'seq'], columns='ID', values=value_col)
    piv = piv.sort_index()
    piv.columns.name = ''
    return piv

def backup_cloud(path, days=365, write_multiindex=False, fields=None):
    """
    Backup: pro Measurement *genau eine CSV* im Wide-Format (IDs als Spalten),
    **ohne Aggregation** und **ohne Unterordner**. Rohdaten bleiben vollständig erhalten.

    Vorgehen
    --------
    - Zeitraum: [now - days, now]
    - Für jedes Measurement:
        1) `read_raw(...)` laden.
        2) Numerische Felder bestimmen (z. B. T, Q, V).
        3) Für jedes Feld *ohne Aggregation* pivotieren:
           - Mehrfacheinträge je (Zeit, ID) werden mit einer Sequenz `seq` (0,1,2,…) unterscheidbar gemacht.
           - Ergebnisindex ist (time, seq). So bleiben alle Rohwerte erhalten.
        4) Bei mehreren Feldern wird je Feld ein Präfix gesetzt (z. B. 'T_', 'Q_').
        5) Es entsteht genau *eine* CSV-Datei pro Measurement.

    Spezialfälle
    ------------
    - Falls kein 'ID' existiert, aber 'Station' (z. B. MeteoSchweiz), wird 'Station' als 'ID' verwendet.
    - Gibt es keine numerischen Felder, wird als Fallback die Roh-Tabelle (long) exportiert.
    - `write_multiindex=False` (Default): schreibt time/seq als gewöhnliche Spalten ins CSV.
      Setze `write_multiindex=True`, wenn der MultiIndex im CSV beibehalten werden soll.
    - Mit `fields=['T','Q']` kann man optional auf bestimmte Felder einschränken (Default: alle numerischen).

    Parameter
    ---------
    path : str
        Zielbasisordner; es wird ein datierter Ordner erzeugt.
    days : int, optional (Default: 365)
        Lookback in Tagen.
    write_multiindex : bool, optional (Default: False)
        False → Index (time, seq) wird vor dem Schreiben zu Spalten zurückgesetzt.
        True  → MultiIndex bleibt als Index im CSV erhalten.
    fields : list[str] | None, optional
        Liste numerischer Felder, die exportiert werden sollen. None → alle numerischen Felder.

    Ausgabe
    -------
    <path>/InfluxDB_backup_<YYYY-MM-DD>/<measurement>.csv

    Hinweise
    --------
    - Spaltennamen:
        * Ein Feld → Spalten = IDs (z. B. T01, T02, …)
        * Mehrere Felder → Spalten = "<Feld>_<ID>" (z. B. T_T01, Q_T01, …)
    - Komplett leere Spalten werden vor dem Schreiben entfernt.
    """
    date = str(_pd.Timestamp.now().date())
    outdir = path.rstrip(_os.sep) + _os.sep + f'InfluxDB_backup_{date}' + _os.sep
    _os.makedirs(outdir, exist_ok=True)

    print(f'Backup (max {days} Tage) nach: {outdir}')
    measurements = list_measurements(days=days, display=False)

    stop = _pd.Timestamp.now()
    start = stop - _pd.Timedelta(days=days)

    for m in measurements:
        print(' -', m)
        try:
            df_raw = read_raw(m, start=start, stop=stop)
        except Exception as e:
            warnings.warn(f"[backup_cloud] read_raw() failed for '{m}': {e}")
            continue

        if df_raw is None or df_raw.empty:
            # Nichts im Zeitfenster → überspringen
            continue

        # Falls 'ID' fehlt, aber 'Station' vorhanden ist (z. B. MeteoSchweiz),
        # verwenden wir Station als ID, um dennoch pro "Sensor" Spalten zu erhalten.
        if 'ID' not in df_raw.columns and 'Station' in df_raw.columns:
            try:
                df_raw = df_raw.copy()
                df_raw['ID'] = df_raw['Station'].astype(str)
            except Exception:
                pass  # falls das nicht klappt, bleibt Fallback später aktiv

        # Numerische Felder bestimmen (Metadaten ausschliessen)
        meta_cols = {'ID', 'Position', 'Station', 'result', 'table', '_measurement'}
        num_fields_all = [
            c for c in df_raw.columns
            if c not in meta_cols and _pd.api.types.is_numeric_dtype(df_raw[c])
        ]

        # Optional auf gewünschte Felder einschränken
        if fields is not None:
            num_fields = [c for c in num_fields_all if c in set(fields)]
        else:
            num_fields = num_fields_all

        df_wide = None

        if 'ID' in df_raw.columns and len(num_fields) > 0:
            multi_field = len(num_fields) > 1
            for fld in num_fields:
                dfx = df_raw[['ID', fld]].dropna(subset=['ID'])
                # Ggf. komplett leere Spalte überspringen
                if dfx[fld].dropna().empty:
                    continue

                piv = _pivot_no_agg_by_id(dfx, fld)   # **keine Aggregation**

                if multi_field:
                    piv = piv.add_prefix(fld + '_')    # Feldpräfix bei mehreren Feldern

                if df_wide is None:
                    df_wide = piv
                else:
                    # Outer-Align über (time, seq) + Spalten
                    df_wide = df_wide.combine_first(piv)

        # Fallback: kein ID oder keine numerischen Felder → Rohdaten exportieren
        if df_wide is None or df_wide.empty:
            df_out = df_raw.copy()
            # Für lesbare CSV ggf. Zeitindex zurück in Spalte
            df_out = df_out.reset_index()
            fname = f"{_sanitize_filename(m)}.csv"
            df_out.to_csv(outdir + fname, index=False, encoding="utf-8")
            continue

        # Komplett leere Spalten entfernen
        df_wide = df_wide.dropna(axis=1, how='all')

        # CSV schreiben
        fname = f"{_sanitize_filename(m)}.csv"
        if write_multiindex:
            # MultiIndex (time, seq) bleibt erhalten
            df_wide.to_csv(outdir + fname, index=True, encoding="utf-8")
        else:
            # Index als Spalten 'time','seq' für CSV-Benutzerfreundlichkeit
            df_wide.reset_index().to_csv(outdir + fname, index=False, encoding="utf-8")

        

@_database
def write(df, measurement, tags=['ID'], **kwargs):
    """
    Write a pandas DataFrame to InfluxDB as a measurement.

    Parameters
    ----------
    df : pandas.DataFrame
        Time-indexed DataFrame to write. Index is localized to `tz` before writing.
    measurement : str
        Target measurement name in InfluxDB (e.g., "24-000").
    tags : list of str, optional
        Column names to be used as tag columns (default: ['ID']).
    **kwargs : dict
        Injected by @_database: client, bucket, org, tz.

    Returns
    -------
    None

    Notes
    -----
    - Uses the DataFrame write API with `data_frame_measurement_name`
      and `data_frame_tag_columns`.
    - Ensure all numeric fields are of numeric dtype; strings become tags or fields.

    Examples
    --------
    >>> df_out = df.copy()
    >>> df_out['ID'] = 'T01'
    >>> write(df_out, "24-000", tags=['ID'])
    """
    df = df.tz_localize(kwargs['tz'])
    options = _WriteOptions()
    with kwargs['client'].write_api(write_options=options) as write_client:
        write_client.write(kwargs['bucket'], kwargs['org'], record=df, 
                           data_frame_measurement_name=measurement,
                           data_frame_tag_columns=tags)                    

@_database
def list_measurements(days=365, display=True, **kwargs):
    """
    List available measurements (time series) from the configured InfluxDB bucket.

    Parameters
    ----------
    days : int, optional (default=365)
        Number of days to look back in time when querying available measurements.
    display : bool, optional (default=True)
        If True, the list of measurements will also be printed to stdout.
    **kwargs : dict
        Injected automatically by the @_database decorator. Contains:
        - client : InfluxDBClient
        - bucket : str
        - org    : str
        - tz     : timezone string

    Returns
    -------
    list of str
        A list of measurement names (e.g. ["24-000", "MeteoSchweiz", "VMmonitor"]).

    Examples
    --------
    >>> list_measurements()
    ['24-000', 'MeteoSchweiz', 'VMmonitor']

    >>> ms = list_measurements(days=30, display=False)
    >>> print(ms)
    ['24-000', '25-900']
    """
    query_api = kwargs['client'].query_api()
    tables = query_api.query(f'''
                             import "influxdata/influxdb/schema"
                             schema.measurements(
                             bucket: "{kwargs['bucket']}", 
                             start: -{days}d)
                             ''')
    measurements = [row.values["_value"] for table in tables for row in table]
    if display:
        print('measurements in', kwargs['bucket'], ':', measurements)
    return measurements


@_database
def list_signal_names(measurement, days=365, display=True, **kwargs):
    """
    List all signal names (field keys) for a given measurement.

    Parameters
    ----------
    measurement : str
        Name of the measurement (e.g. "24-000", "MeteoSchweiz").
    days : int, optional (default=365)
        Number of days to look back in time when querying available field keys.
    display : bool, optional (default=True)
        If True, the list of signal names will also be printed to stdout.
    **kwargs : dict
        Injected automatically by the @_database decorator. Contains:
        - client : InfluxDBClient
        - bucket : str
        - org    : str
        - tz     : timezone string

    Returns
    -------
    list of str
        A list of field names (signal keys) present in the measurement.

    Examples
    --------
    >>> list_signal_names("24-000")
    ['T', 'Q', 'V']

    >>> signals = list_signal_names("MeteoSchweiz", days=30, display=False)
    >>> print(signals)
    ['Temperature', 'Humidity', 'Wind']
    """
    query_api = kwargs['client'].query_api()
    tables = query_api.query(f'''
                             import "influxdata/influxdb/schema"
                             schema.measurementFieldKeys(
                             bucket: "{kwargs['bucket']}",
                             measurement: "{measurement}",
                             start:  -{days}d)
                             ''')
    signal_names = [row.values["_value"] for table in tables for row in table]
    if display:
        print('signal names in', measurement, ':', signal_names)
    return signal_names


@_database
def delete_measurement(measurement, start='2024-01-01 00:00:00',
                       stop='2030-12-31 23:59:00', **kwargs):
    """
    Delete all data of a measurement within a specified time range,
    with a dry-run count and interactive confirmation.

    This function first queries the number of points that would be deleted
    from the given measurement in the specified time range. It then requires
    explicit keyboard confirmation ("Yes") before executing the delete request
    via the InfluxDB Delete API.

    Parameters
    ----------
    measurement : str
        Name of the measurement to delete (e.g. "25-900").
        Must exactly match one of the measurements in the target bucket.
    start : str, optional
        Start timestamp (inclusive) for deletion.
        Format: "YYYY-MM-DD HH:MM:SS". Default "2024-01-01 00:00:00".
    stop : str, optional
        End timestamp (inclusive) for deletion.
        Format: "YYYY-MM-DD HH:MM:SS". Default "2030-12-31 23:59:00".
    **kwargs : dict
        Injected automatically by the @_database decorator. Contains:
        - client : InfluxDBClient
            Active client connection.
        - bucket : str
            Name of the target bucket (e.g. "records").
        - org : str
            Organization name used for queries/deletes.
        - tz : str
            Timezone string (e.g. "Europe/Zurich"). Local times are converted
            to UTC internally before querying/deleting.

    Returns
    -------
    None
        No return value. Prints status messages about number of points found,
        confirmation prompt, and success/abort outcome.

    Workflow
    --------
    1. Counts all data points in `measurement` between `start` and `stop`.
    2. Prints the total number of points to be deleted.
    3. Prompts the user:
       - Typing "Yes" (case-insensitive) executes the delete.
       - Any other input aborts the operation.
    4. If confirmed, issues a delete request to InfluxDB.

    Notes
    -----
    - This operation is **irreversible**. Deleted data cannot be recovered.
    - Use a narrow time window and dry-run count to confirm scope before deletion.
    - Recommended to run a `count()` query manually before large deletes.

    Examples
    --------
    >>> delete_measurement("25-900",
    ...                    start="2024-01-01 00:00:00",
    ...                    stop="2024-12-31 23:59:59")
    [delete_measurement] Measurement '25-900' between 2024-01-01 00:00:00 and 2024-12-31 23:59:59: 43800 points will be deleted.
    Type 'Yes' to confirm deletion, 'No' to abort: Yes
    [delete_measurement] Deleted 43800 points from '25-900'.
    """
    def utc_time(t):
        return (_pd.Timestamp(t).tz_localize(kwargs['tz'])
                                .tz_convert('UTC').tz_localize(None)
                                .isoformat() + 'Z')

    query_api = kwargs['client'].query_api()
    q = f"""
        from(bucket: "{kwargs['bucket']}")
          |> range(start: {utc_time(start)}, stop: {utc_time(stop)})
          |> filter(fn: (r) => r._measurement == "{measurement}")
          |> count()
    """
    tables = query_api.query(q, org=kwargs['org'])
    total = sum(row.values["_value"] for table in tables for row in table)

    print(f"[delete_measurement] Measurement '{measurement}' "
          f"between {start} and {stop}: \n \n{total} points will be deleted.\n")

    # Confirmation
    confirm = input("Type 'Yes' to confirm deletion, 'No' to abort: ").strip()
    if confirm.lower() != "yes":
        print("[delete_measurement] Aborted by user.")
        return

    delete_api = kwargs['client'].delete_api()
    delete_api.delete(utc_time(start), utc_time(stop),
                      f'_measurement="{measurement}"',
                      kwargs['bucket'], kwargs['org'])

    print(f"[delete_measurement] Deleted {total} points from '{measurement}'.")

