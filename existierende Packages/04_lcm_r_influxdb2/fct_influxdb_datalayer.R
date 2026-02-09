# ======================================================================
# fct_influxdb_datalayer.R
# Data layer functions that combine config files with InfluxDB v2
# Replaces Akenza-based data fetching
# ======================================================================

library(dplyr)
library(lubridate)

# ======================================================================
# Lookup table: InfluxDB field name -> default unit
# Non-MeteoSwiss fields only - MeteoSwiss units are loaded from API
# ======================================================================
FIELD_TO_UNIT <- list(
  # TEMPERATURE
  "temperature_degrC_abs" = "°C",

  # HUMIDITY
  "humidity_perc_abs" = "%",

  # CO2
  "co2_ppm_abs" = "ppm",

  # VOC
  "voc_index_abs" = "Index",

  # BRIGHTNESS
  "brightness_lux_abs" = "lux",

  # PRESSURE
  "pressure_hPa_abs" = "hPa",

  # BINARY / OPEN-CLOSE
  "open_state_abs" = "",

  # ENERGY
  "energy_kWh_abs" = "kWh",
  "energy_kWh_inc" = "kWh",
  "energy_Wh_abs" = "Wh",
  "energy_Wh_inc" = "Wh",

  # POWER
  "power_kW_abs" = "kW",
  "power_W_abs" = "W",

  # CURRENT
  "current_ampere_abs" = "A",

  # VOLUME
  "volume_m3_abs" = "m³",
  "volume_m3_inc" = "m³",
  "volume_l_abs" = "l",
  "volume_l_inc" = "l",

  # VOLUME FLOW
  "volumeFlow_m3perh_abs" = "m³/h",
  "volumeFlow_lperh_abs" = "l/h",

  # BATTERY
  "battery_perc_abs" = "%",
  "battery_volt_abs" = "V"
)

#' Get default unit from field name
#' First checks local FIELD_TO_UNIT, then falls back to MeteoSwiss API
#'
#' @param field_name The InfluxDB field name
#' @return The default unit string
get_default_unit <- function(field_name) {
  # First check local lookup
  unit <- FIELD_TO_UNIT[[field_name]]
  if (!is.null(unit)) {
    return(unit)
  }

  # Fall back to MeteoSwiss API (from fct_datapoint_types.R)
  meteo_info <- get_meteoswiss_parameter_info(field_name)
  if (!is.null(meteo_info$unit) && nchar(meteo_info$unit) > 0) {
    return(meteo_info$unit)
  }

  return("")
}

# ======================================================================
# Lookup table: InfluxDB field name -> sensor_type
# Non-MeteoSwiss fields only - MeteoSwiss sensor types are loaded from API
# ======================================================================
FIELD_TO_SENSOR_TYPE <- list(
  # TEMPERATURE
  "temperature_degrC_abs" = "temperature",

  # HUMIDITY
  "humidity_perc_abs" = "humidity",

  # CO2
  "co2_ppm_abs" = "co2",

  # VOC
  "voc_index_abs" = "voc",

  # BRIGHTNESS
  "brightness_lux_abs" = "illuminance",

  # PRESSURE
  "pressure_hPa_abs" = "pressure",

  # BINARY / OPEN-CLOSE
  "open_state_abs" = "openClose",

  # ENERGY
  "energy_kWh_abs" = "energy",
  "energy_kWh_inc" = "energyTotal",
  "energy_Wh_abs" = "energy",
  "energy_Wh_inc" = "energyTotal",

  # POWER
  "power_kW_abs" = "power",
  "power_W_abs" = "power",

  # CURRENT
  "current_ampere_abs" = "current",

  # VOLUME
  "volume_m3_abs" = "waterConsumption",
  "volume_m3_inc" = "waterConsumption",
  "volume_l_abs" = "waterConsumption",
  "volume_l_inc" = "waterConsumption",

  # VOLUME FLOW
  "volumeFlow_m3perh_abs" = "airFlow",
  "volumeFlow_lperh_abs" = "airFlow",

  # BATTERY
  "battery_perc_abs" = "batteryStatus",
  "battery_volt_abs" = "batteryVoltage",

  # COUNTER
  "counter_impulse_inc" = "counter",
  "counter_inc" = "counter",
  "count_abs" = "counter"
)

# Mapping from datastructure_type to simplified dpType for module filtering
DATASTRUCTURE_TO_DPTYPE <- list(
  "temperature_degrC_abs" = "temperature",
  "humidity_perc_abs" = "humidity",
  "co2_ppm_abs" = "co2",
  "voc_index_abs" = "voc",
  "brightness_lux_abs" = "illuminance",
  "pressure_hPa_abs" = "pressure",
  "precipitation_mm_abs" = "precipitation",
  "sunshine_min_abs" = "sunshine",
  "radiation_Wperm2_abs" = "radiation",
  "windDirection_degr_abs" = "windDirection",
  "windSpeed_kmperh_abs" = "windSpeed",
  "windGust_kmperh_abs" = "windGust"
)

#' Get sensor_type from field name
#' First checks local FIELD_TO_SENSOR_TYPE, then falls back to MeteoSwiss API
#' which returns datastructure_type (e.g., temperature_degrC_abs)
#' The datastructure_type is mapped to a simplified dpType for module filtering
#'
#' @param field_name The InfluxDB field name
#' @return The sensor_type string for module filtering
get_sensor_type <- function(field_name) {
  # First check local lookup
  sensor_type <- FIELD_TO_SENSOR_TYPE[[field_name]]
  if (!is.null(sensor_type)) {
    return(sensor_type)
  }

  # Fall back to MeteoSwiss API (from fct_datapoint_types.R)
  # MeteoSwiss parameters include datastructure_type in the API response
  meteo_info <- get_meteoswiss_parameter_info(field_name)
  if (!is.null(meteo_info$datastructure_type) && nchar(meteo_info$datastructure_type) > 0) {
    ds_type <- meteo_info$datastructure_type
    # Map datastructure_type to simplified dpType
    simplified <- DATASTRUCTURE_TO_DPTYPE[[ds_type]]
    if (!is.null(simplified)) {
      return(simplified)
    }
    return(ds_type)
  }

  # Return field name as-is if no mapping found
  return(field_name)
}

# ======================================================================
# Load and enrich datapoints from config files
# ======================================================================
#' Get Datapoints from Config Files
#'
#' Reads configDatapoints.csv and enriches it with hierarchy information
#' from configHierarchy.csv, replacing the Akenza-based getDataPoints()
#'
#' @param configDatapointsPath Path to configDatapoints.csv
#' @param configHierarchyPath Path to configHierarchy.csv
#' @param configDpTypesPath Path to configDpTypes.csv (optional, for unit/name defaults)
#' @param projectFilter Optional filter for project
#' @param entityFilter Optional filter for entity
#' @return Dataframe with all datapoints and hierarchy information
#'
getDataPointsFromConfig <- function(configDatapointsPath = NULL,
                                     configHierarchyPath = NULL,
                                     configDpTypesPath = NULL,
                                     projectFilter = NULL,
                                     entityFilter = NULL,
                                     includeLastValues = FALSE) {

  # Use default paths if not provided
  if (is.null(configDatapointsPath)) {
    configDatapointsPath <- here::here("config", "configDatapoints.csv")
  }
  if (is.null(configHierarchyPath)) {
    configHierarchyPath <- here::here("config", "configHierarchy.csv")
  }
  if (is.null(configDpTypesPath)) {
    configDpTypesPath <- here::here("utils", "configDpTypes.csv")
  }

  # Helper function to return empty dataframe with expected structure
  get_empty_datapoints_df <- function() {
    data.frame(
      uuid = character(),
      deviceId = character(),
      deviceIdProject = character(),
      field = character(),
      dpName = character(),
      dpType = character(),
      dpCategory = character(),
      hierarchyId = character(),
      unit = character(),
      enabled = logical(),
      measurement = character(),
      bucket = character(),
      sensor = character(),
      project = character(),
      building = character(),
      entity = character(),
      floor = character(),
      room = character(),
      dpUnit = character(),
      stringsAsFactors = FALSE
    )
  }

  # Check if config file exists
  if (!file.exists(configDatapointsPath)) {
    return(get_empty_datapoints_df())
  }

  # Read datapoints config
  df_datapoints <- read.csv2(configDatapointsPath, stringsAsFactors = FALSE)

  # Return empty dataframe if no datapoints
  if (nrow(df_datapoints) == 0) {
    return(get_empty_datapoints_df())
  }

  # Filter only enabled datapoints
  if ("enabled" %in% names(df_datapoints)) {
    df_datapoints <- df_datapoints %>% filter(enabled == TRUE | enabled == "TRUE")
  }

  # Read hierarchy config
  df_hierarchy <- read.csv2(configHierarchyPath, stringsAsFactors = FALSE)

  # Read dpTypes for defaults (optional)
  df_dpTypes <- NULL
  if (file.exists(configDpTypesPath)) {
    df_dpTypes <- read.csv2(configDpTypesPath, stringsAsFactors = FALSE)
  }

  # Build hierarchy lookup tables
  projects <- df_hierarchy %>% filter(type == "project") %>% select(id, name) %>% rename(project = name, projectId = id)
  buildings <- df_hierarchy %>% filter(type == "building") %>% select(id, parentId, name) %>% rename(building = name, buildingId = id)
  entities <- df_hierarchy %>% filter(type == "entity") %>% select(id, parentId, name) %>% rename(entity = name, entityId = id)
  floors <- df_hierarchy %>% filter(type == "floor") %>% select(id, parentId, name) %>% rename(floor = name, floorId = id)
  rooms <- df_hierarchy %>% filter(type == "room") %>% select(id, parentId, name) %>% rename(room = name, roomId = id)

  # Enrich datapoints with hierarchy information
  df_enriched <- df_datapoints %>%
    left_join(rooms, by = c("hierarchyId" = "roomId")) %>%
    rename(roomParentId = parentId) %>%
    left_join(floors, by = c("roomParentId" = "floorId")) %>%
    rename(floorParentId = parentId) %>%
    left_join(entities, by = c("floorParentId" = "entityId")) %>%
    rename(entityParentId = parentId) %>%
    left_join(buildings, by = c("entityParentId" = "buildingId")) %>%
    rename(buildingParentId = parentId) %>%
    left_join(projects, by = c("buildingParentId" = "projectId"))

  # Handle cases where hierarchyId points directly to entity (for electricityEntity etc.)
  df_direct_entity <- df_datapoints %>%
    filter(!(hierarchyId %in% rooms$roomId) & (hierarchyId %in% entities$entityId)) %>%
    left_join(entities, by = c("hierarchyId" = "entityId")) %>%
    rename(entityParentId = parentId) %>%
    left_join(buildings, by = c("entityParentId" = "buildingId")) %>%
    rename(buildingParentId = parentId) %>%
    left_join(projects, by = c("buildingParentId" = "projectId")) %>%
    mutate(
      room = NA_character_,
      floor = NA_character_
    ) %>%
    select(-entityParentId, -buildingParentId)

  # Handle cases where hierarchyId points directly to building (for outsideClimate etc.)
  df_direct_building <- df_datapoints %>%
    filter(!(hierarchyId %in% rooms$roomId) & !(hierarchyId %in% entities$entityId) & (hierarchyId %in% buildings$buildingId)) %>%
    left_join(buildings, by = c("hierarchyId" = "buildingId")) %>%
    left_join(projects, by = c("parentId" = "projectId")) %>%
    mutate(
      room = NA_character_,
      floor = NA_character_,
      entity = NA_character_
    ) %>%
    select(-parentId)

  # Handle cases where hierarchyId points directly to project (for outsideClimate at project level)
  # These datapoints apply to ALL buildings in the project
  df_direct_project <- df_datapoints %>%
    filter(!(hierarchyId %in% rooms$roomId) & !(hierarchyId %in% entities$entityId) & !(hierarchyId %in% buildings$buildingId) & (hierarchyId %in% projects$projectId)) %>%
    left_join(projects, by = c("hierarchyId" = "projectId"))

  # For project-level datapoints, create one entry per building in that project
  if (nrow(df_direct_project) > 0) {
    # Get all buildings with their projects
    buildings_with_projects <- buildings %>%
      left_join(projects, by = c("parentId" = "projectId"))

    # Expand project-level datapoints to all buildings in that project
    df_direct_project_expanded <- df_direct_project %>%
      left_join(
        buildings_with_projects %>% select(buildingId, building, project),
        by = "project",
        relationship = "many-to-many"
      ) %>%
      mutate(
        room = NA_character_,
        floor = NA_character_,
        entity = NA_character_
      )
  } else {
    df_direct_project_expanded <- data.frame()
  }

  # Combine and clean up
  df_enriched <- df_enriched %>%
    filter(hierarchyId %in% rooms$roomId) %>%
    select(-roomParentId, -floorParentId, -entityParentId, -buildingParentId)

  # Merge direct entity assignments
  if (nrow(df_direct_entity) > 0) {
    common_cols <- intersect(names(df_enriched), names(df_direct_entity))
    df_enriched <- bind_rows(
      df_enriched %>% select(all_of(common_cols)),
      df_direct_entity %>% select(all_of(common_cols))
    )
  }

  # Merge direct building assignments
  if (nrow(df_direct_building) > 0) {
    # Ensure same columns
    common_cols <- intersect(names(df_enriched), names(df_direct_building))
    df_enriched <- bind_rows(
      df_enriched %>% select(all_of(common_cols)),
      df_direct_building %>% select(all_of(common_cols))
    )
  }

  # Merge project-level assignments (expanded to buildings)
  if (nrow(df_direct_project_expanded) > 0) {
    common_cols <- intersect(names(df_enriched), names(df_direct_project_expanded))
    df_enriched <- bind_rows(
      df_enriched,
      df_direct_project_expanded %>% select(all_of(common_cols))
    )
  }

  # Add dpUnit - first from CSV, then from field name lookup (vectorized)
  if ("unit" %in% names(df_enriched)) {
    # Use vapply for vectorized lookup - much faster than rowwise()
    default_units <- vapply(df_enriched$field, get_default_unit, character(1))
    df_enriched$dpUnit <- ifelse(
      is.na(df_enriched$unit) | df_enriched$unit == "",
      default_units,
      df_enriched$unit
    )
  } else {
    df_enriched$dpUnit <- vapply(df_enriched$field, get_default_unit, character(1))
  }

  # Add source column for compatibility
  df_enriched$source <- "influxdb"

  # Derive dpType from field name using lookup table (vectorized)
  # This maps InfluxDB field names to module-compatible sensor types
  if ("dpType" %in% names(df_enriched)) {
    # Use vapply for vectorized lookup - much faster than rowwise()
    sensor_types <- vapply(df_enriched$field, get_sensor_type, character(1))
    needs_lookup <- is.na(df_enriched$dpType) | df_enriched$dpType == "" | df_enriched$dpType == df_enriched$field
    df_enriched$dpType <- ifelse(needs_lookup, sensor_types, df_enriched$dpType)
  } else {
    df_enriched$dpType <- vapply(df_enriched$field, get_sensor_type, character(1))
  }

  # Derive dpName from datastructure API if dpName equals field name (default)
  # This provides human-readable names like "Temperatur" instead of "temperature_degrC_abs"
  # Uses get_datapoint_type_info() which accesses pre-loaded lookup tables (vectorized)
  if ("dpName" %in% names(df_enriched) && "field" %in% names(df_enriched)) {
    # Vectorized lookup - get names for all fields at once
    lookup_names <- vapply(df_enriched$field, function(f) {
      info <- get_datapoint_type_info(f)
      if (!is.null(info$name)) info$name else f
    }, character(1))

    # Only update where dpName is empty or equals field name
    needs_update <- is.na(df_enriched$dpName) | df_enriched$dpName == "" | df_enriched$dpName == df_enriched$field
    # Only use lookup if it provides a different name than the field
    valid_lookup <- lookup_names != df_enriched$field

    df_enriched$dpName <- ifelse(
      needs_update & valid_lookup,
      lookup_names,
      df_enriched$dpName
    )
  }

  # Apply filters
  if (!is.null(projectFilter)) {
    df_enriched <- df_enriched %>% filter(project %in% projectFilter)
  }

  if (!is.null(entityFilter)) {
    df_enriched <- df_enriched %>% filter(entity %in% entityFilter | is.na(entity))
  }

  # Add placeholder columns for compatibility with existing modules
  if (!"deviceDescription" %in% names(df_enriched)) {
    # Use deviceIdProject if available, otherwise empty string
    if ("deviceIdProject" %in% names(df_enriched) && nrow(df_enriched) > 0) {
      df_enriched$deviceDescription <- df_enriched$deviceIdProject
    } else {
      df_enriched$deviceDescription <- character(nrow(df_enriched))
    }
  }
  if (!"devicePictureUrl" %in% names(df_enriched)) {
    df_enriched$devicePictureUrl <- character(nrow(df_enriched))
  }
  if (!"lastValue" %in% names(df_enriched)) {
    df_enriched$lastValue <- rep(NA_real_, nrow(df_enriched))
  }
  if (!"lastValueUpdateTimestamp" %in% names(df_enriched)) {
    df_enriched$lastValueUpdateTimestamp <- rep(NA_real_, nrow(df_enriched))
  }

  # Optionally fetch last values from InfluxDB (also updates devicePictureUrl and deviceDescription)
  # Default is FALSE for faster startup - call updateDatapointsLastValues() separately when needed
  if (includeLastValues) {
    df_enriched <- updateDatapointsLastValues(df_enriched)
  }

  return(df_enriched)
}

# ======================================================================
# Get timeseries from InfluxDB with module-compatible interface
# ======================================================================
#' Get Timeseries from InfluxDB
#'
#' Fetches timeseries data from InfluxDB v2, compatible with existing
#' getTimeseries() interface used by modules
#'
#' @param uuidStr UUID of the datapoint (from configDatapoints)
#' @param dpList Dataframe with datapoint definitions
#' @param datetimeStart Optional start timestamp
#' @param datetimeEnd Optional end timestamp
#' @param func Aggregation function: "raw", "mean", "min", "max", "median", "sum", "count", "diffMax"
#' @param agg Aggregation interval: "15m", "1h", "1d", "1W", "1M", "1Y"
#' @param cached Whether to use cache (for compatibility, not used with InfluxDB)
#' @return Dataframe with time and value columns
#'
getTimeseriesFromInflux <- function(uuidStr,
                                     dpList,
                                     datetimeStart = NULL,
                                     datetimeEnd = NULL,
                                     func = "mean",
                                     agg = "1d",
                                     cached = FALSE) {

  # Get datapoint info from dpList
  row <- dpList %>% filter(uuid == uuidStr)

  if (nrow(row) == 0) {
    warning(paste("Datapoint not found:", uuidStr))
    return(data.frame(time = as.POSIXct(character()), value = numeric()))
  }

  if (nrow(row) > 1) {
    warning("Multiple datapoints with same UUID, using first")
    row <- head(row, 1)
  }

  # Get connection settings from environment
  default_bucket <- Sys.getenv("INFLUXDB_BUCKET", "")
  default_measurement <- Sys.getenv("INFLUXDB_MEASUREMENT", "")

  # Use bucket from datapoint config if available, otherwise use default
  bucket <- default_bucket
  if ("bucket" %in% names(row) && !is.na(row$bucket) && nchar(row$bucket) > 0) {
    bucket <- row$bucket
  }

  # Use measurement from datapoint config if available, otherwise use default
  measurement <- default_measurement
  if ("measurement" %in% names(row) && !is.na(row$measurement) && nchar(row$measurement) > 0) {
    measurement <- row$measurement
  }

  # Convert datetime parameters to InfluxDB format
  if (is.null(datetimeStart)) {
    start <- "-365d"
  } else {
    start <- format(as.POSIXct(datetimeStart), "%Y-%m-%dT%H:%M:%SZ")
  }

  if (is.null(datetimeEnd)) {
    stop <- "now()"
  } else {
    stop <- format(as.POSIXct(datetimeEnd), "%Y-%m-%dT%H:%M:%SZ")
  }

  # Map aggregation interval
  agg_map <- list(
    "5m" = "5m",
    "10m" = "10m",
    "15m" = "15m",
    "1h" = "1h",
    "1d" = "1d",
    "1W" = "1w",
    "1M" = "1mo",
    "1Y" = "1y"
  )
  aggregate_window <- agg_map[[agg]]
  if (is.null(aggregate_window)) aggregate_window <- "1d"

  # Map aggregation function
  aggregation <- switch(func,
                         "raw" = NULL,
                         "mean" = "mean",
                         "min" = "min",
                         "max" = "max",
                         "median" = "median",
                         "sum" = "sum",
                         "count" = "count",
                         "diffMax" = "max",  # Handle diffMax specially after query
                         "mean"  # default
  )

  # Build and execute query
  tryCatch({
    # Get sensor value (may be NULL/NA for older datapoints)
    sensor_val <- if ("sensor" %in% names(row) && !is.na(row$sensor) && nchar(row$sensor) > 0) row$sensor else NULL

    # Try with device_id tag first
    df <- influxdb_get_timeseries_raw(
      device_id = row$deviceId,
      field = row$field,
      bucket = bucket,
      measurement = measurement,
      device_tag = "device_id",
      sensor = sensor_val,
      start = start,
      stop = stop,
      aggregation = aggregation,
      aggregate_window = aggregate_window
    )

    # If no data found, try with device_id_project tag
    if (nrow(df) == 0) {
      df <- influxdb_get_timeseries_raw(
        device_id = row$deviceId,
        field = row$field,
        bucket = bucket,
        measurement = measurement,
        device_tag = "device_id_project",
        sensor = sensor_val,
        start = start,
        stop = stop,
        aggregation = aggregation,
        aggregate_window = aggregate_window
      )
    }

    if (nrow(df) == 0) {
      return(data.frame(time = as.POSIXct(character()), value = numeric()))
    }

    # Handle diffMax (difference between consecutive max values)
    if (func == "diffMax") {
      df <- df %>%
        arrange(time) %>%
        mutate(
          value = value - lag(value),
          value = ifelse(is.na(value), 0, value)
        )
    }

    # Convert timezone
    df$time <- with_tz(df$time, tzone = "Europe/Zurich")

    return(df)

  }, error = function(e) {
    warning(paste("Error fetching timeseries:", e$message))
    return(data.frame(time = as.POSIXct(character()), value = numeric()))
  })
}

# ======================================================================
# Raw timeseries query for InfluxDB (internal)
# Supports both TTN (device_id tag + _field) and MeteoSwiss (sensor tag)
# ======================================================================
influxdb_get_timeseries_raw <- function(device_id,
                                         field,
                                         bucket = NULL,
                                         measurement = NULL,
                                         device_tag = "device_id",
                                         sensor = NULL,
                                         start = "-7d",
                                         stop = "now()",
                                         aggregation = NULL,
                                         aggregate_window = "1h") {

  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_BUCKET", "")

  if (nchar(bucket) == 0 || nchar(device_id) == 0 || nchar(field) == 0) {
    return(data.frame(time = as.POSIXct(character()), value = numeric()))
  }

  # Detect MeteoSwiss bucket - uses different data structure and token
  meteo_bucket <- Sys.getenv("INFLUXDB_METEO_BUCKET", "meteoSwiss")
  is_meteoswiss <- (bucket == meteo_bucket)

  # Use MeteoSwiss-specific token if querying MeteoSwiss bucket
  meteo_token <- if (is_meteoswiss) {
    token <- Sys.getenv("INFLUXDB_METEO_TOKEN", "")
    if (nchar(token) == 0) {
      stop("INFLUXDB_METEO_TOKEN not configured")
    }
    token
  } else {
    NULL
  }

  # Build Flux query
  flux_query <- paste0(
    'from(bucket: "', bucket, '")\n',
    '  |> range(start: ', start, ', stop: ', stop, ')\n'
  )

  if (is_meteoswiss) {
    # MeteoSwiss structure:
    # - _measurement = station code (e.g., "LUZ")
    # - sensor tag = sensor type (e.g., "tre200s0")
    # - _field = "value"
    # Here: device_id = station, field = sensor type
    flux_query <- paste0(
      flux_query,
      '  |> filter(fn: (r) => r._measurement == "', device_id, '")\n',
      '  |> filter(fn: (r) => r.sensor == "', field, '")\n',
      '  |> filter(fn: (r) => r._field == "value")\n'
    )
  } else {
    # TTN/Standard structure:
    # - _measurement = measurement name
    # - device_id tag = device identifier
    # - _field = sensor field name
    # - sensor tag = optional sensor identifier
    if (!is.null(measurement) && nchar(measurement) > 0) {
      flux_query <- paste0(
        flux_query,
        '  |> filter(fn: (r) => r._measurement == "', measurement, '")\n'
      )
    }

    flux_query <- paste0(
      flux_query,
      '  |> filter(fn: (r) => r.', device_tag, ' == "', device_id, '")\n'
    )

    # Add sensor filter if specified
    if (!is.null(sensor) && nchar(sensor) > 0) {
      flux_query <- paste0(
        flux_query,
        '  |> filter(fn: (r) => r.sensor == "', sensor, '")\n'
      )
    }

    flux_query <- paste0(
      flux_query,
      '  |> filter(fn: (r) => r._field == "', field, '")\n'
    )
  }

  # Add aggregation if specified
  if (!is.null(aggregation) && aggregation %in% c("mean", "sum", "min", "max", "count")) {
    flux_query <- paste0(
      flux_query,
      '  |> aggregateWindow(every: ', aggregate_window, ', fn: ', aggregation, ', createEmpty: false)\n'
    )
  }

  flux_query <- paste0(flux_query, '  |> yield(name: "result")')

  # Execute query (use MeteoSwiss token if applicable)
  tryCatch({
    csv_content <- influxdb_query(flux_query, token = meteo_token)
    df <- parse_influx_timeseries(csv_content)
    return(df)
  }, error = function(e) {
    warning(paste("Error querying InfluxDB:", e$message))
    return(data.frame(time = as.POSIXct(character()), value = numeric()))
  })
}

# ======================================================================
# Parse InfluxDB timeseries CSV response
# ======================================================================
parse_influx_timeseries <- function(csv_content) {
  lines <- strsplit(csv_content, "\r?\n")[[1]]
  data_lines <- lines[!startsWith(trimws(lines), "#") & nchar(trimws(lines)) > 0]

  if (length(data_lines) < 2) {
    return(data.frame(time = as.POSIXct(character()), value = numeric()))
  }

  # Parse header
  header <- strsplit(data_lines[1], ",")[[1]]
  time_col_idx <- which(header == "_time")
  value_col_idx <- which(header == "_value")

  if (length(time_col_idx) == 0 || length(value_col_idx) == 0) {
    return(data.frame(time = as.POSIXct(character()), value = numeric()))
  }

  # Parse data
  times <- c()
  values <- c()

  for (line in data_lines[-1]) {
    parts <- strsplit(line, ",")[[1]]
    if (length(parts) >= max(time_col_idx, value_col_idx)) {
      time_str <- trimws(parts[time_col_idx])
      value_str <- trimws(parts[value_col_idx])

      if (nchar(time_str) > 0 && nchar(value_str) > 0) {
        times <- c(times, time_str)
        values <- c(values, as.numeric(value_str))
      }
    }
  }

  if (length(times) == 0) {
    return(data.frame(time = as.POSIXct(character()), value = numeric()))
  }

  df <- data.frame(
    time = as.POSIXct(times, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    value = values,
    stringsAsFactors = FALSE
  )

  df <- df[!is.na(df$time), ]
  df <- df[order(df$time), ]

  return(df)
}

# ======================================================================
# Batch query: Get timeseries for multiple datapoints in ONE query
# ======================================================================
#' Get Timeseries for Multiple Datapoints
#'
#' Fetches timeseries data for multiple datapoints in a single InfluxDB query.
#' Much faster than calling getTimeseriesFromInflux for each datapoint separately.
#'
#' @param dpList Dataframe with datapoint definitions (must have deviceId, field, uuid columns)
#' @param datetimeStart Optional start timestamp
#' @param datetimeEnd Optional end timestamp
#' @param func Aggregation function: "raw", "mean", "min", "max", "median", "sum", "count", "diffMax"
#' @param agg Aggregation interval: "15m", "1h", "1d", "1W", "1M", "1Y"
#' @return Dataframe with time, value, and uuid columns
#'
getTimeseriesBatch <- function(dpList,
                                datetimeStart = NULL,
                                datetimeEnd = NULL,
                                func = "mean",
                                agg = "1h") {

  if (is.null(dpList) || nrow(dpList) == 0) {
    return(data.frame(time = as.POSIXct(character()), value = numeric(), uuid = character()))
  }

  default_bucket <- Sys.getenv("INFLUXDB_BUCKET", "")
  default_measurement <- Sys.getenv("INFLUXDB_MEASUREMENT", "")

  if (nchar(default_bucket) == 0) {
    return(data.frame(time = as.POSIXct(character()), value = numeric(), uuid = character()))
  }

  # Add measurement column if not present, using default
  if (!"measurement" %in% names(dpList)) {
    dpList$measurement <- default_measurement
  }
  # Fill NA measurements with default
  dpList$measurement[is.na(dpList$measurement) | dpList$measurement == ""] <- default_measurement

  # Add bucket column if not present, using default
  if (!"bucket" %in% names(dpList)) {
    dpList$bucket <- default_bucket
  }
  # Fill NA/empty buckets with default
  dpList$bucket[is.na(dpList$bucket) | dpList$bucket == ""] <- default_bucket

  # Group by bucket AND measurement and run separate queries
  dpList$query_group <- paste0(dpList$bucket, "|", dpList$measurement)
  query_groups <- unique(dpList$query_group)

  # Detect MeteoSwiss bucket
  meteo_bucket <- Sys.getenv("INFLUXDB_METEO_BUCKET", "meteoSwiss")

  # Helper function for single bucket/measurement batch query
  runBatchQuery <- function(dpSubset, query_bucket, meas) {
    # Convert datetime parameters to InfluxDB format
    if (is.null(datetimeStart)) {
      start <- "-365d"
    } else {
      start <- format(as.POSIXct(datetimeStart), "%Y-%m-%dT%H:%M:%SZ")
    }

    if (is.null(datetimeEnd)) {
      stop <- "now()"
    } else {
      stop <- format(as.POSIXct(datetimeEnd), "%Y-%m-%dT%H:%M:%SZ")
    }

    # Map aggregation interval
    agg_map <- list(
      "5m" = "5m",
      "10m" = "10m",
      "15m" = "15m",
      "1h" = "1h",
      "1d" = "1d",
      "1W" = "1w",
      "1M" = "1mo",
      "1Y" = "1y"
    )
    aggregate_window <- agg_map[[agg]]
    if (is.null(aggregate_window) && agg != "raw") aggregate_window <- "1h"

    # Map aggregation function
    # If agg = "raw", force no aggregation regardless of func
    aggregation <- if (agg == "raw") {
      NULL
    } else {
      switch(func,
             "raw" = NULL,
             "mean" = "mean",
             "min" = "min",
             "max" = "max",
             "median" = "median",
             "sum" = "sum",
             "count" = "count",
             "diffMax" = "max",  # Handle diffMax specially after query
             "mean"  # default
      )
    }

    # Check if this is MeteoSwiss data
    is_meteoswiss <- (query_bucket == meteo_bucket)

    # Use MeteoSwiss-specific token if querying MeteoSwiss bucket
    query_token <- if (is_meteoswiss) {
      token <- Sys.getenv("INFLUXDB_METEO_TOKEN", "")
      if (nchar(token) == 0) {
        stop("INFLUXDB_METEO_TOKEN not configured")
      }
      token
    } else {
      NULL
    }

    # Build Flux query with specified bucket
    flux_query <- paste0(
      'from(bucket: "', query_bucket, '")\n',
      '  |> range(start: ', start, ', stop: ', stop, ')\n'
    )

    if (is_meteoswiss) {
      # MeteoSwiss structure:
      # - _measurement = station code (stored in deviceId/meas)
      # - sensor tag = sensor type (stored in field)
      # - _field = "value"

      # Build measurement filter for all stations
      stations <- unique(dpSubset$deviceId)
      if (length(stations) == 1) {
        flux_query <- paste0(
          flux_query,
          '  |> filter(fn: (r) => r._measurement == "', stations[1], '")\n'
        )
      } else {
        station_conditions <- sapply(stations, function(s) paste0('r._measurement == "', s, '"'))
        flux_query <- paste0(
          flux_query,
          '  |> filter(fn: (r) => ', paste(station_conditions, collapse = " or "), ')\n'
        )
      }

      # Build sensor filter conditions
      filter_conditions <- sapply(1:nrow(dpSubset), function(i) {
        paste0('(r._measurement == "', dpSubset$deviceId[i], '" and r.sensor == "', dpSubset$field[i], '")')
      })
      filter_expr <- paste(filter_conditions, collapse = " or ")

      flux_query <- paste0(
        flux_query,
        '  |> filter(fn: (r) => r._field == "value")\n',
        '  |> filter(fn: (r) => ', filter_expr, ')\n'
      )
    } else {
      # TTN/Standard structure:
      # - _measurement = measurement name
      # - device_id tag = device identifier
      # - _field = sensor field name
      # - sensor tag = channel identifier (optional, e.g. "channel1", "channel2")

      # Check if sensor column exists and has non-empty values
      has_sensor <- "sensor" %in% names(dpSubset)

      # Build device/field filter conditions (with optional sensor tag)
      filter_conditions <- sapply(1:nrow(dpSubset), function(i) {
        base_filter <- paste0('(r.device_id == "', dpSubset$deviceId[i], '" and r._field == "', dpSubset$field[i], '"')
        # Add sensor filter if sensor column exists and has a non-empty value
        if (has_sensor && !is.na(dpSubset$sensor[i]) && nchar(trimws(dpSubset$sensor[i])) > 0) {
          paste0(base_filter, ' and r.sensor == "', dpSubset$sensor[i], '")')
        } else {
          paste0(base_filter, ')')
        }
      })
      filter_expr <- paste(filter_conditions, collapse = " or ")

      if (!is.null(meas) && nchar(meas) > 0) {
        flux_query <- paste0(
          flux_query,
          '  |> filter(fn: (r) => r._measurement == "', meas, '")\n'
        )
      }

      flux_query <- paste0(
        flux_query,
        '  |> filter(fn: (r) => ', filter_expr, ')\n'
      )
    }

    # Add aggregation if specified
    if (!is.null(aggregation)) {
      flux_query <- paste0(
        flux_query,
        '  |> aggregateWindow(every: ', aggregate_window, ', fn: ', aggregation, ', createEmpty: false)\n'
      )
    }

    flux_query <- paste0(flux_query, '  |> yield(name: "result")')

    # Execute query and parse results (use MeteoSwiss token if applicable)
    tryCatch({
      csv_content <- influxdb_query(flux_query, token = query_token)
      df <- parse_influx_batch_response(csv_content, dpSubset)

      if (nrow(df) > 0) {
        df$time <- with_tz(df$time, tzone = "Europe/Zurich")
      }

      return(df)
    }, error = function(e) {
      warning(paste("Error in batch query:", e$message))
      return(data.frame(time = as.POSIXct(character()), value = numeric(), uuid = character()))
    })
  }

  # Run queries for each bucket/measurement combination and combine results
  all_results <- lapply(query_groups, function(qg) {
    dp_subset <- dpList[dpList$query_group == qg, ]
    query_bucket <- dp_subset$bucket[1]
    query_measurement <- dp_subset$measurement[1]
    runBatchQuery(dp_subset, query_bucket, query_measurement)
  })

  # Combine results
  result <- do.call(rbind, all_results)

  # Handle diffMax (difference between consecutive max values) for each uuid
  if (func == "diffMax" && nrow(result) > 0) {
    result <- result %>%
      dplyr::group_by(uuid) %>%
      dplyr::arrange(time) %>%
      dplyr::mutate(
        value = value - dplyr::lag(value),
        value = ifelse(is.na(value), 0, value)
      ) %>%
      dplyr::ungroup()
  }

  return(result)
}

# ======================================================================
# Batch query: Get LAST values for multiple datapoints in ONE query
# ======================================================================
#' Get Last Values for Multiple Datapoints
#'
#' Fetches the last value for multiple datapoints in a single InfluxDB query.
#' Much faster than calling getTimeseries with func="last" for each datapoint separately.
#'
#' @param dpList Dataframe with datapoint definitions (must have deviceId, field, uuid columns)
#' @return Dataframe with uuid, value, and time columns
#'
getLastValuesBatch <- function(dpList) {

  if (is.null(dpList) || nrow(dpList) == 0) {
    return(data.frame(uuid = character(), value = numeric(), time = as.POSIXct(character())))
  }

  default_bucket <- Sys.getenv("INFLUXDB_BUCKET", "")
  default_measurement <- Sys.getenv("INFLUXDB_MEASUREMENT", "")

  if (nchar(default_bucket) == 0) {
    return(data.frame(uuid = character(), value = numeric(), time = as.POSIXct(character())))
  }

  # Add measurement column if not present, using default
  if (!"measurement" %in% names(dpList)) {
    dpList$measurement <- default_measurement
  }
  # Fill NA measurements with default
  dpList$measurement[is.na(dpList$measurement) | dpList$measurement == ""] <- default_measurement

  # Add bucket column if not present, using default
  if (!"bucket" %in% names(dpList)) {
    dpList$bucket <- default_bucket
  }
  # Fill NA/empty buckets with default
  dpList$bucket[is.na(dpList$bucket) | dpList$bucket == ""] <- default_bucket

  # Group by bucket AND measurement and run separate queries
  dpList$query_group <- paste0(dpList$bucket, "|", dpList$measurement)
  query_groups <- unique(dpList$query_group)

  # Detect MeteoSwiss bucket
  meteo_bucket <- Sys.getenv("INFLUXDB_METEO_BUCKET", "meteoSwiss")

  # Helper function for single bucket/measurement batch query
  runBatchQuery <- function(dpSubset, query_bucket, meas) {
    # Check if this is MeteoSwiss data
    is_meteoswiss <- (query_bucket == meteo_bucket)

    # Use MeteoSwiss-specific token if querying MeteoSwiss bucket
    query_token <- if (is_meteoswiss) {
      token <- Sys.getenv("INFLUXDB_METEO_TOKEN", "")
      if (nchar(token) == 0) {
        stop("INFLUXDB_METEO_TOKEN not configured")
      }
      token
    } else {
      NULL
    }

    # Build Flux query with specified bucket
    flux_query <- paste0(
      'from(bucket: "', query_bucket, '")\n',
      '  |> range(start: -30d)\n'
    )

    if (is_meteoswiss) {
      # MeteoSwiss structure:
      # - _measurement = station code (stored in deviceId/meas)
      # - sensor tag = sensor type (stored in field)
      # - _field = "value"

      # Build measurement filter for all stations
      stations <- unique(dpSubset$deviceId)
      if (length(stations) == 1) {
        flux_query <- paste0(
          flux_query,
          '  |> filter(fn: (r) => r._measurement == "', stations[1], '")\n'
        )
      } else {
        station_conditions <- sapply(stations, function(s) paste0('r._measurement == "', s, '"'))
        flux_query <- paste0(
          flux_query,
          '  |> filter(fn: (r) => ', paste(station_conditions, collapse = " or "), ')\n'
        )
      }

      # Build sensor filter conditions
      filter_conditions <- sapply(1:nrow(dpSubset), function(i) {
        paste0('(r._measurement == "', dpSubset$deviceId[i], '" and r.sensor == "', dpSubset$field[i], '")')
      })
      filter_expr <- paste(filter_conditions, collapse = " or ")

      flux_query <- paste0(
        flux_query,
        '  |> filter(fn: (r) => r._field == "value")\n',
        '  |> filter(fn: (r) => ', filter_expr, ')\n',
        '  |> group(columns: ["_measurement", "sensor"])\n',
        '  |> last()\n',
        '  |> yield(name: "result")'
      )
    } else {
      # TTN/Standard structure:
      # - _measurement = measurement name
      # - device_id tag = device identifier
      # - _field = sensor field name
      # - sensor tag = channel identifier (optional, for multi-channel devices)

      # Check if sensor column exists and has non-empty values
      has_sensor <- "sensor" %in% names(dpSubset) && any(!is.na(dpSubset$sensor) & dpSubset$sensor != "")

      # Build device/field/sensor filter conditions
      filter_conditions <- sapply(1:nrow(dpSubset), function(i) {
        sensor_val <- if (has_sensor && !is.na(dpSubset$sensor[i]) && dpSubset$sensor[i] != "") {
          dpSubset$sensor[i]
        } else {
          NULL
        }

        if (!is.null(sensor_val)) {
          paste0('(r.device_id == "', dpSubset$deviceId[i], '" and r._field == "', dpSubset$field[i], '" and r.sensor == "', sensor_val, '")')
        } else {
          paste0('(r.device_id == "', dpSubset$deviceId[i], '" and r._field == "', dpSubset$field[i], '")')
        }
      })
      filter_expr <- paste(filter_conditions, collapse = " or ")

      if (!is.null(meas) && nchar(meas) > 0) {
        flux_query <- paste0(
          flux_query,
          '  |> filter(fn: (r) => r._measurement == "', meas, '")\n'
        )
      }

      # Group by device_id, _field, and sensor (if present)
      group_cols <- if (has_sensor) '["device_id", "_field", "sensor"]' else '["device_id", "_field"]'

      flux_query <- paste0(
        flux_query,
        '  |> filter(fn: (r) => ', filter_expr, ')\n',
        '  |> group(columns: ', group_cols, ')\n',
        '  |> last()\n',
        '  |> group()\n',  # Ungroup to bring back all columns including sensor
        '  |> yield(name: "result")'
      )
    }

    # Execute query and parse results (use MeteoSwiss token if applicable)
    tryCatch({
      csv_content <- influxdb_query(flux_query, token = query_token)
      df <- parse_influx_batch_response(csv_content, dpSubset)

      if (nrow(df) > 0) {
        df$time <- with_tz(df$time, tzone = "Europe/Zurich")
      }

      return(df)
    }, error = function(e) {
      warning(paste("getLastValuesBatch error:", e$message))
      return(data.frame(uuid = character(), value = numeric(), time = as.POSIXct(character())))
    })
  }

  # Run queries for each bucket/measurement combination and combine results
  all_results <- lapply(query_groups, function(qg) {
    dp_subset <- dpList[dpList$query_group == qg, ]
    query_bucket <- dp_subset$bucket[1]
    query_measurement <- dp_subset$measurement[1]
    runBatchQuery(dp_subset, query_bucket, query_measurement)
  })

  # Combine results
  result <- do.call(rbind, all_results)
  return(result)
}

# ======================================================================
# Parse batch query response with device_id and field mapping to uuid
# OPTIMIZED: Uses vectorized operations instead of row-by-row parsing
# Supports both TTN (device_id + _field) and MeteoSwiss (_measurement + sensor)
# ======================================================================
parse_influx_batch_response <- function(csv_content, dpList) {
  # Quick check for empty content
  if (is.null(csv_content) || nchar(csv_content) < 10) {
    return(data.frame(time = as.POSIXct(character()), value = numeric(), uuid = character()))
  }

  tryCatch({
    # Filter out comment lines and empty lines, then parse as CSV
    lines <- strsplit(csv_content, "\r?\n")[[1]]
    data_lines <- lines[!startsWith(trimws(lines), "#") & nchar(trimws(lines)) > 0]

    if (length(data_lines) < 2) {
      return(data.frame(time = as.POSIXct(character()), value = numeric(), uuid = character()))
    }

    # Parse CSV without modifying lines - read.csv handles it properly
    # Then remove the empty first column that InfluxDB adds
    csv_text <- paste(data_lines, collapse = "\n")

    # Read without assuming row names - use header=FALSE to avoid row.names issues
    df_raw <- read.csv(text = csv_text, stringsAsFactors = FALSE, check.names = FALSE, header = FALSE)

    # First row is header
    col_names <- as.character(df_raw[1, ])
    df <- df_raw[-1, , drop = FALSE]
    names(df) <- col_names

    # Helper to check if column name is empty/NA
    is_empty_col_name <- function(name) {
      is.na(name) || name == "" || name == "NA" || trimws(name) == ""
    }

    # Remove the empty first column (InfluxDB annotation column)
    if (ncol(df) > 0 && is_empty_col_name(names(df)[1])) {
      df <- df[, -1, drop = FALSE]
    }

    # Also remove any trailing empty columns
    while (ncol(df) > 0 && is_empty_col_name(names(df)[ncol(df)])) {
      df <- df[, -ncol(df), drop = FALSE]
    }

    # Reset row names
    rownames(df) <- NULL

    # Check required columns exist
    if (!all(c("_time", "_value") %in% names(df))) {
      return(data.frame(time = as.POSIXct(character()), value = numeric(), uuid = character()))
    }

    # Create lookup from device_id + field + sensor -> uuid (vectorized)
    # Check if dpList has sensor column with non-empty values
    dpList_has_sensor <- "sensor" %in% names(dpList) && any(!is.na(dpList$sensor) & dpList$sensor != "")

    if (dpList_has_sensor) {
      # Include sensor in lookup key (use empty string if NA)
      dpList$lookup_key <- paste0(dpList$deviceId, "|", dpList$field, "|",
                                   ifelse(is.na(dpList$sensor) | dpList$sensor == "", "", dpList$sensor))
    } else {
      dpList$lookup_key <- paste0(dpList$deviceId, "|", dpList$field)
    }
    uuid_lookup <- setNames(dpList$uuid, dpList$lookup_key)

    # Determine data structure: TTN has device_id column, MeteoSwiss has sensor column as tag
    if ("device_id" %in% names(df)) {
      # TTN/Standard structure: use device_id + _field + sensor (if present)
      df_has_sensor <- "sensor" %in% names(df) && any(!is.na(df$sensor) & df$sensor != "")

      if (df_has_sensor && dpList_has_sensor) {
        # Include sensor in lookup key
        df$lookup_key <- paste0(df$device_id, "|", df$`_field`, "|",
                                 ifelse(is.na(df$sensor) | df$sensor == "", "", df$sensor))
      } else {
        df$lookup_key <- paste0(df$device_id, "|", df$`_field`)
      }
    } else if ("sensor" %in% names(df) && "_measurement" %in% names(df)) {
      # MeteoSwiss structure: use _measurement (station) + sensor
      df$lookup_key <- paste0(df$`_measurement`, "|", df$sensor)
    } else {
      # Fallback: try to match using _measurement + _field
      df$lookup_key <- paste0(df$`_measurement`, "|", df$`_field`)
    }

    # Vectorized: lookup uuid for all rows
    df$uuid <- uuid_lookup[df$lookup_key]

    # Filter out rows without matching uuid
    df <- df[!is.na(df$uuid), ]

    if (nrow(df) == 0) {
      return(data.frame(time = as.POSIXct(character()), value = numeric(), uuid = character()))
    }

    # Vectorized: clean time strings (remove nanoseconds)
    time_clean <- sub("\\.\\d+Z$", "Z", df$`_time`)

    # Build result dataframe
    result <- data.frame(
      time = as.POSIXct(time_clean, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
      value = as.numeric(df$`_value`),
      uuid = df$uuid,
      stringsAsFactors = FALSE
    )

    # Remove NA times
    result <- result[!is.na(result$time), ]

    return(result)

  }, error = function(e) {
    warning(paste("Error parsing batch response:", e$message))
    return(data.frame(time = as.POSIXct(character()), value = numeric(), uuid = character()))
  })
}

# ======================================================================
# Fetch last values for all datapoints from InfluxDB
# OPTIMIZED: Uses getLastValuesBatch() for single batch query instead of N queries
# ======================================================================
#' Update Datapoints with Last Values
#'
#' Fetches the latest value for each datapoint from InfluxDB
#' Uses batch query for performance (single query instead of N queries)
#'
#' @param df_datapoints Dataframe with datapoint definitions
#' @return Dataframe with lastValue and lastValueUpdateTimestamp columns updated
#'
updateDatapointsLastValues <- function(df_datapoints) {

  if (is.null(df_datapoints) || nrow(df_datapoints) == 0) {
    return(df_datapoints)
  }

  default_bucket <- Sys.getenv("INFLUXDB_BUCKET", "")
  default_measurement <- Sys.getenv("INFLUXDB_MEASUREMENT", "")
  meteo_bucket <- Sys.getenv("INFLUXDB_METEO_BUCKET", "meteoSwiss")

  # Initialize columns if they don't exist
  if (!"lastValue" %in% names(df_datapoints)) {
    df_datapoints$lastValue <- NA_real_
  }
  if (!"lastValueUpdateTimestamp" %in% names(df_datapoints)) {
    df_datapoints$lastValueUpdateTimestamp <- NA_real_
  }
  if (!"applicationId" %in% names(df_datapoints)) {
    df_datapoints$applicationId <- NA_character_
  }

  # =====================================================================
  # BATCH QUERY: Get all last values in ONE query instead of N queries
  # =====================================================================
  tryCatch({
    batch_results <- getLastValuesBatch(df_datapoints)

    if (!is.null(batch_results) && nrow(batch_results) > 0) {
      # Create lookup from uuid -> (value, time)
      for (i in seq_len(nrow(batch_results))) {
        uuid <- batch_results$uuid[i]
        matching_rows <- which(df_datapoints$uuid == uuid)

        if (length(matching_rows) > 0) {
          df_datapoints$lastValue[matching_rows] <- as.numeric(batch_results$value[i])

          if (!is.na(batch_results$time[i])) {
            df_datapoints$lastValueUpdateTimestamp[matching_rows] <- as.numeric(batch_results$time[i])
          }
        }
      }
    }
  }, error = function(e) {
    warning(paste("Batch query for last values failed:", e$message))
  })

  # =====================================================================
  # Get application_id for devices (batch per unique device)
  # =====================================================================
  # Determine effective bucket for each row
  if (!"bucket" %in% names(df_datapoints)) {
    df_datapoints$bucket <- NA_character_
  }

  df_datapoints <- df_datapoints %>%
    mutate(
      effective_bucket = ifelse(
        !is.na(bucket) & nchar(as.character(bucket)) > 0,
        as.character(bucket),
        default_bucket
      )
    )

  # Get unique devices (excluding MeteoSwiss)
  unique_devices <- df_datapoints %>%
    filter(effective_bucket != meteo_bucket) %>%
    select(deviceId, effective_bucket, measurement) %>%
    distinct()

  # Cache for application_id
  device_app_cache <- list()

  # Query application_id for each unique device (much fewer queries than N datapoints)
  for (i in seq_len(nrow(unique_devices))) {
    dev_id <- unique_devices$deviceId[i]
    dev_bucket <- unique_devices$effective_bucket[i]
    dev_measurement <- if (!is.na(unique_devices$measurement[i])) unique_devices$measurement[i] else default_measurement

    tryCatch({
      app_id <- influxdb_get_device_application_id(
        device_id = dev_id,
        bucket = dev_bucket,
        measurement = dev_measurement
      )

      if (!is.na(app_id) && nchar(app_id) > 0) {
        device_app_cache[[dev_id]] <- app_id
      }
    }, error = function(e) {
      # Silently skip
    })
  }

  # Apply cached application_ids to datapoints
  for (dev_id in names(device_app_cache)) {
    df_datapoints$applicationId[df_datapoints$deviceId == dev_id] <- device_app_cache[[dev_id]]
  }

  # =====================================================================
  # Update devicePictureUrl and deviceDescription
  # =====================================================================
  unique_devices_all <- unique(df_datapoints$deviceId)

  for (dev_id in unique_devices_all) {
    dev_idx <- which(df_datapoints$deviceId == dev_id)
    if (length(dev_idx) == 0) next

    first_idx <- dev_idx[1]
    eff_bucket <- df_datapoints$effective_bucket[first_idx]
    app_id <- df_datapoints$applicationId[first_idx]

    # Get device picture URL
    pic_url <- get_device_image_url(app_id, eff_bucket)

    # Get device description
    if (!is.na(eff_bucket) && eff_bucket == meteo_bucket) {
      # For MeteoSwiss, use dpName (which contains the sensor description)
      dev_desc <- df_datapoints$dpName[first_idx]
    } else {
      # For TTN devices, get name from API
      app_name <- get_device_name_by_app(app_id)
      if (!is.null(app_name) && !is.na(app_name) && nchar(app_name) > 0 && app_name != app_id) {
        dev_desc <- app_name
      } else {
        dev_desc <- df_datapoints$deviceDescription[first_idx]
      }
    }

    # Update all rows for this device
    df_datapoints$devicePictureUrl[dev_idx] <- pic_url
    df_datapoints$deviceDescription[dev_idx] <- dev_desc
  }

  df_datapoints <- df_datapoints %>% select(-effective_bucket)

  return(df_datapoints)
}
