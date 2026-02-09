# ======================================================================
# fct_influxdbv2_client.R
# InfluxDB v2 client functions for querying devices and fields
# ======================================================================

# ======================================================================
# Helper: Execute Flux query and return parsed result
# ======================================================================
influxdb_query <- function(flux_query, url = NULL, org = NULL, token = NULL) {
  # Use environment variables if not provided
  if (is.null(url)) url <- Sys.getenv("INFLUXDB_URL", "")
  if (is.null(org)) org <- Sys.getenv("INFLUXDB_ORG", "")
  if (is.null(token)) token <- Sys.getenv("INFLUXDB_TOKEN", "")

  # Check each setting individually for specific error messages
  if (nchar(url) == 0) {
    stop("INFLUXDB_URL not configured")
  }
  if (nchar(org) == 0) {
    stop("INFLUXDB_ORG not configured")
  }
  if (nchar(token) == 0) {
    stop("INFLUXDB_TOKEN not configured")
  }

  query_url <- paste0(url, "/api/v2/query?org=", utils::URLencode(org))

  response <- httr::POST(
    query_url,
    httr::add_headers(
      Authorization = paste("Token", token),
      `Content-Type` = "application/vnd.flux",
      Accept = "application/csv"
    ),
    body = flux_query,
    encode = "raw",
    httr::timeout(60)
  )

  if (httr::status_code(response) != 200) {
    stop(paste("InfluxDB query failed with status:", httr::status_code(response)))
  }

  content <- httr::content(response, "text", encoding = "UTF-8")
  return(content)
}

# ======================================================================
# Parse InfluxDB CSV response into data frame
# ======================================================================
parse_influx_csv <- function(csv_content, extra_columns = NULL) {
  # Split by newlines (handle both \r\n and \n)
  lines <- strsplit(csv_content, "\r?\n")[[1]]

  # Find data lines (skip comments and empty lines)
  data_lines <- lines[!startsWith(trimws(lines), "#") & nchar(trimws(lines)) > 0]

  if (length(data_lines) < 2) {
    return(data.frame())
  }

  # Parse header (first line) - InfluxDB CSV starts with empty column
  header <- strsplit(data_lines[1], ",")[[1]]

  # Find column indices
  value_col_idx <- which(header == "_value")
  time_col_idx <- which(header == "_time")

  # Find extra column indices (e.g., "application_id")
  extra_col_indices <- list()
  if (!is.null(extra_columns)) {
    for (col in extra_columns) {
      idx <- which(header == col)
      if (length(idx) > 0) {
        extra_col_indices[[col]] <- idx
      }
    }
  }

  # Parse data rows
  values <- c()
  times <- c()
  extra_values <- list()
  for (col in names(extra_col_indices)) {
    extra_values[[col]] <- c()
  }

  for (line in data_lines[-1]) {
    parts <- strsplit(line, ",")[[1]]

    # Extract _value
    if (length(value_col_idx) > 0 && length(parts) >= value_col_idx) {
      val <- trimws(parts[value_col_idx])
      if (nchar(val) > 0) {
        values <- c(values, val)

        # Extract _time if available
        if (length(time_col_idx) > 0 && length(parts) >= time_col_idx) {
          times <- c(times, trimws(parts[time_col_idx]))
        } else {
          times <- c(times, NA)
        }

        # Extract extra columns
        for (col in names(extra_col_indices)) {
          idx <- extra_col_indices[[col]]
          if (length(parts) >= idx) {
            extra_values[[col]] <- c(extra_values[[col]], trimws(parts[idx]))
          } else {
            extra_values[[col]] <- c(extra_values[[col]], NA)
          }
        }
      }
    } else if (length(parts) >= 4) {
      # Fallback: _value is usually the last column
      val <- trimws(parts[length(parts)])
      if (nchar(val) > 0 && val != "_value") {
        values <- c(values, val)
        times <- c(times, NA)
        for (col in names(extra_col_indices)) {
          extra_values[[col]] <- c(extra_values[[col]], NA)
        }
      }
    }
  }

  if (length(values) == 0) {
    return(data.frame())
  }

  # Return data frame with _value and _time columns
  df <- data.frame(`_value` = values, stringsAsFactors = FALSE, check.names = FALSE)
  if (length(times) > 0 && !all(is.na(times))) {
    df$`_time` <- times
  }

  # Add extra columns
  for (col in names(extra_values)) {
    if (length(extra_values[[col]]) > 0) {
      df[[col]] <- extra_values[[col]]
    }
  }

  return(df)
}

# ======================================================================
# Get unique device IDs from InfluxDB
# ======================================================================
influxdb_get_devices <- function(bucket = NULL, measurement = NULL,
                                  device_tag = "device_id",
                                  url = NULL, org = NULL, token = NULL) {
  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_BUCKET", "")

  if (nchar(bucket) == 0) {
    stop("Bucket not specified")
  }

  # Build Flux query to get unique device IDs
  flux_query <- paste0(
    'import "influxdata/influxdb/schema"\n',
    'schema.tagValues(\n',
    '  bucket: "', bucket, '",\n',
    '  tag: "', device_tag, '"\n'
  )

  # Add measurement filter if specified
  if (!is.null(measurement) && nchar(measurement) > 0) {
    flux_query <- paste0(
      flux_query,
      ',\n  predicate: (r) => r._measurement == "', measurement, '"\n'
    )
  }

  flux_query <- paste0(flux_query, ')')

  tryCatch({
    csv_content <- influxdb_query(flux_query, url, org, token)
    df <- parse_influx_csv(csv_content)

    if (nrow(df) > 0 && "_value" %in% names(df)) {
      devices <- unique(df$`_value`)
      return(devices)
    }

    return(character(0))
  }, error = function(e) {
    print(paste("Error getting devices:", e$message))
    return(character(0))
  })
}

# ======================================================================
# Get devices with both device_id and device_id_project for display
# ======================================================================
influxdb_get_devices_with_names <- function(bucket = NULL, measurement = NULL,
                                             start = "-365d",
                                             url = NULL, org = NULL, token = NULL) {
  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_BUCKET", "")

  if (nchar(bucket) == 0) {
    stop("Bucket not specified")
  }

  # Query to get unique combinations of device_id and device_id_project
  # Default: look back 1 year to find all devices
  flux_query <- paste0(
    'from(bucket: "', bucket, '")\n',
    '  |> range(start: ', start, ')\n'
  )

  if (!is.null(measurement) && nchar(measurement) > 0) {
    flux_query <- paste0(
      flux_query,
      '  |> filter(fn: (r) => r._measurement == "', measurement, '")\n'
    )
  }

  flux_query <- paste0(
    flux_query,
    '  |> keep(columns: ["device_id", "device_id_project"])\n',
    '  |> group(columns: ["device_id", "device_id_project"])\n',
    '  |> distinct(column: "device_id")\n',
    '  |> group()'
  )

  tryCatch({
    csv_content <- influxdb_query(flux_query, url, org, token)

    # Parse the response to get device_id and device_id_project pairs
    lines <- strsplit(csv_content, "\r?\n")[[1]]
    data_lines <- lines[!startsWith(trimws(lines), "#") & nchar(trimws(lines)) > 0]

    if (length(data_lines) < 2) {
      return(data.frame(device_id = character(0), device_id_project = character(0), stringsAsFactors = FALSE))
    }

    # Find column indices
    header <- strsplit(data_lines[1], ",")[[1]]
    device_id_idx <- which(header == "device_id")
    device_id_project_idx <- which(header == "device_id_project")

    results <- list()
    for (line in data_lines[-1]) {
      parts <- strsplit(line, ",")[[1]]

      device_id <- ""
      device_id_project <- ""

      if (length(device_id_idx) > 0 && length(parts) >= device_id_idx) {
        device_id <- trimws(parts[device_id_idx])
      }
      if (length(device_id_project_idx) > 0 && length(parts) >= device_id_project_idx) {
        device_id_project <- trimws(parts[device_id_project_idx])
      }

      if (nchar(device_id) > 0) {
        results[[length(results) + 1]] <- list(device_id = device_id, device_id_project = device_id_project)
      }
    }

    if (length(results) == 0) {
      return(data.frame(device_id = character(0), device_id_project = character(0), stringsAsFactors = FALSE))
    }

    df <- do.call(rbind, lapply(results, as.data.frame, stringsAsFactors = FALSE))

    # Remove duplicates - keep first occurrence per device_id
    df <- df[!duplicated(df$device_id), ]

    return(df)
  }, error = function(e) {
    print(paste("Error getting devices with names:", e$message))
    return(data.frame(device_id = character(0), device_id_project = character(0), stringsAsFactors = FALSE))
  })
}

# ======================================================================
# Get available sensors (tag values) for a LoRaWAN device
# ======================================================================
influxdb_get_device_sensors <- function(device_id, bucket = NULL, measurement = NULL,
                                         device_tag = "device_id",
                                         url = NULL, org = NULL, token = NULL) {
  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_BUCKET", "")

  if (nchar(bucket) == 0 || nchar(device_id) == 0) {
    return(character(0))
  }

  # Build query to find unique sensor tag values for a specific device
  flux_query <- paste0(
    'from(bucket: "', bucket, '")\n',
    '  |> range(start: -365d)\n'
  )

  if (!is.null(measurement) && nchar(measurement) > 0) {
    flux_query <- paste0(
      flux_query,
      '  |> filter(fn: (r) => r._measurement == "', measurement, '")\n'
    )
  }

  flux_query <- paste0(
    flux_query,
    '  |> filter(fn: (r) => r.', device_tag, ' == "', device_id, '")\n',
    '  |> filter(fn: (r) => exists r.sensor)\n',
    '  |> group(columns: ["sensor"])\n',
    '  |> first()\n',
    '  |> keep(columns: ["sensor"])'
  )

  tryCatch({
    csv_content <- influxdb_query(flux_query, url, org, token)

    # Parse CSV to extract sensor values
    lines <- strsplit(csv_content, "\r?\n")[[1]]
    data_lines <- lines[!startsWith(trimws(lines), "#") & nchar(trimws(lines)) > 0]

    if (length(data_lines) < 2) {
      return(character(0))
    }

    # Find sensor column index
    header <- strsplit(data_lines[1], ",")[[1]]
    sensor_idx <- which(header == "sensor")

    if (length(sensor_idx) == 0) {
      return(character(0))
    }

    # Extract sensor values from data rows
    sensors <- c()
    for (line in data_lines[-1]) {
      parts <- strsplit(line, ",")[[1]]
      if (length(parts) >= sensor_idx) {
        val <- trimws(parts[sensor_idx])
        if (nchar(val) > 0 && val != "sensor") {
          sensors <- c(sensors, val)
        }
      }
    }

    return(unique(sensors))
  }, error = function(e) {
    # Silently return empty if no sensor tags exist for this device
    return(character(0))
  })
}

# ======================================================================
# Get available tags for a bucket/measurement
# ======================================================================
influxdb_get_tags <- function(bucket = NULL, measurement = NULL,
                               url = NULL, org = NULL, token = NULL) {
  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_BUCKET", "")

  if (nchar(bucket) == 0) {
    stop("Bucket not specified")
  }

  flux_query <- paste0(
    'import "influxdata/influxdb/schema"\n',
    'schema.tagKeys(\n',
    '  bucket: "', bucket, '"'
  )

  if (!is.null(measurement) && nchar(measurement) > 0) {
    flux_query <- paste0(
      flux_query,
      ',\n  predicate: (r) => r._measurement == "', measurement, '"'
    )
  }

  flux_query <- paste0(flux_query, '\n)')

  tryCatch({
    csv_content <- influxdb_query(flux_query, url, org, token)
    df <- parse_influx_csv(csv_content)

    if (nrow(df) > 0 && "_value" %in% names(df)) {
      tags <- unique(df$`_value`)
      # Filter out internal tags
      tags <- tags[!tags %in% c("_start", "_stop", "_measurement")]
      return(tags)
    }

    return(character(0))
  }, error = function(e) {
    print(paste("Error getting tags:", e$message))
    return(character(0))
  })
}

# ======================================================================
# Get available fields for a bucket/measurement
# ======================================================================
influxdb_get_fields <- function(bucket = NULL, measurement = NULL,
                                 url = NULL, org = NULL, token = NULL) {
  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_BUCKET", "")

  if (nchar(bucket) == 0) {
    stop("Bucket not specified")
  }

  flux_query <- paste0(
    'import "influxdata/influxdb/schema"\n',
    'schema.fieldKeys(\n',
    '  bucket: "', bucket, '"'
  )

  if (!is.null(measurement) && nchar(measurement) > 0) {
    flux_query <- paste0(
      flux_query,
      ',\n  predicate: (r) => r._measurement == "', measurement, '"'
    )
  }

  flux_query <- paste0(flux_query, '\n)')

  tryCatch({
    csv_content <- influxdb_query(flux_query, url, org, token)
    df <- parse_influx_csv(csv_content)

    if (nrow(df) > 0 && "_value" %in% names(df)) {
      return(unique(df$`_value`))
    }

    return(character(0))
  }, error = function(e) {
    print(paste("Error getting fields:", e$message))
    return(character(0))
  })
}

# ======================================================================
# Get fields for a specific device
# ======================================================================
influxdb_get_device_fields <- function(device_id, bucket = NULL, measurement = NULL,
                                        device_tag = "device_id",
                                        url = NULL, org = NULL, token = NULL) {
  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_BUCKET", "")

  if (nchar(bucket) == 0 || nchar(device_id) == 0) {
    return(character(0))
  }

  # Query to find fields that have data for this device
  flux_query <- paste0(
    'from(bucket: "', bucket, '")\n',
    '  |> range(start: -365d)\n'
  )

  if (!is.null(measurement) && nchar(measurement) > 0) {
    flux_query <- paste0(
      flux_query,
      '  |> filter(fn: (r) => r._measurement == "', measurement, '")\n'
    )
  }

  flux_query <- paste0(
    flux_query,
    '  |> filter(fn: (r) => r.', device_tag, ' == "', device_id, '")\n',
    '  |> keep(columns: ["_field"])\n',
    '  |> distinct(column: "_field")'
  )

  tryCatch({
    csv_content <- influxdb_query(flux_query, url, org, token)
    df <- parse_influx_csv(csv_content)

    if (nrow(df) > 0 && "_value" %in% names(df)) {
      return(unique(df$`_value`))
    }

    return(character(0))
  }, error = function(e) {
    print(paste("Error getting device fields:", e$message))
    return(character(0))
  })
}

# ======================================================================
# Get last value for a specific device/field
# ======================================================================
influxdb_get_last_value <- function(device_id, field, bucket = NULL, measurement = NULL,
                                     device_tag = "device_id", sensor = NULL,
                                     url = NULL, org = NULL, token = NULL) {
  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_BUCKET", "")

  if (nchar(bucket) == 0 || nchar(device_id) == 0 || nchar(field) == 0) {
    return(NULL)
  }

  # Detect MeteoSwiss bucket - uses different data structure
  meteo_bucket <- Sys.getenv("INFLUXDB_METEO_BUCKET", "meteoSwiss")
  is_meteoswiss <- (bucket == meteo_bucket)

  flux_query <- paste0(
    'from(bucket: "', bucket, '")\n',
    '  |> range(start: -365d)\n'
  )

  if (is_meteoswiss) {
    # MeteoSwiss structure:
    # - _measurement = station code (e.g., "LUZ")
    # - sensor tag = sensor type (e.g., "tre200s0")
    # - _field = "value"
    flux_query <- paste0(
      flux_query,
      '  |> filter(fn: (r) => r._measurement == "', device_id, '")\n',
      '  |> filter(fn: (r) => r.sensor == "', field, '")\n',
      '  |> filter(fn: (r) => r._field == "value")\n',
      '  |> last()'
    )
  } else {
    # TTN/Standard structure
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
      '  |> filter(fn: (r) => r._field == "', field, '")\n',
      '  |> last()'
    )
  }

  tryCatch({
    csv_content <- influxdb_query(flux_query, url, org, token)

    # Parse CSV - use read.csv for more reliable parsing of all columns
    if (nchar(csv_content) > 0) {
      # Remove comment lines and parse
      lines <- strsplit(csv_content, "\r?\n")[[1]]
      data_lines <- lines[!startsWith(trimws(lines), "#") & nchar(trimws(lines)) > 0]

      if (length(data_lines) >= 2) {
        # Parse as CSV
        df <- tryCatch({
          read.csv(text = paste(data_lines, collapse = "\n"), stringsAsFactors = FALSE)
        }, error = function(e) {
          data.frame()
        })

        if (nrow(df) > 0) {
          # Find value column (could be _value or X_value due to R naming)
          value_col <- if ("_value" %in% names(df)) "_value" else if ("X_value" %in% names(df)) "X_value" else NULL
          time_col <- if ("_time" %in% names(df)) "_time" else if ("X_time" %in% names(df)) "X_time" else NULL

          if (!is.null(value_col)) {
            result <- list(
              value = df[[value_col]][1],
              time = if (!is.null(time_col)) df[[time_col]][1] else NA,
              field = field,
              application_id = if ("application_id" %in% names(df)) df$application_id[1] else NA
            )
            return(result)
          }
        }
      }
    }

    return(NULL)
  }, error = function(e) {
    print(paste("Error getting last value:", e$message))
    return(NULL)
  })
}

# ======================================================================
# Get application_id for a device from InfluxDB
# ======================================================================
influxdb_get_device_application_id <- function(device_id, bucket = NULL, measurement = NULL,
                                                device_tag = "device_id",
                                                url = NULL, org = NULL, token = NULL) {
  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_BUCKET", "")

  if (nchar(bucket) == 0 || nchar(device_id) == 0) {
    return(NA_character_)
  }

  # Query to get the application_id tag for this device
  flux_query <- paste0(
    'from(bucket: "', bucket, '")\n',
    '  |> range(start: -30d)\n'
  )

  if (!is.null(measurement) && nchar(measurement) > 0) {
    flux_query <- paste0(
      flux_query,
      '  |> filter(fn: (r) => r._measurement == "', measurement, '")\n'
    )
  }

  flux_query <- paste0(
    flux_query,
    '  |> filter(fn: (r) => r.', device_tag, ' == "', device_id, '")\n',
    '  |> keep(columns: ["application_id"])\n',
    '  |> distinct(column: "application_id")\n',
    '  |> limit(n: 1)'
  )

  tryCatch({
    csv_content <- influxdb_query(flux_query, url, org, token)

    if (nchar(csv_content) > 0) {
      lines <- strsplit(csv_content, "\r?\n")[[1]]
      data_lines <- lines[!startsWith(trimws(lines), "#") & nchar(trimws(lines)) > 0]

      if (length(data_lines) >= 2) {
        df <- tryCatch({
          read.csv(text = paste(data_lines, collapse = "\n"), stringsAsFactors = FALSE)
        }, error = function(e) {
          data.frame()
        })

        if (nrow(df) > 0 && "application_id" %in% names(df)) {
          return(as.character(df$application_id[1]))
        }
      }
    }

    return(NA_character_)
  }, error = function(e) {
    return(NA_character_)
  })
}

# ======================================================================
# Get MeteoSwiss stations from meteoSwiss bucket
# ======================================================================
influxdb_get_meteoswiss_stations <- function(bucket = NULL,
                                              url = NULL, org = NULL, token = NULL) {
  # Use environment variable for bucket if not provided
  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_METEO_BUCKET", "meteoSwiss")

  # Use MeteoSwiss-specific token if not provided
  if (is.null(token)) {
    token <- Sys.getenv("INFLUXDB_METEO_TOKEN", "")
    if (nchar(token) == 0) {
      stop("INFLUXDB_METEO_TOKEN not configured")
    }
  }

  if (nchar(bucket) == 0) {
    stop("INFLUXDB_METEO_BUCKET not configured")
  }

  # Get unique measurements (station codes) from meteoSwiss bucket
  flux_query <- paste0(
    'import "influxdata/influxdb/schema"\n',
    'schema.measurements(bucket: "', bucket, '")'
  )

  tryCatch({
    csv_content <- influxdb_query(flux_query, url, org, token)
    df <- parse_influx_csv(csv_content)

    if (nrow(df) > 0 && "_value" %in% names(df)) {
      stations <- unique(df$`_value`)
      return(stations)
    }

    return(character(0))
  }, error = function(e) {
    return(character(0))
  })
}

# ======================================================================
# Get available sensors (tag values) for a MeteoSwiss station
# ======================================================================
influxdb_get_meteoswiss_sensors <- function(station, bucket = NULL,
                                             url = NULL, org = NULL, token = NULL) {
  # Use environment variable for bucket if not provided
  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_METEO_BUCKET", "meteoSwiss")

  # Use MeteoSwiss-specific token if not provided
  if (is.null(token)) {
    token <- Sys.getenv("INFLUXDB_METEO_TOKEN", "")
    if (nchar(token) == 0) {
      stop("INFLUXDB_METEO_TOKEN not configured")
    }
  }

  if (nchar(bucket) == 0) {
    stop("INFLUXDB_METEO_BUCKET not configured")
  }

  if (nchar(station) == 0) {
    return(character(0))
  }

  # Query to find unique sensor tag values for a specific station (measurement)
  flux_query <- paste0(
    'import "influxdata/influxdb/schema"\n',
    'schema.tagValues(\n',
    '  bucket: "', bucket, '",\n',
    '  tag: "sensor",\n',
    '  predicate: (r) => r._measurement == "', station, '"\n',
    ')'
  )

  tryCatch({
    csv_content <- influxdb_query(flux_query, url, org, token)
    df <- parse_influx_csv(csv_content)

    if (nrow(df) > 0 && "_value" %in% names(df)) {
      return(unique(df$`_value`))
    }

    return(character(0))
  }, error = function(e) {
    print(paste("Error getting MeteoSwiss sensors:", e$message))
    return(character(0))
  })
}

# ======================================================================
# Get last value for a MeteoSwiss station/sensor
# ======================================================================
influxdb_get_meteoswiss_last_value <- function(station, sensor, bucket = NULL,
                                                url = NULL, org = NULL, token = NULL) {
  # Use environment variable for bucket if not provided
  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_METEO_BUCKET", "meteoSwiss")

  # Use MeteoSwiss-specific token if not provided
  if (is.null(token)) {
    token <- Sys.getenv("INFLUXDB_METEO_TOKEN", "")
    if (nchar(token) == 0) {
      stop("INFLUXDB_METEO_TOKEN not configured")
    }
  }

  if (nchar(bucket) == 0) {
    stop("INFLUXDB_METEO_BUCKET not configured")
  }

  if (nchar(station) == 0 || nchar(sensor) == 0) {
    return(NULL)
  }

  # Filter by measurement (station) and sensor tag
  flux_query <- paste0(
    'from(bucket: "', bucket, '")\n',
    '  |> range(start: -30d)\n',
    '  |> filter(fn: (r) => r._measurement == "', station, '")\n',
    '  |> filter(fn: (r) => r.sensor == "', sensor, '")\n',
    '  |> last()'
  )

  tryCatch({
    csv_content <- influxdb_query(flux_query, url, org, token)
    df <- parse_influx_csv(csv_content)

    if (nrow(df) > 0 && "_value" %in% names(df)) {
      result <- list(
        value = df$`_value`[1],
        time = if ("_time" %in% names(df)) df$`_time`[1] else NA,
        sensor = sensor
      )
      return(result)
    }

    return(NULL)
  }, error = function(e) {
    print(paste("Error getting MeteoSwiss last value:", e$message))
    return(NULL)
  })
}

# ======================================================================
# Get time series data for a device/field
# ======================================================================
influxdb_get_timeseries <- function(device_id, field, bucket = NULL, measurement = NULL,
                                     device_tag = "device_id", sensor = NULL,
                                     start = "-7d", stop = "now()",
                                     aggregation = NULL, aggregate_window = "1h",
                                     url = NULL, org = NULL, token = NULL) {
  if (is.null(bucket)) bucket <- Sys.getenv("INFLUXDB_BUCKET", "")

  if (nchar(bucket) == 0 || nchar(device_id) == 0 || nchar(field) == 0) {
    return(data.frame())
  }

  flux_query <- paste0(
    'from(bucket: "', bucket, '")\n',
    '  |> range(start: ', start, ', stop: ', stop, ')\n'
  )

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

  # Add aggregation if specified
  if (!is.null(aggregation) && aggregation %in% c("mean", "sum", "min", "max", "count")) {
    flux_query <- paste0(
      flux_query,
      '  |> aggregateWindow(every: ', aggregate_window, ', fn: ', aggregation, ', createEmpty: false)\n'
    )
  }

  flux_query <- paste0(flux_query, '  |> yield(name: "result")')

  tryCatch({
    csv_content <- influxdb_query(flux_query, url, org, token)
    df <- parse_influx_csv(csv_content)

    if (nrow(df) > 0) {
      # Clean up the dataframe
      result <- data.frame(
        time = if ("_time" %in% names(df)) as.POSIXct(df$`_time`, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC") else NA,
        value = if ("_value" %in% names(df)) as.numeric(df$`_value`) else NA,
        stringsAsFactors = FALSE
      )
      result <- result[!is.na(result$time), ]
      return(result)
    }

    return(data.frame())
  }, error = function(e) {
    print(paste("Error getting timeseries:", e$message))
    return(data.frame())
  })
}
