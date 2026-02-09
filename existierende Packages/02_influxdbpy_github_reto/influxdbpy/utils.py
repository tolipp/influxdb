"""
Utility functions for building queries for InfluxDB.
"""

def get_fieldkey(func, fieldKey="value"):
    """
    Get the field key string for the InfluxDB query.

    Parameters:
    ----------
    func : str
        The aggregation function.
    fieldKey : str, optional
        The field key to aggregate (default is "value").
    
    Returns:
    -------
    str
        The field key formatted for the query.
    """
    allowed_funcs = ["mean", "median", "min", "max", "percentile", "sum", "count"]
    if func not in allowed_funcs:
        return f'"{fieldKey}"'  # Default to raw value if unknown function
    return f'{func}("{fieldKey}")'

def get_groupby(func, agg="1d"):
    """
    Get the group by clause for the InfluxDB query.

    Parameters:
    ----------
    func : str
        The aggregation function.
    agg : str, optional
        The time aggregation interval (default is "1d").
    
    Returns:
    -------
    str
        The group by clause for the query.
    """
    return 'time(NaN)' if func == 'raw' else f'time({agg})'

def get_tags(tags):
    """
    Generate the tags part of the query.

    Parameters:
    ----------
    tags : dict
        Dictionary of tags.

    Returns:
    -------
    str
        Tags formatted for the query.
    """
    tag_string = ""
    if tags:
        tag_string = " AND " + " OR ".join([f'"{key}"=\'{value}\'' for key, value in tags.items()])
    return tag_string

def build_time_condition(datetimeStart, datetimeEnd):
    """
    Build the time condition for the query.

    Parameters:
    ----------
    datetimeStart : str, optional
        The start time for the query.
    datetimeEnd : str, optional
        The end time for the query.
    
    Returns:
    -------
    str
        The time condition for the query.
    """
    condition = ""
    if datetimeStart:
        condition += f"time >= '{datetimeStart}'"
    if datetimeEnd:
        condition += f" AND time <= '{datetimeEnd}'" if datetimeStart else f"time <= '{datetimeEnd}'"
    return condition
