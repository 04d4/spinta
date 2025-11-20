"""
SAS Helper Functions

This module provides utility functions for working with SAS-specific data types
and operations within the Spinta framework.

SAS uses a different epoch date (January 1, 1960) compared to most systems,
requiring special conversion functions for date, datetime, and time values.
"""

from collections.abc import Sequence
from typing import Union
from datetime import date, datetime, time, timedelta

import sqlalchemy as sa


# SAS epoch: January 1, 1960
SAS_EPOCH_DATE = date(1960, 1, 1)


def sas_date_to_python(sas_date_value):
    """
    Convert SAS date value to Python date object.

    SAS stores dates as the number of days since January 1, 1960.
    This function converts that integer value to a Python date object.

    Args:
        sas_date_value: Integer representing days since SAS epoch (1960-01-01),
                       or None for NULL values

    Returns:
        date: Python date object, or None if input is None

    Example:
        >>> sas_date_to_python(0)      # Returns date(1960, 1, 1)
        >>> sas_date_to_python(365)    # Returns date(1961, 1, 1)
        >>> sas_date_to_python(None)   # Returns None

    Note:
        SAS dates can be negative for dates before 1960-01-01.
    """
    if sas_date_value is None:
        return None

    try:
        # Add the days to the SAS epoch
        return SAS_EPOCH_DATE + timedelta(days=int(sas_date_value))
    except (ValueError, OverflowError, TypeError) as e:
        # Handle invalid dates gracefully
        raise ValueError(f"Invalid SAS date value: {sas_date_value}") from e


def sas_datetime_to_python(sas_datetime_value):
    """
    Convert SAS datetime value to Python datetime object.

    SAS stores datetimes as the number of seconds since January 1, 1960 00:00:00.
    This function converts that floating-point value to a Python datetime object.

    Args:
        sas_datetime_value: Float representing seconds since SAS epoch (1960-01-01 00:00:00),
                           or None for NULL values

    Returns:
        datetime: Python datetime object, or None if input is None

    Example:
        >>> sas_datetime_to_python(0)           # Returns datetime(1960, 1, 1, 0, 0, 0)
        >>> sas_datetime_to_python(86400)       # Returns datetime(1960, 1, 2, 0, 0, 0)
        >>> sas_datetime_to_python(3600.5)      # Returns datetime(1960, 1, 1, 1, 0, 0, 500000)
        >>> sas_datetime_to_python(None)        # Returns None

    Note:
        - SAS datetimes can be negative for dates before 1960-01-01
        - The value can include fractional seconds
    """
    if sas_datetime_value is None:
        return None

    try:
        # Convert SAS epoch to datetime
        sas_epoch_datetime = datetime(1960, 1, 1, 0, 0, 0)

        # Add the seconds (can be fractional) to the SAS epoch
        return sas_epoch_datetime + timedelta(seconds=float(sas_datetime_value))
    except (ValueError, OverflowError, TypeError) as e:
        # Handle invalid datetimes gracefully
        raise ValueError(f"Invalid SAS datetime value: {sas_datetime_value}") from e


def sas_time_to_python(sas_time_value):
    """
    Convert SAS time value to Python time object.

    SAS stores times as the number of seconds since midnight (00:00:00).
    This function converts that floating-point value to a Python time object.

    Args:
        sas_time_value: Float representing seconds since midnight,
                       or None for NULL values

    Returns:
        time: Python time object, or None if input is None

    Example:
        >>> sas_time_to_python(0)           # Returns time(0, 0, 0)
        >>> sas_time_to_python(3600)        # Returns time(1, 0, 0)
        >>> sas_time_to_python(3661.5)      # Returns time(1, 1, 1, 500000)
        >>> sas_time_to_python(None)        # Returns None

    Note:
        - Valid values are 0 to 86400 (24 hours)
        - The value can include fractional seconds
        - Values >= 86400 will raise ValueError
    """
    if sas_time_value is None:
        return None

    try:
        # Validate the time value is within a day
        seconds = float(sas_time_value)
        if seconds < 0 or seconds >= 86400:
            raise ValueError(f"Time value must be between 0 and 86400 seconds, got {seconds}")

        # Calculate hours, minutes, seconds, and microseconds
        hours = int(seconds // 3600)
        remaining = seconds % 3600
        minutes = int(remaining // 60)
        remaining = remaining % 60
        secs = int(remaining)
        microsecs = int((remaining - secs) * 1_000_000)

        return time(hours, minutes, secs, microsecs)
    except (ValueError, TypeError) as e:
        # Handle invalid times gracefully
        raise ValueError(f"Invalid SAS time value: {sas_time_value}") from e


def group_array(column: Union[sa.Column, Sequence[sa.Column]]):
    """
    Create an array aggregation expression for SAS.

    SAS does not natively support array aggregation like PostgreSQL's array_agg.
    This function implements array aggregation using SAS's CATX function for
    comma-separated string concatenation.

    Args:
        column: SQLAlchemy Column or sequence of Columns to aggregate

    Returns:
        SQLAlchemy expression for array aggregation using string concatenation

    Example:
        >>> # Single column aggregation
        >>> group_array(table.c.name)
        >>> # Results in: CATX(',', name)

        >>> # Multiple column aggregation
        >>> group_array([table.c.first_name, table.c.last_name])
        >>> # Results in: CATX(',', first_name, last_name)

    Note:
        - Values are concatenated with comma delimiter
        - NULL values are automatically ignored by CATX
        - Result is a comma-separated string, not a true array type
        - Consumers should split the result string to obtain individual values
    """
    if isinstance(column, Sequence) and not isinstance(column, str):
        # Multiple columns: concatenate them all with comma delimiter
        # CATX(',', col1, col2, ...) concatenates with delimiter, skipping NULLs
        return sa.func.catx(",", *column)
    else:
        # Single column: use CATX with comma delimiter
        # CATX(',', col) automatically handles NULL values
        return sa.func.catx(",", column)
