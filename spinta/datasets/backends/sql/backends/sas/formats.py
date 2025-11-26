"""
SAS Format Constants and Type Mapping.

This module provides SAS format definitions and type mapping logic for
converting SAS data types to SQLAlchemy types.

SAS uses formats to indicate how data should be displayed and what the
underlying data type represents. This module provides:
- Format constant definitions for categorization
- Type mapping logic from SAS types to SQLAlchemy types

SAS has only two basic data types (numeric and character), but formats
are used to indicate specialized types like dates, times, booleans,
and formatted numbers.
"""

import re
import logging
from sqlalchemy import types as sqltypes


logger = logging.getLogger(__name__)


# =============================================================================
# ISO 8601 Format Prefixes
# =============================================================================

ISO_DATE_PREFIXES = ("E8601DA",)
ISO_DATETIME_PREFIXES = ("E8601DT",)
ISO_TIME_PREFIXES = ("E8601TM",)


# =============================================================================
# Standard SAS Date Formats
# =============================================================================

DATE_FORMATS = frozenset(
    [
        "DATE",
        "DAY",
        "DDMMYY",
        "MMDDYY",
        "YYMMDD",
        "YYMM",
        "YYMON",
        "YYQ",
        "YYQR",
        "JULDAY",
        "JULIAN",
        "MONYY",
        "MONNAME",
        "MONTH",
        "QTR",
        "QTRR",
        "WEEKDATE",
        "WEEKDATX",
        "WEEKDAY",
        "WORDDATE",
        "WORDDATX",
        "YEAR",
        "NENGO",
        "MINGUO",
        "PDJULG",
        "PDJULI",
        "EURDFDD",
        "EURDFDE",
        "EURDFDN",
        "EURDFDMY",
        "EURDFMY",
        "EURDFWDX",
        "EURDFWKX",
        "WEEKV",
    ]
)


# =============================================================================
# SAS DateTime Formats
# =============================================================================

DATETIME_FORMATS = frozenset(
    [
        "DATETIME",
        "DTYYQC",
    ]
)

TIMESTAMP_FORMATS = frozenset(
    [
        "TODSTAMP",
        "DTMONYY",
        "DTWKDATX",
        "DTYEAR",
        "DTYYQC",
    ]
)


# =============================================================================
# SAS Time Formats
# =============================================================================

TIME_FORMATS = frozenset(
    [
        "TIME",
        "TIMEAMPM",
        "TOD",
        "HHMM",
        "HOUR",
        "MMSS",
        "NLTIME",
        "NLTIMAP",
        "STIMER",
    ]
)


# =============================================================================
# Boolean Formats
# =============================================================================

BOOLEAN_FORMATS = frozenset(
    [
        "YESNO",
        "YN",
        "BOOLEAN",
    ]
)


# =============================================================================
# Numeric Formats (with potential decimal places)
# =============================================================================

NUMERIC_FORMATS = frozenset(
    [
        "COMMA",
        "COMMAX",
        "DOLLAR",
        "EURX",
        "EURO",
        "PERCENT",
        "BEST",
        "NLMNY",
        "NLMNYI",
        "NLNUM",
        "NLNUMI",
        "NLPCT",
        "NLPCTI",
        "SSN",
        "PVALUE",
        "NEGPAREN",
        "ROMAN",
        "WORDS",
        "WORDF",
    ]
)


# =============================================================================
# Integer Formats (no decimals)
# =============================================================================

INTEGER_FORMATS = frozenset(
    [
        "Z",
        "ZD",
        "BINARY",
        "HEX",
        "OCTAL",
        "IB",
        "PD",
        "PK",
        "RB",
        "PIB",
        "ZIP",
        "NUMX",
        "S370FF",
        "S370FIB",
        "S370FPIB",
        "S370FPD",
        "S370FRB",
        "S370FZD",
    ]
)


# =============================================================================
# Standard Numeric Format Identifiers
# =============================================================================

STANDARD_NUMERIC_FORMATS = frozenset(
    [
        "F",
        "NUMX",
        "NUMERIC",
    ]
)


# =============================================================================
# Format Parsing Functions
# =============================================================================


def parse_sas_format(format_str: str) -> dict:
    """
    Parse a SAS format string to extract format name, width, and decimals.

    SAS formats follow the pattern: NAME[width[.decimals]]

    Examples:
        "COMMA12.2" -> {"format": "COMMA", "width": 12, "decimals": 2}
        "DATE9." -> {"format": "DATE", "width": 9, "decimals": None}
        "DATETIME20." -> {"format": "DATETIME", "width": 20, "decimals": None}
        "$20." -> {"format": "$", "width": 20, "decimals": None}

    Args:
        format_str: SAS format string (e.g., "COMMA12.2", "DATE9.")

    Returns:
        Dictionary with 'format', 'width', and 'decimals' keys
    """
    if not format_str:
        return {"format": None, "width": None, "decimals": None}

    try:
        # Match SAS format pattern: NAME[width[.decimals]]
        # Examples: DATE9., COMMA12.2, DOLLAR10., BEST12.
        match = re.match(r"^([A-Z]+\$?)(\d+)?(?:\.(\d+)?)?\.?$", format_str.upper().strip())

        if match:
            format_name = match.group(1)
            width = int(match.group(2)) if match.group(2) else None
            decimals = int(match.group(3)) if match.group(3) else None

            return {"format": format_name, "width": width, "decimals": decimals}
        else:
            # If regex doesn't match, try simpler parse
            clean_format = format_str.upper().strip().rstrip(".")
            return {"format": clean_format, "width": None, "decimals": None}
    except Exception as e:
        logger.warning(f"Failed to parse SAS format '{format_str}': {e}")
        return {"format": format_str.upper() if format_str else None, "width": None, "decimals": None}


# =============================================================================
# Type Mapping Functions
# =============================================================================


def map_sas_type_to_sqlalchemy(sas_type: str | None, length, format_str: str | None, cache: dict | None = None):
    """
    Map SAS data types to SQLAlchemy types with comprehensive format support.

    SAS has two basic types (numeric and character) but uses formats
    to indicate specialized types like dates, times, booleans, and formatted numbers.

    This function handles 50+ SAS formats including date/time, numeric, and special formats.

    Args:
        sas_type: SAS type ('num' or 'char')
        length: Column length (can be string representation of int or float)
        format_str: SAS format string (e.g., 'DATE9.', 'DATETIME20.', 'COMMA12.2')
        cache: Optional cache dictionary for performance optimization

    Returns:
        SQLAlchemy type instance
    """
    try:
        # Check cache first for performance
        if cache is not None:
            cache_key = f"{sas_type}_{format_str}"
            if cache_key in cache:
                return cache[cache_key]
        else:
            cache_key = None

        # Handle character types
        if sas_type and sas_type.lower() == "char":
            # Length might be a float string like '50.0', convert via float first
            sa_type = sqltypes.VARCHAR(length=int(float(length)))
            if cache is not None:
                cache[cache_key] = sa_type
            return sa_type

        # Numeric type - determine specific type based on format
        if format_str:
            # Parse the format string for detailed analysis
            format_info = parse_sas_format(format_str)
            format_name = format_info["format"]
            decimals = format_info["decimals"]

            if format_name:
                # ISO 8601 formats (E8601*)
                if format_name.startswith(ISO_DATE_PREFIXES):
                    # E8601DA* = Date format (ISO 8601 Date)
                    sa_type = sqltypes.DATE()
                    if cache is not None:
                        cache[cache_key] = sa_type
                    return sa_type

                if format_name.startswith(ISO_DATETIME_PREFIXES):
                    # E8601DT* = DateTime format (ISO 8601 DateTime)
                    sa_type = sqltypes.DATETIME()
                    if cache is not None:
                        cache[cache_key] = sa_type
                    return sa_type

                if format_name.startswith(ISO_TIME_PREFIXES):
                    # E8601TM* = Time format (ISO 8601 Time)
                    sa_type = sqltypes.TIME()
                    if cache is not None:
                        cache[cache_key] = sa_type
                    return sa_type

                # DateTime formats (must check before DATE)
                if format_name.startswith("DATETIME") or format_name in DATETIME_FORMATS:
                    sa_type = sqltypes.DATETIME()
                    if cache is not None:
                        cache[cache_key] = sa_type
                    return sa_type

                # Timestamp formats
                if format_name in TIMESTAMP_FORMATS:
                    sa_type = sqltypes.DATETIME()
                    if cache is not None:
                        cache[cache_key] = sa_type
                    return sa_type

                # Standard SAS date formats
                if any(format_name.startswith(fmt) for fmt in DATE_FORMATS):
                    sa_type = sqltypes.DATE()
                    if cache is not None:
                        cache[cache_key] = sa_type
                    return sa_type

                # Time formats
                if any(format_name.startswith(fmt) for fmt in TIME_FORMATS):
                    sa_type = sqltypes.TIME()
                    if cache is not None:
                        cache[cache_key] = sa_type
                    return sa_type

                # Boolean formats
                if format_name in BOOLEAN_FORMATS:
                    sa_type = sqltypes.BOOLEAN()
                    if cache is not None:
                        cache[cache_key] = sa_type
                    return sa_type

                # Numeric formats with potential decimal places
                if any(format_name.startswith(fmt) for fmt in NUMERIC_FORMATS):
                    # Check if format has decimal places
                    if decimals is not None and decimals > 0:
                        sa_type = sqltypes.NUMERIC(precision=format_info.get("width"), scale=decimals)
                    else:
                        # No decimals - treat as integer
                        sa_type = sqltypes.INTEGER()
                    if cache is not None:
                        cache[cache_key] = sa_type
                    return sa_type

                # Explicit integer formats (no decimals)
                if any(format_name.startswith(fmt) for fmt in INTEGER_FORMATS):
                    sa_type = sqltypes.INTEGER()
                    if cache is not None:
                        cache[cache_key] = sa_type
                    return sa_type

                # Standard numeric format 'F' or 'NUMX' - check for decimals
                if format_name in STANDARD_NUMERIC_FORMATS:
                    if decimals is not None and decimals > 0:
                        sa_type = sqltypes.NUMERIC(precision=format_info.get("width"), scale=decimals)
                    else:
                        sa_type = sqltypes.INTEGER()
                    if cache is not None:
                        cache[cache_key] = sa_type
                    return sa_type

                # Log unrecognized format for debugging
                logger.debug(f"Unrecognized SAS format: {format_str}, using default NUMERIC type")

        # Default numeric type for unformatted or unrecognized formats
        sa_type = sqltypes.NUMERIC()
        if cache is not None:
            cache[cache_key] = sa_type
        return sa_type

    except Exception as e:
        # Comprehensive error handling - log and return safe default
        logger.warning(
            f"Error mapping SAS type to SQLAlchemy: sas_type={sas_type}, "
            f"length={length}, format={format_str}, error={e}"
        )
        # Return safe default type based on sas_type
        if sas_type and sas_type.lower() == "char":
            try:
                return sqltypes.VARCHAR(length=int(float(length)))
            except (ValueError, TypeError):
                return sqltypes.VARCHAR(length=255)  # Safe default length
        else:
            return sqltypes.NUMERIC()  # Safe default for numeric types
