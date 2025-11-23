import logging
import re
from datetime import datetime, date, time, timedelta
from sqlalchemy import types as sqltypes, pool
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.compiler import IdentifierPreparer


# Set up logger for SAS dialect debugging
logger = logging.getLogger(__name__)


class SASStringType(sqltypes.TypeDecorator):
    """
    Enhanced string type for SAS that handles encoding, space stripping, and missing values.

    SAS often pads character fields with spaces, so this type ensures
    that returned string values have trailing spaces removed. Also handles
    encoding issues and missing value detection.
    """

    impl = sqltypes.VARCHAR
    cache_ok = True

    def __init__(self, length=None, strip_spaces=True, **kwargs):
        """
        Initialize the SAS string type.

        Args:
            length: Maximum string length
            strip_spaces: Whether to strip leading/trailing spaces (default: True)
            **kwargs: Additional arguments passed to VARCHAR
        """
        super().__init__(**kwargs)
        self.length = length
        self.strip_spaces = strip_spaces
        if length:
            self.impl = sqltypes.VARCHAR(length=length)

    def process_result_value(self, value, dialect):
        """
        Process the result value with enhanced string handling.

        Args:
            value: The raw value from the database
            dialect: The dialect instance

        Returns:
            The processed value with proper encoding and space handling
        """
        if value is None:
            return None

        # Handle empty strings as potential missing values
        if isinstance(value, str):
            if self.strip_spaces:
                value = value.strip()

            # Empty strings after stripping could be missing values
            if not value:
                return None

            # Handle encoding issues - ensure proper string encoding
            try:
                # Ensure the string is properly decoded
                if isinstance(value, bytes):
                    value = value.decode("utf-8", errors="replace")
            except (UnicodeDecodeError, AttributeError):
                pass

            return value

        return str(value) if value else None


class SASDateType(sqltypes.TypeDecorator):
    """
    SAS Date type that converts numeric days since 1960-01-01 to Python date objects.

    SAS stores dates as the number of days since January 1, 1960.
    """

    impl = sqltypes.DATE
    cache_ok = True

    # SAS epoch: January 1, 1960
    SAS_EPOCH_DATE = date(1960, 1, 1)

    def process_result_value(self, value, dialect):
        """
        Convert SAS numeric date value to Python date object.

        Args:
            value: Numeric value representing days since 1960-01-01
            dialect: The dialect instance

        Returns:
            Python date object or None for missing values
        """
        if value is None:
            return None

        try:
            # Check for SAS missing values
            if self._is_sas_missing(value):
                return None

            # Convert numeric days to date
            days = int(float(value))
            return self.SAS_EPOCH_DATE + timedelta(days=days)
        except (ValueError, OverflowError, TypeError) as e:
            logger.warning(f"Invalid SAS date value: {value} - {e}")
            return None

    def _is_sas_missing(self, value):
        """Check if value represents a SAS missing value."""
        try:
            float_val = float(value)
            # Check for NaN or special missing values
            if float_val != float_val:  # NaN check
                return True
            # SAS missing values are typically very large negative numbers
            if float_val < -1e10:
                return True
            return False
        except (ValueError, TypeError):
            return True


class SASDateTimeType(sqltypes.TypeDecorator):
    """
    SAS DateTime type that converts numeric seconds since 1960-01-01 to Python datetime objects.

    SAS stores datetimes as the number of seconds since January 1, 1960 00:00:00.
    """

    impl = sqltypes.DATETIME
    cache_ok = True

    # SAS epoch: January 1, 1960 00:00:00
    SAS_EPOCH_DATETIME = datetime(1960, 1, 1, 0, 0, 0)

    def process_result_value(self, value, dialect):
        """
        Convert SAS numeric datetime value to Python datetime object.

        Args:
            value: Numeric value representing seconds since 1960-01-01 00:00:00
            dialect: The dialect instance

        Returns:
            Python datetime object or None for missing values
        """
        if value is None:
            return None

        try:
            # Check for SAS missing values
            if self._is_sas_missing(value):
                return None

            # Convert numeric seconds to datetime
            seconds = float(value)
            return self.SAS_EPOCH_DATETIME + timedelta(seconds=seconds)
        except (ValueError, OverflowError, TypeError) as e:
            logger.warning(f"Invalid SAS datetime value: {value} - {e}")
            return None

    def _is_sas_missing(self, value):
        """Check if value represents a SAS missing value."""
        try:
            float_val = float(value)
            # Check for NaN or special missing values
            if float_val != float_val:  # NaN check
                return True
            # SAS missing values are typically very large negative numbers
            if float_val < -1e10:
                return True
            return False
        except (ValueError, TypeError):
            return True


class SASTimeType(sqltypes.TypeDecorator):
    """
    SAS Time type that converts numeric seconds to Python time objects.

    SAS stores times as the number of seconds since midnight.
    """

    impl = sqltypes.TIME
    cache_ok = True

    def process_result_value(self, value, dialect):
        """
        Convert SAS numeric time value to Python time object.

        Args:
            value: Numeric value representing seconds since midnight
            dialect: The dialect instance

        Returns:
            Python time object or None for missing values
        """
        if value is None:
            return None

        try:
            # Check for SAS missing values
            if self._is_sas_missing(value):
                return None

            # Convert numeric seconds to time
            total_seconds = int(float(value))
            hours = (total_seconds // 3600) % 24
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            return time(hours, minutes, seconds)
        except (ValueError, OverflowError, TypeError) as e:
            logger.warning(f"Invalid SAS time value: {value} - {e}")
            return None

    def _is_sas_missing(self, value):
        """Check if value represents a SAS missing value."""
        try:
            float_val = float(value)
            # Check for NaN or special missing values
            if float_val != float_val:  # NaN check
                return True
            # SAS missing values are typically very large negative numbers
            if float_val < -1e10:
                return True
            return False
        except (ValueError, TypeError):
            return True


class SASIdentifierPreparer(IdentifierPreparer):
    """
    Custom identifier preparer for SAS that never quotes identifiers.

    SAS does not support quoted identifiers in SQL syntax. This preparer
    ensures that table names, column names, and other identifiers are never
    wrapped in quotes, preventing SQL syntax errors.
    """

    def quote(self, ident, force=None):
        """Return the identifier without quotes."""
        return ident

    def _requires_quotes(self, ident):
        """SAS never requires quotes for identifiers."""
        return False


class BaseDialect(object):
    jdbc_db_name = ""
    jdbc_driver_name = ""
    supports_native_decimal = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_unicode_binds = True
    description_encoding = None

    @classmethod
    def dbapi(cls):
        import jaydebeapi

        return jaydebeapi

    def is_disconnect(self, e, connection, cursor):
        if not isinstance(e, self.dbapi().ProgrammingError):
            return False
        e = str(e)
        return "connection is closed" in e or "cursor is closed" in e

    def do_rollback(self, dbapi_connection):
        pass


class SASDialect(BaseDialect, DefaultDialect):
    """
    SQLAlchemy dialect for SAS databases using JDBC.

    This dialect provides connectivity to SAS databases through the SAS IOM JDBC driver,
    enabling schema introspection and query execution against SAS libraries and datasets.

    Configuration:
        - jdbc_db_name: "sasiom" (required for JDBC URL construction)
        - jdbc_driver_name: "com.sas.rio.MVADriver"

    Example connection URL:
        jdbc:sasiom://host:port/?schema=libname
    """

    # Dialect identification
    name = "sas"
    driver = "jdbc"  # Required by SQLAlchemy for sas+jdbc:// URLs
    jdbc_db_name = "sasiom"
    jdbc_driver_name = "com.sas.rio.MVADriver"

    # Feature support flags
    supports_comments = True

    # Schema and transaction support
    supports_schemas = True
    supports_views = True
    requires_name_normalize = True

    # Transaction handling (SAS operates in auto-commit mode)
    supports_transactions = False
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False

    # SAS does not use quoted identifiers - disable quoting
    quote_identifiers = False

    # TODO(oa): Tikrai reikalingi!
    supports_statement_cache = True

    @classmethod
    def get_dialect_pool_class(cls, url):
        """
        Return the connection pool class to use with enhanced error recovery.

        This method is required by SQLAlchemy's engine creation.
        Uses QueuePool with SAS-specific configuration for better connection handling.
        """
        return pool.QueuePool

    @classmethod
    def get_dialect_cls(cls, url):
        """
        Return the dialect class for SQLAlchemy's dialect loading mechanism.

        This method is required by SQLAlchemy's dialect registry system.

        Args:
            url: SQLAlchemy URL object

        Returns:
            The SASDialect class
        """
        return cls

    def __init__(self, **kwargs):
        """
        Initialize the SAS dialect with enhanced connection pooling configuration.

        Calls DefaultDialect.__init__ to set up all SQLAlchemy infrastructure.
        BaseDialect has no __init__, so super() correctly resolves to DefaultDialect.
        """
        # Configure connection pooling parameters for better SAS connection handling
        pool_kwargs = {
            "pool_size": 5,  # Smaller pool size for SAS connections
            "max_overflow": 10,  # Allow overflow connections
            "pool_timeout": 30,  # Connection timeout
            "pool_recycle": 3600,  # Recycle connections every hour
            "pool_pre_ping": True,  # Test connections before use
        }

        # Merge with any existing pool kwargs
        if "poolclass" not in kwargs:
            kwargs["poolclass"] = self.get_dialect_pool_class(None)

        # Update kwargs with pool configuration
        kwargs.update(pool_kwargs)

        # This calls DefaultDialect.__init__(**kwargs)
        super().__init__(**kwargs)

        # Override the identifier preparer with our custom SAS version
        self.identifier_preparer = SASIdentifierPreparer(self)

        # Initialize type mapping cache for performance optimization
        self._type_mapping_cache = {}

        # SAS-specific initialization if needed
        # self.default_schema_name will be set by initialize()

    def _configure_jvm_memory(self):
        """
        Configure JVM memory settings to prevent OutOfMemoryError during SAS operations.

        This method sets appropriate JVM heap sizes and garbage collection options
        to handle large SAS datasets without causing memory issues.
        """
        import os

        # Set JVM heap size limits to prevent memory issues
        # Default to reasonable limits if not already set
        jvm_opts = []

        # Check for existing JVM options
        existing_opts = os.environ.get("_JAVA_OPTIONS", "")
        if existing_opts:
            jvm_opts.extend(existing_opts.split())

        # Add memory management if not already present
        has_heap_min = any("-Xms" in opt for opt in jvm_opts)
        has_heap_max = any("-Xmx" in opt for opt in jvm_opts)

        if not has_heap_min:
            # Set minimum heap size to 256MB
            jvm_opts.append("-Xms256m")

        if not has_heap_max:
            # Set maximum heap size to 1GB to prevent excessive memory usage
            jvm_opts.append("-Xmx1g")

        # Add garbage collection tuning for better memory management
        has_gc_tuning = any("-XX:" in opt for opt in jvm_opts)
        if not has_gc_tuning:
            # Use G1GC for better performance with large datasets
            jvm_opts.extend(["-XX:+UseG1GC", "-XX:MaxGCPauseMillis=200"])

        # Update environment variable
        os.environ["_JAVA_OPTIONS"] = " ".join(jvm_opts)

    def on_connect_url(self, url):
        """
        Return a callable to be executed on new connections.

        SAS doesn't need any special connection initialization.

        Args:
            url: SQLAlchemy URL object

        Returns:
            None (no special initialization needed)
        """
        self.url = url
        return None

    def initialize(self, connection):
        """
        Initialize dialect with connection-specific settings and fallback mechanisms.

        Args:
            connection: Database connection object
        """
        logger = logging.getLogger(__name__)
        try:
            # BaseDialect may or may not have initialize method depending on version
            # Only call parent if it exists
            if hasattr(super(SASDialect, self), "initialize"):
                super(SASDialect, self).initialize(connection)

            # SQLAlchemy will set attributes via other mechanisms
            # Extract schema from URL query parameters if available
            if hasattr(self, "url") and self.url and self.url.query:
                schema_from_url = self.url.query.get("schema")
                logger.debug(f"SAS dialect: Found schema in URL query: {schema_from_url}")
                self.default_schema_name = schema_from_url
            else:
                logger.debug("SAS dialect: No schema found in URL query, using empty string")
                self.default_schema_name = ""

            logger.debug(f"SAS dialect: default_schema_name set to: '{self.default_schema_name}'")

        except Exception as e:
            # Log initialization errors but don't fail completely
            logger.warning(f"SAS dialect initialization failed: {e}. Using fallback settings.")
            self.default_schema_name = ""

    def create_connect_args(self, url):
        """
        Parse the SQLAlchemy URL and create JDBC connection arguments with JVM memory management.

        The SAS JDBC URL format is:
            jdbc:sasiom://host:port/?schema=libname

        Additional options can be passed as query parameters.

        Args:
            url: SQLAlchemy URL object

        Returns:
            Tuple of (args, kwargs) for JDBC connection compatible with jaydebeapi
        """
        logger.debug(f"Creating connection args for URL: {url}")

        try:
            # Configure JVM memory settings before creating connection
            # self._configure_jvm_memory()

            # Build JDBC URL
            jdbc_url = f"jdbc:{self.jdbc_db_name}://{url.host}"

            if url.port:
                jdbc_url += f":{url.port}"

            logger.debug(f"Built JDBC URL: {jdbc_url}")

            # Base driver arguments
            # IMPORTANT: All driver_args values MUST be strings for java.util.Properties
            # jaydebeapi converts these to Java Properties which only accepts String values
            driver_args = {"user": url.username or "", "password": url.password or "", "applyFormats": "false"}

            # Log driver_args with types for debugging
            logger.debug(f"Driver args with types: {[(k, type(v).__name__, v) for k, v in driver_args.items()]}")

            # Add log4j configuration to suppress warnings
            # Set system properties that will be picked up by the Java process
            from pathlib import Path

            # Find the log4j.properties file in the same directory as this dialect
            current_dir = Path(__file__).parent
            log4j_config_path = current_dir / "log4j.properties"

            if log4j_config_path.exists():
                # Add log4j configuration as a system property
                driver_args["log4j.configuration"] = f"file://{log4j_config_path.absolute()}"

                # Also add the directory to the classpath for automatic loading
                # jaydebeapi will add this to the JVM classpath
                jars = [str(current_dir)]
            else:
                jars = []

            # Add query parameters if present
            if url.query:
                # Query parameters could be added to driver_args if needed
                logger.debug(f"Query parameters: {dict(url.query)}")
                pass

            # jaydebeapi expects: connect(jclassname, url, driver_args, jars, libs)
            # Return format compatible with jaydebeapi.connect()
            kwargs = {
                "jclassname": self.jdbc_driver_name,
                "url": jdbc_url,
                "driver_args": driver_args,
            }

            # Add jars parameter if log4j configuration directory was found
            if jars is not None:
                kwargs["jars"] = jars

            logger.debug(f"Connection args created successfully: jclassname={self.jdbc_driver_name}, url={jdbc_url}")
            return ((), kwargs)

        except Exception as e:
            logger.error(f"Error in create_connect_args: {e}", exc_info=True)
            raise

    def do_rollback(self, dbapi_connection):
        """
        Handle transaction rollback.

        SAS operates in auto-commit mode and does not support transactions,
        so this is a no-op.

        Args:
            dbapi_connection: JDBC connection object
        """
        # SAS doesn't support transactions - no-op
        pass

    def do_commit(self, dbapi_connection):
        """
        Handle transaction commit.

        SAS operates in auto-commit mode and does not support transactions,
        so this is a no-op.

        Args:
            dbapi_connection: JDBC connection object
        """
        # SAS doesn't support transactions - no-op
        pass

    def get_schema_names(self, connection, **kw):
        """
        Retrieve list of schema (library) names from SAS with fallback mechanisms.

        Queries the DICTIONARY.LIBNAMES table to get all accessible libraries.
        Falls back to empty list if query fails to prevent inspect command failures.

        Args:
            connection: Database connection
            **kw: Additional keyword arguments

        Returns:
            List of schema names (library names)
        """
        try:
            query = """
            SELECT DISTINCT libname
            FROM dictionary.libnames
            WHERE libname IS NOT NULL
            ORDER BY libname
            """

            result = connection.execute(query)
            return [row[0].strip() for row in result]
        except Exception as e:
            # Log error and return empty list as fallback
            logger.error(f"Failed to retrieve schema names: {e}. Returning empty list.")
            return []

    def get_table_names(self, connection, schema=None, **kw):
        """
        Retrieve list of table names from a schema with fallback mechanisms.

        Queries DICTIONARY.TABLES filtering for MEMTYPE='DATA'.
        Falls back gracefully if schema introspection fails.

        Args:
            connection: Database connection
            schema: Schema (library) name, defaults to default schema
            **kw: Additional keyword arguments

        Returns:
            List of table names
        """
        try:
            if schema is None:
                schema = self.default_schema_name
                logger.debug(f"get_table_names: using default_schema_name='{schema}'")

            logger.debug(f"get_table_names: querying schema='{schema}'")

            query = """
            SELECT memname
            FROM dictionary.tables
            WHERE libname = ? AND memtype = 'DATA'
            ORDER BY memname
            """

            result = connection.execute(query, (schema.upper() if schema else None,))
            # Strip trailing spaces from table names (common in SAS databases)
            table_names = [row[0].strip() for row in result]
            logger.debug(f"get_table_names: found {len(table_names)} tables in schema '{schema}': {table_names[:5]}...")
            return table_names
        except Exception as e:
            # Log error and return empty list as fallback
            logger.error(f"Failed to retrieve table names for schema {schema}: {e}. Returning empty list.")
            logger.debug(f"Exception type: {type(e).__name__}, Exception message: {str(e)}")
            return []

    def get_view_names(self, connection, schema=None, **kw):
        """
        Retrieve list of view names from a schema with fallback mechanisms.

        Queries DICTIONARY.TABLES filtering for MEMTYPE='VIEW'.
        Falls back gracefully if view introspection fails.

        Args:
            connection: Database connection
            schema: Schema (library) name, defaults to default schema
            **kw: Additional keyword arguments

        Returns:
            List of view names
        """
        try:
            if schema is None:
                schema = self.default_schema_name

            query = """
            SELECT memname
            FROM dictionary.tables
            WHERE libname = ? AND memtype = 'VIEW'
            ORDER BY memname
            """

            result = connection.execute(query, (schema.upper() if schema else None,))
            return [row[0].strip() for row in result]
        except Exception as e:
            # Log error and return empty list as fallback
            logger.error(f"Failed to retrieve view names for schema {schema}: {e}. Returning empty list.")
            return []

    def _safe_value_to_str(self, value):
        """
        Safely convert SAS values to strings, handling both strings and Java numeric types.

        When applyFormats is "false", SAS returns numeric values as java.lang.Double
        instead of formatted strings. This helper handles both cases.

        Args:
            value: Value from SAS (can be string, Java Double, or other numeric types)

        Returns:
            String representation of the value, or None if value is None
        """
        if value is None:
            return None

        # If it's already a string, strip it
        if isinstance(value, str):
            return value.strip()

        # For numeric types (including Java types), convert to string
        # This handles java.lang.Double, java.lang.Integer, etc.
        return str(value)

    def _parse_sas_format(self, format_str):
        """
        Parse a SAS format string to extract format name, width, and decimals.

        Examples:
            "COMMA12.2" -> {"format": "COMMA", "width": 12, "decimals": 2}
            "DATE9." -> {"format": "DATE", "width": 9, "decimals": None}
            "DATETIME20." -> {"format": "DATETIME", "width": 20, "decimals": None}

        Args:
            format_str: SAS format string (e.g., "COMMA12.2", "DATE9.")

        Returns:
            Dictionary with format, width, and decimals keys
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

    def _process_sas_numeric_value(self, value):
        """
        Process SAS numeric values, handling missing values and special cases.

        SAS uses "." and ".A" through ".Z" for missing numeric values.
        Also handles Java NaN and Infinity values from JDBC.

        Args:
            value: The numeric value from SAS

        Returns:
            The numeric value or None if it represents a missing value
        """
        if value is None:
            return None

        try:
            # Convert to float for checking
            float_val = float(value)

            # Check for NaN (Not a Number)
            if float_val != float_val:  # NaN check
                logger.debug("SAS missing value detected: NaN")
                return None

            # Check for Infinity
            if float_val == float("inf") or float_val == float("-inf"):
                logger.debug("SAS invalid value detected: Infinity")
                return None

            # SAS missing values are typically represented as very large negative numbers
            # Standard SAS missing value "." is approximately -1.797693e+308
            if float_val < -1e10:
                logger.debug(f"SAS missing value detected: {float_val}")
                return None

            return value

        except (ValueError, TypeError, OverflowError) as e:
            logger.debug(f"Invalid numeric value: {value} - {e}")
            return None

    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        Retrieve column metadata for a table with fallback mechanisms.

        Queries DICTIONARY.COLUMNS to get column definitions including:
        - Column name
        - Data type
        - Length
        - Format
        - Label
        - Nullable status

        Falls back gracefully if column introspection fails.

        Args:
            connection: Database connection
            table_name: Name of the table
            schema: Schema (library) name, defaults to default schema
            **kw: Additional keyword arguments

        Returns:
            List of column dictionaries with metadata
        """
        try:
            if schema is None:
                schema = self.default_schema_name

            query = """
            SELECT
                name,
                type,
                length,
                format,
                label,
                notnull
            FROM dictionary.columns
            WHERE libname = ? AND memname = ?
            ORDER BY varnum
            """

            result = connection.execute(query, (schema.upper() if schema else None, table_name.upper()))

            columns = []
            for row in result:
                col_name = self._safe_value_to_str(row[0])
                col_type = self._safe_value_to_str(row[1])  # 'num' or 'char'
                col_length = self._safe_value_to_str(row[2])
                col_format = self._safe_value_to_str(row[3])
                col_label = self._safe_value_to_str(row[4])
                # notnull can be numeric (0 or 1), convert to bool directly
                col_notnull = bool(row[5]) if row[5] is not None else False

                # Map SAS type to SQLAlchemy type
                sa_type = self._map_sas_type_to_sqlalchemy(col_type, col_length, col_format)

                column_info = {
                    "name": col_name,
                    "type": sa_type,
                    "nullable": not bool(col_notnull),
                    "default": None,
                }

                if col_label:
                    column_info["comment"] = col_label

                columns.append(column_info)

            return columns
        except Exception as e:
            # Log error and return empty list as fallback
            logger.error(f"Failed to retrieve columns for table {table_name}: {e}. Returning empty list.")
            return []

    def _map_sas_type_to_sqlalchemy(self, sas_type, length, format_str):
        """
        Map SAS data types to SQLAlchemy types with comprehensive format support.

        SAS has two basic types (numeric and character) but uses formats
        to indicate specialized types like dates, times, booleans, and formatted numbers.

        This method handles 50+ SAS formats including date/time, numeric, and special formats.

        Args:
            sas_type: SAS type ('num' or 'char')
            length: Column length (can be string representation of int or float)
            format_str: SAS format string (e.g., 'DATE9.', 'DATETIME20.', 'COMMA12.2')

        Returns:
            SQLAlchemy type instance
        """
        try:
            # Check cache first for performance
            cache_key = f"{sas_type}_{format_str}"
            if cache_key in self._type_mapping_cache:
                return self._type_mapping_cache[cache_key]

            # Handle character types
            if sas_type.lower() == "char":
                # Length might be a float string like '50.0', convert via float first
                sa_type = SASStringType(length=int(float(length)))
                self._type_mapping_cache[cache_key] = sa_type
                return sa_type

            # Numeric type - determine specific type based on format
            if format_str:
                # Parse the format string for detailed analysis
                format_info = self._parse_sas_format(format_str)
                format_name = format_info["format"]
                decimals = format_info["decimals"]

                if format_name:
                    # ISO 8601 formats (E8601*)
                    if format_name.startswith("E8601DA"):
                        # E8601DA* = Date format (ISO 8601 Date)
                        sa_type = SASDateType()
                        self._type_mapping_cache[cache_key] = sa_type
                        return sa_type

                    if format_name.startswith("E8601DT"):
                        # E8601DT* = DateTime format (ISO 8601 DateTime)
                        sa_type = SASDateTimeType()
                        self._type_mapping_cache[cache_key] = sa_type
                        return sa_type

                    if format_name.startswith("E8601TM"):
                        # E8601TM* = Time format (ISO 8601 Time)
                        sa_type = SASTimeType()
                        self._type_mapping_cache[cache_key] = sa_type
                        return sa_type

                    # DateTime formats (must check before DATE)
                    if format_name.startswith("DATETIME") or format_name == "DTYYQC":
                        sa_type = SASDateTimeType()
                        self._type_mapping_cache[cache_key] = sa_type
                        return sa_type

                    # Timestamp formats
                    if format_name in ["TODSTAMP", "DTMONYY", "DTWKDATX", "DTYEAR", "DTYYQC"]:
                        sa_type = SASDateTimeType()
                        self._type_mapping_cache[cache_key] = sa_type
                        return sa_type

                    # Standard SAS date formats
                    date_formats = [
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
                    if any(format_name.startswith(fmt) for fmt in date_formats):
                        sa_type = SASDateType()
                        self._type_mapping_cache[cache_key] = sa_type
                        return sa_type

                    # Time formats
                    time_formats = ["TIME", "TIMEAMPM", "TOD", "HHMM", "HOUR", "MMSS", "NLTIME", "NLTIMAP", "STIMER"]
                    if any(format_name.startswith(fmt) for fmt in time_formats):
                        sa_type = SASTimeType()
                        self._type_mapping_cache[cache_key] = sa_type
                        return sa_type

                    # Boolean formats
                    boolean_formats = ["YESNO", "YN", "BOOLEAN"]
                    if format_name in boolean_formats:
                        sa_type = sqltypes.BOOLEAN()
                        self._type_mapping_cache[cache_key] = sa_type
                        return sa_type

                    # Numeric formats with potential decimal places
                    numeric_formats = [
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
                    if any(format_name.startswith(fmt) for fmt in numeric_formats):
                        # Check if format has decimal places
                        if decimals is not None and decimals > 0:
                            sa_type = sqltypes.NUMERIC(precision=format_info.get("width"), scale=decimals)
                        else:
                            # Could be integer or float depending on usage
                            sa_type = sqltypes.NUMERIC()
                        self._type_mapping_cache[cache_key] = sa_type
                        return sa_type

                    # Explicit integer formats (no decimals)
                    integer_formats = [
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
                    if any(format_name.startswith(fmt) for fmt in integer_formats):
                        sa_type = sqltypes.INTEGER()
                        self._type_mapping_cache[cache_key] = sa_type
                        return sa_type

                    # Standard numeric format 'F' or 'NUMX' - check for decimals
                    if format_name in ["F", "NUMX", "NUMERIC"]:
                        if decimals is not None and decimals > 0:
                            sa_type = sqltypes.NUMERIC(precision=format_info.get("width"), scale=decimals)
                        else:
                            sa_type = sqltypes.INTEGER()
                        self._type_mapping_cache[cache_key] = sa_type
                        return sa_type

                    # Log unrecognized format for debugging
                    logger.debug(f"Unrecognized SAS format: {format_str}, using default NUMERIC type")

            # Default numeric type for unformatted or unrecognized formats
            sa_type = sqltypes.NUMERIC()
            self._type_mapping_cache[cache_key] = sa_type
            return sa_type

        except Exception as e:
            # Comprehensive error handling - log and return safe default
            logger.warning(
                f"Error mapping SAS type to SQLAlchemy: sas_type={sas_type}, "
                f"length={length}, format={format_str}, error={e}"
            )
            # Return safe default type based on sas_type
            if sas_type.lower() == "char":
                try:
                    return SASStringType(length=int(float(length)))
                except (ValueError, TypeError):
                    return SASStringType(length=255)  # Safe default length
            else:
                return sqltypes.NUMERIC()  # Safe default for numeric types

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """
        Retrieve primary key constraint information.

        SAS does not support primary key constraints, so this always
        returns an empty constraint.

        Args:
            connection: Database connection
            table_name: Name of the table
            schema: Schema (library) name
            **kw: Additional keyword arguments

        Returns:
            Dictionary with empty constrained_columns list
        """
        # SAS doesn't support primary keys
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Retrieve foreign key constraint information.

        SAS does not support foreign key constraints, so this always
        returns an empty list.

        Args:
            connection: Database connection
            table_name: Name of the table
            schema: Schema (library) name
            **kw: Additional keyword arguments

        Returns:
            Empty list (no foreign keys)
        """
        # SAS doesn't support foreign keys
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Retrieve index information for a table with graceful error handling.

        Queries DICTIONARY.INDEXES to get index definitions.
        Falls back gracefully if index introspection fails.

        Args:
            connection: Database connection
            table_name: Name of the table
            schema: Schema (library) name
            **kw: Additional keyword arguments

        Returns:
            List of index dictionaries with metadata
        """
        try:
            if schema is None:
                schema = self.default_schema_name

            query = """
            SELECT
                indxname,
                name,
                unique
            FROM dictionary.indexes
            WHERE libname = ? AND memname = ?
            ORDER BY indxname, indxpos
            """

            result = connection.execute(query, (schema.upper() if schema else None, table_name.upper()))

            # Group columns by index name
            indexes = {}
            for row in result:
                idx_name = self._safe_value_to_str(row[0])
                col_name = self._safe_value_to_str(row[1])
                # is_unique can be numeric (0 or 1) or string, handle both
                is_unique = bool(row[2]) if row[2] is not None else False

                if idx_name not in indexes:
                    indexes[idx_name] = {"name": idx_name, "column_names": [], "unique": is_unique}

                indexes[idx_name]["column_names"].append(col_name)

            return list(indexes.values())
        except Exception as e:
            # Log error and return empty list as fallback
            logger.error(f"Failed to retrieve indexes for table {table_name}: {e}. Returning empty list.")
            return []

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        """
        Retrieve table comment (label) with graceful error handling.

        Queries DICTIONARY.TABLES for the table label.
        Falls back gracefully if comment retrieval fails.

        Args:
            connection: Database connection
            table_name: Name of the table
            schema: Schema (library) name
            **kw: Additional keyword arguments

        Returns:
            Dictionary with 'text' key containing the comment
        """
        try:
            if schema is None:
                schema = self.default_schema_name

            query = """
            SELECT memlabel
            FROM dictionary.tables
            WHERE libname = ? AND memname = ?
            """

            result = connection.execute(query, (schema.upper() if schema else None, table_name.upper()))

            row = result.fetchone()
            if row and row[0]:
                return {"text": row[0].strip()}

            return {"text": None}
        except Exception as e:
            # Log error and return None as fallback
            logger.error(f"Failed to retrieve table comment for {table_name}: {e}. Returning None.")
            return {"text": None}

    def has_table(self, connection, table_name, schema=None):
        """
        Check if a table exists in the schema with graceful error handling.

        Args:
            connection: Database connection
            table_name: Name of the table
            schema: Schema (library) name

        Returns:
            True if table exists, False otherwise (including on errors)
        """
        try:
            if schema is None:
                schema = self.default_schema_name

            query = """
            SELECT COUNT(*)
            FROM dictionary.tables
            WHERE libname = ? AND memname = ? AND memtype = 'DATA'
            """

            result = connection.execute(query, (schema.upper() if schema else None, table_name.upper()))

            row = result.fetchone()
            return int(row[0]) > 0 if row else False
        except Exception as e:
            # Log error and return False as fallback
            logger.error(f"Failed to check table existence for {table_name}: {e}. Returning False.")
            return False

    def has_sequence(self, connection, sequence_name, schema=None):
        """
        Check if a sequence exists.

        SAS does not support sequences, so this always returns False.

        Args:
            connection: Database connection
            sequence_name: Name of the sequence
            schema: Schema name

        Returns:
            False (sequences not supported)
        """
        # SAS doesn't support sequences
        return False

    def normalize_name(self, name):
        """
        Normalize identifier names for SAS.

        Converts to uppercase as SAS identifiers are case-insensitive
        and stored in uppercase. Also strips trailing spaces as SAS
        does not support quoted identifiers.

        Args:
            name: Identifier name

        Returns:
            Normalized name in uppercase with trailing spaces stripped
        """
        if name:
            return name.upper().rstrip()
        return name

    def denormalize_name(self, name):
        """
        Denormalize identifier names from SAS.

        Returns the name in lowercase for more conventional display.

        Args:
            name: Normalized name

        Returns:
            Denormalized name in lowercase
        """
        if name:
            return name.lower()
        return name


def register_sas_dialect():
    """
    Register the SAS dialect with SQLAlchemy.

    This function should be called to make the dialect available
    for use with SQLAlchemy engine creation.

    Example:
        from spinta.datasets.backends.sql.backends.sas.dialect import register_sas_dialect
        register_sas_dialect()

        engine = create_engine('sas+jdbc://host:port', ...)
    """
    from sqlalchemy.dialects import registry

    registry.register("sas.jdbc", "spinta.datasets.backends.sql.backends.sas.dialect", "SASDialect")
