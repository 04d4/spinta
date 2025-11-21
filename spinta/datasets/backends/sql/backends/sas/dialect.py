"""
SAS JDBC SQLAlchemy Dialect

This module implements a SQLAlchemy dialect for SAS databases using JDBC connectivity.
It extends the sqlalchemy-jdbcapi BaseDialect to provide SAS-specific functionality.

Features:
- Schema introspection via SAS DICTIONARY tables
- Type mapping between SAS and SQLAlchemy types
- Table, view, column, and index reflection
- SAS-specific format handling (DATE, DATETIME, TIME)

Limitations:
- No primary key or foreign key support (SAS limitation)
- No transaction support (auto-commit mode)
- Limited DDL operations
"""

from sqlalchemy_jdbcapi.base import BaseDialect
from sqlalchemy import types as sqltypes
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.compiler import IdentifierPreparer


class SASStringType(sqltypes.VARCHAR):
    """
    Custom string type for SAS that strips trailing spaces from VARCHAR data.

    SAS often pads character fields with spaces, so this type ensures
    that returned string values have trailing spaces removed.
    """

    def process_result_value(self, value, dialect):
        """
        Process the result value by stripping trailing spaces.

        Args:
            value: The raw value from the database
            dialect: The dialect instance

        Returns:
            The processed value with trailing spaces stripped
        """
        if value is not None:
            return value.rstrip()
        return value


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


class SASCursorWrapper:
    """
    Cursor wrapper that provides a modified description property.

    This wrapper strips trailing spaces from column names in the cursor.description
    tuple, as SAS often pads character fields with spaces.
    """

    def __init__(self, cursor):
        """
        Initialize the cursor wrapper.

        Args:
            cursor: The original database cursor
        """
        self._cursor = cursor
        # Store original description to avoid recursion when we change the class
        self._original_description = getattr(cursor, "description", None)

    @property
    def description(self):
        """
        Return the cursor description with trailing spaces stripped from column names.

        Returns:
            Tuple of column descriptions with modified names
        """
        desc = self._original_description
        if desc:
            return tuple((col[0].rstrip() if col[0] else col[0],) + col[1:] for col in desc)
        return desc

    def __getattr__(self, name):
        """
        Delegate all other attributes and methods to the wrapped cursor.

        Args:
            name: Attribute or method name

        Returns:
            The attribute or method from the wrapped cursor
        """
        if name == "description":
            # Avoid recursion by returning our property
            return self.description
        return getattr(self._cursor, name)


class SASDialect(BaseDialect, DefaultDialect):
    """
    SQLAlchemy dialect for SAS databases using JDBC.

    This dialect provides connectivity to SAS databases through the SAS IOM JDBC driver,
    enabling schema introspection and query execution against SAS libraries and datasets.

    Configuration:
        - jdbc_db_name: "sasiom" (required for JDBC URL construction)
        - jdbc_driver_name: "com.sas.rio.MVADriver"
        - max_identifier_length: 32 (SAS limitation)

    Example connection URL:
        jdbc:sasiom://host:port/
    """

    # Dialect identification
    name = "sas"
    driver = "jdbc"  # Required by SQLAlchemy for sas+jdbc:// URLs
    jdbc_db_name = "sasiom"
    jdbc_driver_name = "com.sas.rio.MVADriver"

    # Feature support flags
    supports_comments = True
    supports_schemas = True
    supports_views = True
    # Transaction handling (SAS operates in auto-commit mode)
    supports_transactions = False
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False

    # Identifier limits
    max_identifier_length = 32
    max_index_name_length = 32

    # SAS does not use quoted identifiers - disable quoting
    quote_identifiers = False

    # Type specifications
    colspecs = {
        sqltypes.Date: sqltypes.DATE,
        sqltypes.DateTime: sqltypes.DATETIME,
        sqltypes.Time: sqltypes.TIME,
    }

    # @classmethod
    # def dbapi(cls):
    #     """Return the jaydebeapi module for JDBC connections."""
    #     import jaydebeapi

    #     return jaydebeapi

    @classmethod
    def get_dialect_pool_class(cls, url):
        """
        Return the connection pool class to use.

        This method is required by SQLAlchemy's engine creation.
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
        Initialize the SAS dialect.

        Calls DefaultDialect.__init__ to set up all SQLAlchemy infrastructure.
        BaseDialect has no __init__, so super() correctly resolves to DefaultDialect.
        """
        # This calls DefaultDialect.__init__(**kwargs)
        super().__init__(**kwargs)

        # Override the identifier preparer with our custom SAS version
        self.identifier_preparer = SASIdentifierPreparer(self)

        # SAS-specific initialization if needed
        # self.default_schema_name will be set by initialize()

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
        Initialize dialect with connection-specific settings.

        Args:
            connection: Database connection object
        """
        # BaseDialect may or may not have initialize method depending on version
        # Only call parent if it exists
        if hasattr(super(SASDialect, self), "initialize"):
            super(SASDialect, self).initialize(connection)

        # SQLAlchemy will set attributes via other mechanisms
        # Extract schema from URL query parameters if available
        if hasattr(self, "url") and self.url and self.url.query:
            self.default_schema_name = self.url.query.get("schema")
        else:
            self.default_schema_name = ""

    def create_connect_args(self, url):
        # Build JDBC URL
        jdbc_url = f"jdbc:{self.jdbc_db_name}://{url.host}"

        if url.port:
            jdbc_url += f":{url.port}"

        # Base driver arguments
        driver_args = {"user": url.username or "", "password": url.password or "", "applyFormats": "true"}

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
            jars = None

        # Add query parameters if present
        if url.query:
            # Query parameters could be added to driver_args if needed
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

        return ((jdbc_url,), kwargs)

    def do_rollback(self, dbapi_connection):
        # SAS doesn't support transactions - no-op
        pass

    def do_commit(self, dbapi_connection):
        # SAS operates in auto-commit mode and does not support transactions
        pass

    # TODO(oa): ar reikia šito?
    def do_execute(self, cursor, statement, parameters, context=None):
        """
        Execute a SQL statement and modify the cursor to strip trailing spaces from column names.

        Overrides the default execute method to dynamically change the cursor's class
        to SASCursorWrapper, which provides a modified description property that strips
        trailing spaces from column names.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute
            parameters: Query parameters
            context: Execution context
        """
        # Call parent execute method
        super().do_execute(cursor, statement, parameters, context)

        # Dynamically change the cursor's class to our wrapper
        # This preserves the cursor object identity while adding our description property
        original_desc = cursor.description
        cursor.__class__ = SASCursorWrapper
        cursor._cursor = cursor
        cursor._original_description = original_desc

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        query = """
            SELECT DISTINCT libname
            FROM dictionary.libnames
            WHERE libname IS NOT NULL
            ORDER BY libname
            """
        result = connection.execute(query)
        return [row[0] for row in result]

    def get_table_names(self, connection, schema=None, **kw):
        if schema is None:
            schema = self.default_schema_name

        query = """
            SELECT memname
            FROM dictionary.tables
            WHERE libname = ? AND memtype = 'DATA'
            ORDER BY memname
            """
        result = connection.execute(query, (schema.upper() if schema else None,))
        # Strip trailing spaces from table names (common in SAS databases)
        return [row[0].strip() for row in result]

    def get_view_names(self, connection, schema=None, **kw):
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

    def get_columns(self, connection, table_name, schema=None, **kw):
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
            col_name = row[0]
            col_type = row[1]  # 'num' or 'char'
            col_length = row[2]
            col_format = row[3]
            col_label = row[4]
            col_notnull = row[5]

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

    def _map_sas_type_to_sqlalchemy(self, sas_type, length, format_str):
        """
        Map SAS data types to SQLAlchemy types.

        SAS has two basic types (numeric and character) but uses formats
        to indicate specialized types like dates and times.

        Args:
            sas_type: SAS type ('num' or 'char')
            length: Column length
            format_str: SAS format string (e.g., 'DATE9.', 'DATETIME20.')

        Returns:
            SQLAlchemy type instance
        """
        if sas_type.lower() == "char":
            return SASStringType(length=int(length))

        # Numeric type - check format for specialized handling
        if format_str:
            format_upper = format_str.upper()

            # Check for E8601 format strings first (SAS ISO 8601 formats)
            if format_upper.startswith("E8601DA"):
                # E8601DA* = Date format (ISO 8601 Date)
                return sqltypes.DATE()

            if format_upper.startswith("E8601DT"):
                # E8601DT* = DateTime format (ISO 8601 DateTime)
                return sqltypes.DATETIME()

            # Standard SAS date formats
            if any(fmt in format_upper for fmt in ["DATE", "DDMMYY", "MMDDYY", "YYMMDD"]):
                return sqltypes.DATE()

            # DateTime formats - check for DATETIME specifically
            if format_upper.startswith("DATETIME"):
                return sqltypes.DATETIME()

            # Time formats
            if "TIME" in format_upper:
                return sqltypes.TIME()

            # Check for integer formats
            if any(fmt in format_upper for fmt in ["Z", "F", "COMMA", "DOLLAR", "NUMX"]):
                # Could be integer or decimal depending on format
                if "." in format_str:
                    # Has decimal specification
                    parts = format_str.split(".")
                    if len(parts) == 2 and parts[1].isdigit() and int(parts[1]) > 0:
                        # Has decimal places
                        return sqltypes.NUMERIC()
                    else:
                        return sqltypes.INTEGER()
                else:
                    return sqltypes.INTEGER()

        # Default numeric type
        return sqltypes.NUMERIC()

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # SAS doesn't support primary keys
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # SAS doesn't support foreign keys
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
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
            idx_name = row[0]
            col_name = row[1]
            is_unique = row[2]

            if idx_name not in indexes:
                indexes[idx_name] = {"name": idx_name, "column_names": [], "unique": bool(is_unique)}

            indexes[idx_name]["column_names"].append(col_name)

        return list(indexes.values())

    def get_table_comment(self, connection, table_name, schema=None, **kw):
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

    def has_table(self, connection, table_name, schema=None):
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

    def has_sequence(self, connection, sequence_name, schema=None):
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
            return name.strip().upper()
        return name

    def denormalize_name(self, name):
        if name:
            return name.lower()
        return name


def register_sas_dialect():
    from sqlalchemy.dialects import registry

    registry.register("sas.jdbc", "spinta.datasets.backends.sql.backends.sas.dialect", "SASDialect")
