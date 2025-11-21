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
    jdbc_db_name = "sasiom"
    jdbc_driver_name = "com.sas.rio.MVADriver"

    # Feature support flags
    supports_comments = True
    supports_schemas = True
    supports_views = True
    # Transaction handling (SAS operates in auto-commit mode)
    supports_transactions = False

    def __init__(self, **kwargs):
        """
        Initialize the SAS dialect.

        Calls DefaultDialect.__init__ to set up all SQLAlchemy infrastructure.
        BaseDialect has no __init__, so super() correctly resolves to DefaultDialect.
        """
        # This calls DefaultDialect.__init__(**kwargs)
        super().__init__(**kwargs)

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
        self.default_schema_name = ""

    def create_connect_args(self, url):
        # Build JDBC URL
        jdbc_url = f"jdbc:{self.jdbc_db_name}://{url.host}"

        if url.port:
            jdbc_url += f":{url.port}"

        kwargs = {
            "jclassname": self.jdbc_driver_name,
            "url": jdbc_url,
            "driver_args": [url.username or "", url.password or ""],
        }

        if url.query:
            pass

        return ((), kwargs)

    def do_rollback(self, dbapi_connection):
        # SAS doesn't support transactions - no-op
        pass

    def do_commit(self, dbapi_connection):
        # SAS operates in auto-commit mode and does not support transactions
        pass

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
            return sqltypes.VARCHAR(length=int(length))

        # Numeric type - check format for specialized handling
        if format_str:
            format_upper = format_str.upper()

            # Date formats
            if any(fmt in format_upper for fmt in ["DATE", "DDMMYY", "MMDDYY", "YYMMDD"]):
                return sqltypes.DATE()

            # DateTime formats
            if "DATETIME" in format_upper:
                return sqltypes.DATETIME()

            # Time formats
            if "TIME" in format_upper:
                return sqltypes.TIME()

            # Check for integer formats
            if any(fmt in format_upper for fmt in ["Z", "F", "COMMA", "DOLLAR"]):
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
            idxname,
            name,
            unique
        FROM dictionary.indexes
        WHERE libname = ? AND memname = ?
        ORDER BY idxname, indxpos
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
        return row[0] > 0 if row else False

    def has_sequence(self, connection, sequence_name, schema=None):
        # SAS doesn't support sequences
        return False

    def normalize_name(self, name):
        if name:
            return name.upper()
        return name

    def denormalize_name(self, name):
        if name:
            return name.lower()
        return name


def register_sas_dialect():
    from sqlalchemy.dialects import registry

    registry.register("sas.jdbc", "spinta.datasets.backends.sql.backends.sas.dialect", "SASDialect")
