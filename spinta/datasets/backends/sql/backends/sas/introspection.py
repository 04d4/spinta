"""
SAS Schema Introspection for SQLAlchemy.

This module provides schema introspection methods for SAS databases.
These methods query SAS DICTIONARY tables to retrieve metadata about:
- Schema (library) names
- Table names
- View names
- Column metadata
- Index information
- Table comments

The SASIntrospectionMixin class is designed to be mixed into the main
SASDialect class to provide these introspection capabilities.
"""

import logging

from spinta.datasets.backends.sql.backends.sas.formats import map_sas_type_to_sqlalchemy
from spinta.datasets.backends.sql.backends.sas.constants import is_sas_missing_value

logger = logging.getLogger(__name__)


class SASIntrospectionMixin:
    """
    Mixin class providing SAS schema introspection methods.

    These methods query SAS DICTIONARY tables to retrieve metadata
    about libraries, tables, columns, and indexes.

    All methods include graceful error handling that returns empty
    results rather than raising exceptions, ensuring that SQLAlchemy
    inspection commands work even with partial database access.

    Attributes:
        default_schema_name: The default schema (library) name
        _type_mapping_cache: Cache for type mapping performance
    """

    # These attributes are expected to be set by the main dialect class
    default_schema_name: str = ""
    _type_mapping_cache: dict

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
        if is_sas_missing_value(value):
            return None

        return value

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
                sa_type = map_sas_type_to_sqlalchemy(col_type, col_length, col_format, self._type_mapping_cache)

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

    def has_table(self, connection, table_name, schema=None, **kw):
        """
        Check if a table exists in the schema with graceful error handling.

        Args:
            connection: Database connection
            table_name: Name of the table
            schema: Schema (library) name
            **kw: Additional keyword arguments

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

    def has_sequence(self, connection, sequence_name, schema=None, **kw):
        """
        Check if a sequence exists.

        SAS does not support sequences, so this always returns False.

        Args:
            connection: Database connection
            sequence_name: Name of the sequence
            schema: Schema name
            **kw: Additional keyword arguments

        Returns:
            False (sequences not supported)
        """
        # SAS doesn't support sequences
        return False
