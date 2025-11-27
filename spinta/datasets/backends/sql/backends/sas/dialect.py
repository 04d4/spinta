"""
SQLAlchemy Dialect for SAS Databases using JDBC.

This dialect provides connectivity to SAS databases through the SAS IOM JDBC driver,
enabling schema introspection and query execution against SAS libraries and datasets.

Configuration:
    - jdbc_db_name: "sasiom" (required for JDBC URL construction)
    - jdbc_driver_name: "com.sas.rio.MVADriver"

Example connection URL:
    sas+jdbc://user:pass@host:port/?schema=libname
"""

import logging
from pathlib import Path

from sqlalchemy import pool, types as sqltypes
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.schema import Table

from spinta.datasets.backends.sql.backends.sas.base import BaseDialect
from spinta.datasets.backends.sql.backends.sas.identifier import SASIdentifierPreparer
from spinta.datasets.backends.sql.backends.sas.introspection import SASIntrospectionMixin
from spinta.datasets.backends.sql.backends.sas.types import (
    SASDateType,
    SASDateTimeType,
    SASTimeType,
    SASStringType,
)

logger = logging.getLogger(__name__)


class SASCompiler(SQLCompiler):
    """
    Custom SQL compiler for SAS dialect.

    Ensures that table names are always qualified with the schema (library name)
    to prevent SAS from defaulting to the WORK library.
    """

    def visit_table(self, table: Table, asfrom=False, **kw):
        """
        Visit a Table object and compile its name, ensuring schema qualification.

        Args:
            table: The SQLAlchemy Table object.
            asfrom: Boolean indicating if the table is in a FROM clause.
            **kw: Additional keyword arguments.

        Returns:
            The compiled table name with schema.
        """
        # CRITICAL FIX: If table.schema is None but we have a default_schema_name, set it
        # This ensures SAS tables are always qualified with the library name
        if asfrom and not table.schema and hasattr(self.dialect, "default_schema_name"):
            schema = self.dialect.default_schema_name
            if schema:
                table.schema = schema

        if asfrom and table.schema:
            # Ensure schema is always included for tables in FROM clauses
            # Use self.preparer which is the IdentifierPreparer instance
            return self.preparer.format_table(table)

        return super().visit_table(table, asfrom=asfrom, **kw)


class SASDialect(SASIntrospectionMixin, BaseDialect, DefaultDialect):
    """
    SQLAlchemy dialect for SAS databases using JDBC.

    This dialect provides connectivity to SAS databases through the SAS IOM JDBC driver,
    enabling schema introspection and query execution against SAS libraries and datasets.

    The dialect inherits from:
    - SASIntrospectionMixin: Schema introspection methods (get_table_names, get_columns, etc.)
    - BaseDialect: JDBC-specific functionality (dbapi, is_disconnect, etc.)
    - DefaultDialect: SQLAlchemy's default dialect implementation
    """

    # Dialect identification
    name = "sas"
    driver = "jdbc"  # Required by SQLAlchemy for sas+jdbc:// URLs
    jdbc_db_name = "sasiom"
    jdbc_driver_name = "com.sas.rio.MVADriver"

    # SAS identifier limitation (32 characters max)
    max_identifier_length = 32

    # Custom compiler for SAS
    statement_compiler = SASCompiler

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

    # SAS doesn't support PK auto-increment or sequences
    supports_pk_autoincrement = False
    supports_sequences = False

    # SAS does not use quoted identifiers - disable quoting
    quote_identifiers = False

    # Enable statement caching for performance
    supports_statement_cache = True

    # Type colspecs for result processing
    colspecs = {
        sqltypes.Date: SASDateType,
        sqltypes.DateTime: SASDateTimeType,
        sqltypes.Time: SASTimeType,
        sqltypes.String: SASStringType,
        sqltypes.VARCHAR: SASStringType,
    }

    @classmethod
    def get_dialect_pool_class(cls, url):
        """
        Return the connection pool class to use.

        Uses QueuePool for connection pooling with SAS databases.

        Args:
            url: SQLAlchemy URL object

        Returns:
            QueuePool class
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

        Sets up the identifier preparer and type mapping cache.
        """
        super().__init__(**kwargs)

        # Override the identifier preparer with our custom SAS version
        self.identifier_preparer = SASIdentifierPreparer(self)

        # Initialize type mapping cache for performance optimization
        self._type_mapping_cache = {}

    def on_connect_url(self, url):
        """
        Store the URL for later use during initialization.

        Args:
            url: SQLAlchemy URL object

        Returns:
            None (no special initialization callback needed)
        """
        self.url = url
        return None

    def initialize(self, connection):
        """
        Initialize dialect with connection-specific settings and fallback mechanisms.

        Extracts the default schema name from the URL query parameters if available.

        Args:
            connection: Database connection object
        """
        try:
            # Call parent initialize if it exists
            if hasattr(super(SASDialect, self), "initialize"):
                super(SASDialect, self).initialize(connection)

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
        Parse the SQLAlchemy URL and create JDBC connection arguments.

        The SAS JDBC URL format is:
            jdbc:sasiom://host:port/?schema=libname

        Args:
            url: SQLAlchemy URL object

        Returns:
            Tuple of (args, kwargs) for JDBC connection compatible with jaydebeapi
        """
        logger.debug(f"Creating connection args for URL: {url}")

        try:
            # Build JDBC URL
            jdbc_url = f"jdbc:{self.jdbc_db_name}://{url.host}"

            if url.port:
                jdbc_url += f":{url.port}"

            # Add query parameters to JDBC URL if present
            if url.query:
                from urllib.parse import urlencode

                query_string = urlencode(url.query)
                jdbc_url += f"?{query_string}"

            logger.debug(f"Built JDBC URL: {jdbc_url}")

            # Base driver arguments
            # IMPORTANT: All driver_args values MUST be strings for java.util.Properties
            # jaydebeapi converts these to Java Properties which only accepts String values
            driver_args = {
                "user": url.username or "",
                "password": url.password or "",
                "applyFormats": "false",
            }

            # Add schema to driver_args if present in query
            if url.query:
                schema = url.query.get("schema")
                if schema:
                    driver_args["schema"] = schema
                    logger.debug(f"Added schema '{schema}' to driver_args")

            # Log driver_args with types for debugging
            logger.debug(f"Driver args with types: {[(k, type(v).__name__, v) for k, v in driver_args.items()]}")

            # Add log4j configuration to suppress warnings
            current_dir = Path(__file__).parent
            log4j_config_path = current_dir / "log4j.properties"

            if log4j_config_path.exists():
                # Add log4j configuration as a system property
                driver_args["log4j.configuration"] = f"file://{log4j_config_path.absolute()}"
                jars = [str(current_dir)]
            else:
                jars = []

            # Add query parameters if present
            if url.query:
                logger.debug(f"Query parameters: {dict(url.query)}")

            # jaydebeapi expects: connect(jclassname, url, driver_args, jars, libs)
            kwargs = {
                "jclassname": self.jdbc_driver_name,
                "url": jdbc_url,
                "driver_args": driver_args,
            }

            # Add jars parameter if log4j configuration directory was found
            if jars:
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
