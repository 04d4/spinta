"""
SAS Backend Component

This module provides the main backend component class for SAS database integration
within the Spinta framework.

The SAS backend extends the generic SQL backend to provide SAS-specific functionality,
leveraging the custom SAS dialect implemented in dialect.py for SQLAlchemy integration.
"""

import logging

import sqlalchemy as sa

from spinta.components import Model
from spinta.datasets.backends.sql.components import Sql

logger = logging.getLogger(__name__)


class SAS(Sql):
    """
    SAS Backend Component for Spinta framework.

    This backend provides connectivity to SAS databases through JDBC,
    enabling data access from SAS libraries and datasets.

    Attributes:
        type: Backend type identifier ("sql/sas")
        query_builder_type: Query builder type identifier ("sql/sas")
    """

    type = "sql/sas"
    query_builder_type = "sql/sas"

    def __init__(self, **kwargs):
        """
        Initialize the SAS backend.

        Extracts schema from the DSN URL if not already set.
        """
        super().__init__(**kwargs)
        # Extract schema from DSN URL if not already set
        if hasattr(self, "dsn") and self.dsn and not self.dbschema:
            from sqlalchemy.engine.url import make_url

            url = make_url(self.dsn)
            schema = url.query.get("schema")
            if schema:
                self.dbschema = schema

    def get_table(self, model: Model, name: str | None = None) -> sa.Table:
        """
        Get or create a SQLAlchemy Table object for a model.

        Overrides the base implementation to handle SAS-specific schema resolution
        from the dialect's default_schema_name.

        SAS requires special handling because:
        1. Schema names are called "libraries" in SAS terminology
        2. The schema may be specified in the URL query parameters
        3. The dialect stores the default schema name after initialization

        Args:
            model: The model to get the table for
            name: Optional table name override

        Returns:
            SQLAlchemy Table object
        """
        name = name or model.external.name

        # Use dialect's default_schema_name if backend's dbschema is not set
        effective_schema = self.dbschema
        if not effective_schema and hasattr(self.engine.dialect, "default_schema_name"):
            effective_schema = self.engine.dialect.default_schema_name
            logger.debug(f"Using dialect's default_schema_name: '{effective_schema}'")

        if effective_schema:
            key = f"{effective_schema}.{name}"
        else:
            key = name

        if key not in self.schema.tables:
            logger.debug(f"Creating SAS table '{name}' with schema='{effective_schema}'")
            sa.Table(name, self.schema, autoload_with=self.engine, schema=effective_schema)

        return self.schema.tables[key]
