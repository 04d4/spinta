"""
SAS Backend Component

This module provides the main backend component class for SAS database integration
within the Spinta framework.

The SAS backend extends the generic SQL backend to provide SAS-specific functionality,
leveraging the custom SAS dialect implemented in dialect.py for SQLAlchemy integration.
"""

from spinta.datasets.backends.sql.components import Sql


class SAS(Sql):
    type = "sql/sas"
    query_builder_type = "sql/sas"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Extract schema from DSN URL if not already set
        if hasattr(self, "dsn") and self.dsn and not self.dbschema:
            from sqlalchemy.engine.url import make_url

            url = make_url(self.dsn)
            schema = url.query.get("schema")
            if schema:
                self.dbschema = schema

    # Note: The SAS dialect is registered at module import time in __init__.py
    # No need for __init__ override here
