from spinta.datasets.backends.sql.components import Sql


class SAS(Sql):
    type = "sql/sas"
    query_builder_type = "sql/sas"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Extract schema from DSN URL if not already set
        if hasattr(self, "engine") and self.engine and not self.dbschema:
            from sqlalchemy.engine.url import make_url

            url = make_url(str(self.engine.url))
            schema = url.query.get("schema")
            if schema:
                self.dbschema = schema

    # Note: The SAS dialect is registered at module import time in __init__.py
    # No need for __init__ override here
