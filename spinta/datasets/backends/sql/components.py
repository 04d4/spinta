import contextlib

import sqlalchemy as sa
from sqlalchemy.engine.base import Engine

from spinta import commands
from spinta.backends.constants import BackendFeatures
from spinta.components import Model
from spinta.components import Property
from spinta.datasets.components import ExternalBackend
from spinta.exceptions import BackendUnavailable


class Sql(ExternalBackend):
    type: str = "sql"
    engine: Engine = None
    schema: sa.MetaData = None
    dbschema: str = None  # Database schema name

    features = {BackendFeatures.PAGINATION}

    query_builder_type = "sql"
    result_builder_type = "sql"

    @contextlib.contextmanager
    def transaction(self, write=False):
        raise NotImplementedError

    @contextlib.contextmanager
    def begin(self):
        try:
            with self.engine.begin() as conn:
                yield conn
        except sa.exc.OperationalError:
            self.available = False
            raise BackendUnavailable(self)

    def get_table(self, model: Model, name: str = None) -> sa.Table:
        import logging

        logger = logging.getLogger(__name__)

        name = name or model.external.name

        # For SAS dialect, use dialect's default_schema_name if backend's dbschema is None
        effective_schema = self.dbschema
        is_sas = self.engine.dialect.name == "sas"

        if is_sas and not effective_schema and hasattr(self.engine.dialect, "default_schema_name"):
            effective_schema = self.engine.dialect.default_schema_name
            logger.debug(f"Using dialect's default_schema_name: '{effective_schema}'")

        if effective_schema:
            key = f"{effective_schema}.{name}"
        else:
            key = name

        if key not in self.schema.tables:
            if effective_schema and is_sas:
                logger.debug(f"Creating SAS table '{name}' with schema='{effective_schema}'")
                sa.Table(name, self.schema, autoload_with=self.engine, schema=effective_schema)
            else:
                logger.debug(f"Creating table '{name}' without explicit schema")
                sa.Table(name, self.schema, autoload_with=self.engine)

        return self.schema.tables[key]

    def get_column(self, table: sa.Table, prop: Property, *, select=False, **kwargs) -> sa.Column:
        column = commands.get_column(self, prop, table=table, **kwargs)
        return column
