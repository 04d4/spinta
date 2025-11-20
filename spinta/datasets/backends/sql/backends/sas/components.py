from spinta.datasets.backends.sql.components import Sql


class SAS(Sql):
    type = "sql/sas"
    query_builder_type = "sql/sas"
