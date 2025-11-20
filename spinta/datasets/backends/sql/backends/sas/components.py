from spinta.datasets.backends.sql.components import Sql
from spinta.datasets.backends.sql.backends.sas.dialect import register_sas_dialect


class SAS(Sql):
    """
    SAS database backend component.

    This backend component enables Spinta to connect to and interact with SAS databases
    through the SAS IOM JDBC driver. It extends the base SQL backend with SAS-specific
    configuration and initialization.

    Key Features:
    - Automatic registration of the SAS SQLAlchemy dialect on initialization
    - Support for SAS-specific data types (DATE, DATETIME, TIME)
    - Integration with SAS libraries (schemas) and datasets (tables)
    - Query building optimized for SAS SQL implementation

    Attributes:
        type: Backend type identifier for configuration ("sql/sas")
        query_builder_type: Query builder type for SAS-specific SQL generation ("sql/sas")

    Connection Configuration:
        The backend expects connection strings in the format:
            sas+jdbc://username:password@host:port/?schema=LIBNAME
        where LIBNAME is the SAS library (schema) to connect to.
    Possible limitations:
        - No support for database transactions (SAS auto-commit mode)
        - No primary key or foreign key constraints
        - Limited DDL operations support
        - Maximum identifier length of 32 characters
    """

    type = "sql/sas"
    query_builder_type = "sql/sas"

    def __init__(self, *args, **kwargs):
        register_sas_dialect()
        super().__init__(*args, **kwargs)
