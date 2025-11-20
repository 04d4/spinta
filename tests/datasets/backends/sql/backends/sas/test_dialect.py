from unittest.mock import Mock, patch
from sqlalchemy import types as sqltypes
from sqlalchemy.engine.url import make_url

from spinta.datasets.backends.sql.backends.sas.dialect import SASDialect, register_sas_dialect


class TestSASDialect:
    """Test suite for the SAS SQLAlchemy dialect"""

    def test_create_connect_args_with_schema(self):
        """Test URL parsing with schema parameter in query string."""
        url = make_url("sas+jdbc://testuser:testpass@localhost:8591/?schema=MYLIB")
        dialect = SASDialect()

        jdbc_url, props = dialect.create_connect_args(url)

        # Verify JDBC URL format
        assert jdbc_url == ["jdbc:sasiom://localhost:8591"]

        # Verify connection properties include schema
        assert props["user"] == "testuser"
        assert props["password"] == "testpass"
        assert props["schema"] == "MYLIB"

    def test_create_connect_args_no_port(self):
        """Test URL parsing when no port is specified."""
        url = make_url("sas+jdbc://testuser:testpass@localhost")
        dialect = SASDialect()

        jdbc_url, props = dialect.create_connect_args(url)

        # Verify JDBC URL format without port
        assert jdbc_url == ["jdbc:sasiom://localhost"]

    def test_create_connect_args_no_credentials(self):
        """Test URL parsing when credentials are not provided."""
        url = make_url("sas+jdbc://localhost:8591")
        dialect = SASDialect()

        jdbc_url, props = dialect.create_connect_args(url)

        # Verify empty credentials
        assert props["user"] == ""
        assert props["password"] == ""

    def test_type_mapping_char(self):
        """Test CHAR type mapping to VARCHAR."""
        dialect = SASDialect()

        # Test character type mapping
        sa_type = dialect._map_sas_type_to_sqlalchemy("char", 50, None)

        assert isinstance(sa_type, sqltypes.VARCHAR)
        assert sa_type.length == 50

    def test_type_mapping_num_date(self):
        """Test NUM with DATE format mapping to DATE type."""
        dialect = SASDialect()

        # Test with various date formats
        for date_format in ["DATE9.", "DDMMYY10.", "MMDDYY8.", "YYMMDD10."]:
            sa_type = dialect._map_sas_type_to_sqlalchemy("num", 8, date_format)
            assert isinstance(sa_type, sqltypes.DATE), f"Failed for format {date_format}"

    def test_type_mapping_num_datetime(self):
        """Test NUM with DATETIME format mapping to DATETIME type."""
        dialect = SASDialect()

        # Test with datetime formats
        for datetime_format in ["DATETIME20.", "DATETIME19.", "DATETIME."]:
            sa_type = dialect._map_sas_type_to_sqlalchemy("num", 8, datetime_format)
            assert isinstance(sa_type, sqltypes.DATETIME), f"Failed for format {datetime_format}"

    def test_type_mapping_num_time(self):
        """Test NUM with TIME format mapping to TIME type."""
        dialect = SASDialect()

        # Test with time formats
        for time_format in ["TIME8.", "TIME12.", "TIME."]:
            sa_type = dialect._map_sas_type_to_sqlalchemy("num", 8, time_format)
            assert isinstance(sa_type, sqltypes.TIME), f"Failed for format {time_format}"

    def test_type_mapping_num_default(self):
        """Test NUM without format mapping to NUMERIC type."""
        dialect = SASDialect()

        # Test numeric without specific format
        sa_type = dialect._map_sas_type_to_sqlalchemy("num", 8, None)
        assert isinstance(sa_type, sqltypes.NUMERIC)

    def test_type_mapping_num_with_integer_format(self):
        """Test NUM with integer-like formats."""
        dialect = SASDialect()

        # Test formats that indicate integers
        for int_format in ["Z8.", "F10.", "COMMA10.", "DOLLAR12."]:
            sa_type = dialect._map_sas_type_to_sqlalchemy("num", 8, int_format)
            assert isinstance(sa_type, sqltypes.INTEGER), f"Failed for format {int_format}"

    def test_type_mapping_num_with_decimal_format(self):
        """Test NUM with decimal formats."""
        dialect = SASDialect()

        # Test formats with decimal places
        sa_type = dialect._map_sas_type_to_sqlalchemy("num", 8, "COMMA10.2")
        assert isinstance(sa_type, sqltypes.NUMERIC)

        sa_type = dialect._map_sas_type_to_sqlalchemy("num", 8, "DOLLAR12.2")
        assert isinstance(sa_type, sqltypes.NUMERIC)

    def test_normalize_name(self):
        """Test name normalization to uppercase."""
        dialect = SASDialect()

        # Test various cases
        assert dialect.normalize_name("tablename") == "TABLENAME"
        assert dialect.normalize_name("TableName") == "TABLENAME"
        assert dialect.normalize_name("TABLENAME") == "TABLENAME"
        assert dialect.normalize_name("table_name") == "TABLE_NAME"

    def test_normalize_name_none(self):
        """Test that None is handled correctly in normalize_name."""
        dialect = SASDialect()
        assert dialect.normalize_name(None) is None

    def test_denormalize_name(self):
        """Test name denormalization to lowercase."""
        dialect = SASDialect()

        # Test various cases
        assert dialect.denormalize_name("TABLENAME") == "tablename"
        assert dialect.denormalize_name("TableName") == "tablename"
        assert dialect.denormalize_name("tablename") == "tablename"
        assert dialect.denormalize_name("TABLE_NAME") == "table_name"

    def test_denormalize_name_none(self):
        """Test that None is handled correctly in denormalize_name."""
        dialect = SASDialect()
        assert dialect.denormalize_name(None) is None

    def test_do_rollback_noop(self):
        """Test that rollback is a no-op (SAS doesn't support transactions)."""
        dialect = SASDialect()
        mock_connection = Mock()

        # Should not raise any exception
        dialect.do_rollback(mock_connection)

        # Verify no methods were called on the connection
        mock_connection.assert_not_called()

    def test_do_commit_noop(self):
        """Test that commit is a no-op (SAS doesn't support transactions)."""
        dialect = SASDialect()
        mock_connection = Mock()

        # Should not raise any exception
        dialect.do_commit(mock_connection)

        # Verify no methods were called on the connection
        mock_connection.assert_not_called()

    def test_has_sequence_always_false(self):
        """Test that has_sequence always returns False (SAS doesn't support sequences)."""
        dialect = SASDialect()
        mock_connection = Mock()

        result = dialect.has_sequence(mock_connection, "test_sequence", "test_schema")
        assert result is False

    def test_get_pk_constraint_empty(self):
        """Test that primary key constraints return empty (SAS doesn't support PKs)."""
        dialect = SASDialect()
        mock_connection = Mock()

        result = dialect.get_pk_constraint(mock_connection, "test_table", "test_schema")

        assert result == {"constrained_columns": [], "name": None}

    def test_get_foreign_keys_empty(self):
        """Test that foreign key constraints return empty list (SAS doesn't support FKs)."""
        dialect = SASDialect()
        mock_connection = Mock()

        result = dialect.get_foreign_keys(mock_connection, "test_table", "test_schema")

        assert result == []

    @patch("spinta.datasets.backends.sql.backends.sas.dialect.registry")
    def test_register_sas_dialect(self, mock_registry):
        """Test that the dialect registration function works correctly."""
        register_sas_dialect()

        # Verify registry.register was called with correct parameters
        mock_registry.register.assert_called_once_with(
            "sas.jdbc", "spinta.datasets.backends.sql.backends.sas.dialect", "SASDialect"
        )
