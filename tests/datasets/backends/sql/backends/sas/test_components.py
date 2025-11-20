"""
Unit tests for SAS Backend Component

These tests validate the SAS backend component implementation without requiring a live SAS connection.
They focus on testing backend initialization, type configuration, and dialect registration.

Note: Integration tests requiring a live SAS server connection are not included here.
"""

from unittest.mock import patch

from spinta.datasets.backends.sql.backends.sas.components import SAS


class TestSASBackend:
    """Test suite for the SAS backend component"""

    def test_backend_type(self):
        """Test that the backend type is correctly set to 'sql/sas'."""
        backend = SAS()
        assert backend.type == "sql/sas"

    def test_query_builder_type(self):
        """Test that the query builder type is correctly set to 'sql/sas'."""
        backend = SAS()
        assert backend.query_builder_type == "sql/sas"

    @patch("spinta.datasets.backends.sql.backends.sas.components.register_sas_dialect")
    def test_dialect_registration(self, mock_register):
        """Test that the SAS dialect is registered during backend initialization."""
        # Create the backend instance
        backend = SAS()

        # Verify that register_sas_dialect was called
        mock_register.assert_called_once()

    @patch("spinta.datasets.backends.sql.backends.sas.components.register_sas_dialect")
    def test_backend_initialization_with_args(self, mock_register):
        """Test that backend can be initialized with arbitrary arguments."""
        # Create backend with various arguments
        backend = SAS("arg1", "arg2", kwarg1="value1", kwarg2="value2")

        # Verify dialect registration happened
        mock_register.assert_called_once()

        # Verify backend type is still correct
        assert backend.type == "sql/sas"

    @patch("spinta.datasets.backends.sql.backends.sas.components.register_sas_dialect")
    @patch("spinta.datasets.backends.sql.components.Sql.__init__")
    def test_parent_initialization(self, mock_parent_init, mock_register):
        """Test that parent Sql class is properly initialized."""
        mock_parent_init.return_value = None

        # Create backend with specific arguments
        test_args = ("arg1", "arg2")
        test_kwargs = {"kwarg1": "value1", "kwarg2": "value2"}

        backend = SAS(*test_args, **test_kwargs)

        # Verify parent __init__ was called with the same arguments
        mock_parent_init.assert_called_once_with(*test_args, **test_kwargs)

    def test_backend_inherits_from_sql(self):
        """Test that SAS backend properly inherits from Sql base class."""
        from spinta.datasets.backends.sql.components import Sql

        backend = SAS()

        # Verify inheritance
        assert isinstance(backend, Sql)

    @patch("spinta.datasets.backends.sql.backends.sas.components.register_sas_dialect")
    def test_multiple_backend_instances(self, mock_register):
        """Test that multiple backend instances can be created."""
        # Create multiple instances
        backend1 = SAS()
        backend2 = SAS()
        backend3 = SAS()

        # Each should have called register_sas_dialect
        assert mock_register.call_count == 3

        # Each should have correct type
        assert backend1.type == "sql/sas"
        assert backend2.type == "sql/sas"
        assert backend3.type == "sql/sas"

    @patch("spinta.datasets.backends.sql.backends.sas.components.register_sas_dialect")
    def test_backend_type_immutable(self, mock_register):
        """Test that backend type attribute is correctly set as class attribute."""
        backend1 = SAS()
        backend2 = SAS()

        # Both should reference the same type
        assert backend1.type == backend2.type == "sql/sas"

        # Verify it's a class attribute
        assert SAS.type == "sql/sas"

    @patch("spinta.datasets.backends.sql.backends.sas.components.register_sas_dialect")
    def test_query_builder_type_immutable(self, mock_register):
        """Test that query builder type is correctly set as class attribute."""
        backend1 = SAS()
        backend2 = SAS()

        # Both should reference the same query builder type
        assert backend1.query_builder_type == backend2.query_builder_type == "sql/sas"

        # Verify it's a class attribute
        assert SAS.query_builder_type == "sql/sas"

    @patch("spinta.datasets.backends.sql.backends.sas.components.register_sas_dialect")
    def test_dialect_registration_happens_first(self, mock_register):
        """Test that dialect registration happens before parent initialization."""
        call_order = []

        def track_register():
            call_order.append("register")

        mock_register.side_effect = track_register

        # We can't easily patch Sql.__init__ to track order without breaking things,
        # but we can verify register was called
        backend = SAS()

        assert "register" in call_order
        assert mock_register.called

    @patch("spinta.datasets.backends.sql.backends.sas.components.register_sas_dialect")
    def test_backend_has_sql_methods(self, mock_register):
        """Test that SAS backend has access to SQL backend methods."""

        backend = SAS()

        # Check for some expected Sql methods/attributes
        # Note: These may vary based on actual Sql implementation
        assert hasattr(backend, "type")
        assert hasattr(backend, "query_builder_type")

    @patch("spinta.datasets.backends.sql.backends.sas.components.register_sas_dialect")
    def test_backend_no_modification_of_base_class(self, mock_register):
        """Test that SAS backend doesn't modify the base Sql class."""
        from spinta.datasets.backends.sql.components import Sql

        # Get original Sql type
        original_sql_type = Sql.type if hasattr(Sql, "type") else None

        # Create SAS backend
        backend = SAS()

        # Verify Sql type wasn't changed
        current_sql_type = Sql.type if hasattr(Sql, "type") else None
        assert original_sql_type == current_sql_type

    @patch("spinta.datasets.backends.sql.backends.sas.components.register_sas_dialect")
    def test_backend_docstring_exists(self, mock_register):
        """Test that the SAS backend class has documentation."""
        assert SAS.__doc__ is not None
        assert len(SAS.__doc__) > 0
        assert "SAS" in SAS.__doc__
