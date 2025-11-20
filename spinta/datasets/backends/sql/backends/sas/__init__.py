"""
SAS Backend Module Initialization

This module ensures the SAS SQLAlchemy dialect is registered on import,
making it available for engine creation via create_engine('sas+jdbc://...').
"""

from spinta.datasets.backends.sql.backends.sas.dialect import register_sas_dialect

# Register the SAS dialect immediately when this module is imported
# This ensures it's available before any create_engine() calls
register_sas_dialect()
