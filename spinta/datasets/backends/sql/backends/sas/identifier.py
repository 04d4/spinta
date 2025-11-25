"""
SAS Identifier Handling for SQLAlchemy.

This module provides a custom identifier preparer for SAS SQL syntax.

SAS does not support quoted identifiers in SQL syntax. All identifiers
(table names, column names, schema names, etc.) must be returned unquoted
to prevent SQL syntax errors when executing queries.
"""

from sqlalchemy.sql.compiler import IdentifierPreparer


class SASIdentifierPreparer(IdentifierPreparer):
    """
    Custom identifier preparer for SAS that never quotes identifiers.

    SAS does not support quoted identifiers in SQL syntax. This preparer
    ensures that table names, column names, and other identifiers are never
    wrapped in quotes, preventing SQL syntax errors.

    Example:
        With default preparer: "MY_TABLE" (quoted)
        With SAS preparer: MY_TABLE (unquoted)
    """

    def quote(self, ident, force=None):
        """
        Return the identifier without quotes.

        Args:
            ident: The identifier to (not) quote
            force: Force parameter (ignored for SAS)

        Returns:
            The identifier unchanged, without any quoting
        """
        return ident

    def _requires_quotes(self, ident):
        """
        Determine if an identifier requires quotes.

        SAS never requires quotes for identifiers.

        Args:
            ident: The identifier to check

        Returns:
            Always False for SAS
        """
        return False
