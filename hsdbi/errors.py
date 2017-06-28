"""Custom errors for this package."""


class Error(Exception):
    pass


class NotFoundError(Exception):
    """A record was not found in the database.

    Attributes:
      pk: Dictionary, attr-value pairs representing the primary keys.
      table: the table searched.
    """

    def __init__(self, pk, table):
        self.pk = pk
        self.table = table
