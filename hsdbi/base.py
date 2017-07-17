"""Base classes."""


class RepositoryFacade:
    """Abstract base class for a Repository Facade.

    From the Wikipedia entry on "Facade pattern":
      'A facade is an object that provides a simplified interface to a
      larger body of code...'
      https://en.wikipedia.org/wiki/Facade_pattern
    In this case, we provide a single point of access for all Repository
    classes grouped in a conceptual unit, encapsulate the db connection,
    provide a commit() function for saving changes, and implement the magic
    methods __exit__ and __enter__ so this class is valid for use in a "with"
    statement.

    Collecting multiple repositories together might be viewed as an
    inefficiency: it is simple enough to initialize one Repository class as and
    when it is needed. Indeed, this is how I use repositories.

    The Facade comes in handy where we want to share database context between
    Repository classes.

    Implementation details for Repository Facade classes will differ with the
    database used. The intention is for a subclass to be defined for each
    such case. See MySQLRepositoryFacade and MongoRepositoryFacade, for example.
    """

    def __init__(self):
        """Create a new RepositoryFacade."""

    def __enter__(self):
        self.__init__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Implementation note - see:
        https://stackoverflow.com/questions/22417323/
        how-do-enter-and-exit-work-in-python-decorator-classes
        """
        self.dispose()

    def dispose(self):
        """Close the connection for disposal of the RepositoryFacade."""
        raise NotImplementedError()


class Repository:
    """Abstract Repository class

    The docstrings here will indicate the intention of the functions and their
    arguments. Child classes can extend these in part, requiring new arguments.

    The base class remains agnostic as to return types as this will vary across
    databases.

    The exception is the exists() function, which is implemented here.
    """

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __enter__(self):
        self.__init__(**self._kwargs)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Implementation note - see:
        https://stackoverflow.com/questions/22417323/
        how-do-enter-and-exit-work-in-python-decorator-classes
        """
        self.dispose()

    def add(self, item):
        """Add one or more items to the database.

        Args:
          items: an object or list of objects to add.
        """
        raise NotImplementedError()

    def all(self, projection=None):
        """Get all records in the database.

        Args:
          projection: List, optional, of attributes to project.
        """
        raise NotImplementedError()

    def commit(self):
        """Save changes to the database."""
        raise NotImplementedError()

    def count(self):
        """Get a count of how many records are in this table/collection."""
        raise NotImplementedError()

    def delete(self, items=None, **kwargs):
        """Delete item(s) from the database.

        May either specify an object or list of objects to delete, or
        keyword arguments for primary key identification.

        Args:
          items: an object or list of objects to delete.
          kwargs: can specify primary key attribute name(s) and value(s).
        """
        raise NotImplementedError()

    def dispose(self):
        """Dispose of this Repository."""
        raise NotImplementedError()

    def exists(self, **kwargs):
        """Check if a record exists.

        Pass the primary key values in as keyword arguments.

        Returns:
          Boolean indicating if the record exists.
        """
        # NOTE: to project here or not?
        return len(list(self.search(**kwargs))) > 0

    def get(self, expect=True, projection=None, **kwargs):
        """Get an item from the database.

        Primary key values need to be passed as keyword arguments.

        Arguments:
          expect: Bool, whether to throw an error if not found.
          projection: List, optional, of attributes to project.
          kwargs: can specify the primary key attribute name(s) and value(s).
        """
        raise NotImplementedError()

    def search(self, projection=None, **kwargs):
        """Search for records in the database.

        Specify the search attributes and values as keyword arguments.

        NOTE: This must return a generator or list of records - this is
        necessary for the default implementation of the exists() method. Any
        change to this means you should re-implement exists().

        Args:
          projection: List, optional, of attributes to project.
          kwargs: attribute name(s) and value(s) to search on.

        Returns:
          Generator or list of records.
        """
        raise NotImplementedError()
