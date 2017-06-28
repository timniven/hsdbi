"""Base classes."""


class Repository:
    """Abstract Repository class. Essentially an interface declaration."""

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

        Returns:
          List of all records.
        """
        raise NotImplementedError()

    def commit(self):
        """Save changes to the database."""
        raise NotImplementedError()

    def count(self):
        """Get a count of how many records are in this table / collection."""
        raise NotImplementedError()

    def delete(self, items=None, **kwargs):
        """Delete item(s) from the database.

        May either specify an object or list of objects to delete, or
        keyword arguments for primary key identification.

        Args:
          items: an object or list of objects to delete.
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
        return self.get(expect=False, **kwargs) is not None

    def get(self, expect=True, projection=None, **kwargs):
        """Get an item from the database.

        Primary key values need to be passed as keyword arguments.

        Arguments:
          expect: Bool, whether to throw an error if not found.
          projection: List, optional, of attributes to project.
        """
        raise NotImplementedError()

    def search(self, projection=None, **kwargs):
        """Search for records in the database.

        Specify the search attributes and values as keyword arguments.

        Args:
          projection: List, optional, of attributes to project.

        Returns:
          List of records matching the search criteria.
        """
        raise NotImplementedError()


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
    inefficiency: it is simple enough to initialize a Repository class as and
    when it is needed. Indeed, this is how I use repositories.

    The Facade comes in handy where we want to share database context between
    Repository classes.

    Implementation details for Repository Facade classes will differ with the
    database used. The intention is for a subclass to be defined for each
    such case. See MySQLRepositoryFacade and MongoRepositoryFacade, for example.
    """

    def __init__(self, **kwargs):
        """Create a new RepositoryFacade.

        Connection information to be passed as keyword arguments. Different
        subclasses will have different arguments: e.g. SQL passes a
        connection_string, but mongo asks for server, port, and db_name.
        """
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

    def dispose(self):
        raise NotImplementedError()
