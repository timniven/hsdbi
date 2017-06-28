"""Objects for interfacing with SQL databases."""
import base
import errors
import importlib
import sqlalchemy as sa
from sqlalchemy import orm as saorm
from sqlalchemy import func


# Static Functions


def create_sql_session(connection_string):
    """Create a SQLAlchemy session from a connection string.

    Args:
      connection_string: String.

    Returns:
      SQLAlchemy session object.
    """
    engine = sa.create_engine(connection_string)
    session = saorm.sessionmaker(bind=engine)()
    return session


# SQL Implementations


class SQLRepository(base.Repository):
    """Generic wrapper for db access methods for a table."""

    def __init__(self, **kwargs):
        """Create a new Repository.

        Args (expected keyword args):
          class_type: Type, the type of the object this repository will handle.
            It should be one of the orm classes.
          orm_module: String, the name of the module containing orm classes
            that this Repository will work on.
          primary_keys: List of strings, identify the attribute names that
            represent the primary keys for this class.
          session: SQLAlchemy session object, optional.
        """
        super(SQLRepository, self).__init__(**kwargs)
        if 'connection_string' in kwargs.keys():
            self._connection_string = kwargs['connection_string']
            self._session = create_sql_session(self._connection_string)
        elif 'session' in kwargs.keys():
            self._session = kwargs['session']
        else:
            raise ValueError('You must pass either a session or '
                             'connection string.')
        self._class_type = kwargs['class_type']
        self._orm_module = kwargs['orm_module']
        globals()['orm'] = importlib.import_module(kwargs['orm_module'])
        self._primary_keys = kwargs['primary_keys']

    def add(self, items):
        """Add one or more items to the database.

        Args:
          items: one or more objects of the intended type; can be a list or
            a single object.
        """
        if isinstance(items, list):
            for item in items:
                self._session.add(item)
        else:
            self._session.add(items)

    def all(self, projection=None):
        """Retrieve all items of this kind from the database.

        Args:
          projection: List, optional, of attributes to project.

        Returns:
          List of items of the relevant type.
        """
        query = self._session.query(self._class_type)
        if projection:
            query = self.project(query, projection)
        return query.all()

    def commit(self):
        """Commit changes to the database."""
        self._session.commit()

    def count(self):
        """Count the number of records in the table.

        Returns:
          Integer, the number of records in the table.
        """
        return self._session\
            .query(func.count(eval('orm.%s.%s' % (self._class_type.__name__,
                                                  self._primary_keys[0]))))\
            .scalar()

    def delete(self, items=None, **kwargs):
        """Delete item(s) from the database.

        Can either specify items directly as a single object of the expected
        type, or as a list of such objects; or specify a record to delete
        by primary key via keyword arguments.

        Args:
          items: one or more objects of the intended type; can be a list or
            a single object.

        Raises:
          ValueError: if neither items nor keyword arguments are specified.
          ValueError: if items are not specified and any primary key is
            missing from the keyword arguments.
        """
        if not items and len(kwargs) == 0:
            raise ValueError('You must specify either items or kwargs.')
        if items:
            if isinstance(items, list):
                for item in items:
                    self._session.delete(item)
            else:
                self._session.delete(items)
        else:
            for pk in self._primary_keys:
                if pk not in kwargs.keys():
                    raise TypeError('Missing keyword argument: %s' % pk)
            self._session.delete(self.get(**kwargs))

    def delete_all_records(self):
        """Delete all records from this table.

        Returns:
          Integer, the number of records deleted.
        """
        return self._session.query(self._class_type).delete()

    def dispose(self):
        """Dispose of the database connection."""
        self._session.close()

    def get(self, expect=True, projection=None, **kwargs):
        """Get an item from the database.

        Pass the primary key values in as keyword arguments.

        Args:
          expect: whether or not to expect the result. Will raise an exception
            if not found if True.
          projection: List of String attribute names to project, optional.

        Raises:
          TypeError: if any primary key values are missing from the keyword
            arguments received.
          NotFoundError: if the item is not found in the database.
        """
        for pk in self._primary_keys:
            if pk not in kwargs.keys():
                raise TypeError('Missing keyword argument: %s' % pk)
        query = self._session.query(self._class_type)
        for attr, value in kwargs.items():
            query = query.filter(
                eval("orm.%s.%s == '%s'"
                     % (self._class_type.__name__, attr, value)))
        if projection:
            query = self.project(query, projection)
        result = query.one_or_none()
        if not result and expect:
            raise errors.NotFoundError(pk=kwargs, table=self._class_type)
        if projection:
            return result[0]
        else:
            return result

    def project(self, query, projection):
        """Perfoms a projection on the given query.

        Args:
          query: the query object to project.
          projection: List of String attributes to project.

        Returns:
          List of tuples of projected values.

        Raises:
          ValueError if projection is not a list. It is easy to pass a string
            here, so we will catch this case for quick debugging.
        """
        if not isinstance(projection, list):
            raise ValueError('projection must be a list.')
        projection = ['orm.%s.%s' % (self._class_type.__name__, attr)
                      for attr in projection]
        return query.with_entities(eval(', '.join(projection)))

    def search(self, projection=None, **kwargs):
        """Attempt to get items (plural) from the database.

        Pass whatever attributes you want to keyword arguments.

        Args:
          projection: List of String attribute names to project, optional.

        Returns:
          List of matching results.
        """
        query = self._session.query(self._class_type)
        for attr, value in kwargs.items():
            query = query.filter(
                eval("orm.%s.%s == '%s'"
                     % (self._class_type.__name__, attr, value)))
        if projection:
            query = self.project(query, projection)
        return query.all()


class SQLRepositoryFacade(base.RepositoryFacade):
    """Facade for repository access.

    From the Wikipedia entry on "Facade pattern":
      'A facade is an object that provides a simplified interface to a
      larger body of code...'
      https://en.wikipedia.org/wiki/Facade_pattern
    In this case, we provide a single point of access for all Repository
    classes grouped in a conceptual unit, encapsulate the db connection,
    provide a commit() function for saving changes, and implement the magic
    methods __exit__ and __enter__ so this class is valid for use in a "with"
    statement.

    Attributes:
      session: the sqlalchemy session wrapped by this facade. There was a
        question of whether to expose this. It was decided to expose it because
        the Repository classes need access to it, and because it will enable
        flexibility since consumers can directly use it if convenient.
    """

    def __init__(self, **kwargs):
        """Create a new RepositoryFacade.

        This will create the self._engine and self._session variables from
        the passed connection string. It also saves self._connection_string
        for reference.

        Args:
          connection_info: the connection string to the database.
        """
        super(SQLRepositoryFacade, self).__init__(**kwargs)
        self._engine = sa.create_engine(kwargs['connection_string'])
        self.session = saorm.sessionmaker(bind=self._engine)()

    def commit(self):
        """Save changes to the database.

        Calls commit() on the sqlalchemy session object.
        """
        self.session.commit()

    def dispose(self):
        """Dispose of this class.

        Will explicitly dispose of the database connection.
        """
        self.session.close()