"""Objects for interfacing with SQL databases."""
from hsdbi import base
from hsdbi import errors
import importlib
import sqlalchemy as sa
from sqlalchemy import orm as saorm
from sqlalchemy import func
from sqlalchemy.dialects import mysql


# Static Functions


def create_sql_session(connection_string):
    """Create a SQLAlchemy session from a connection string.

    Args:
      connection_string: String.

    Returns:
      sqlalchemy.orm.session.Session object.
    """
    engine = sa.create_engine(connection_string)
    session = saorm.sessionmaker(bind=engine)()
    return session


def print_sql(query):
    print(query.statement.compile(dialect=mysql.dialect(),
                                  compile_kwargs={'literal_binds': True}))


# SQL Implementations


class SQLFacade(base.RepositoryFacade):
    """Facade for SQL repositories.

    Attributes:
      session: the sqlalchemy session wrapped by this facade. There is a
        question of whether to expose this. It was decided to expose it because
        it will enable flexibility since consumers can directly use it if
        convenient, providing better extensibility.
    """

    def __init__(self, connection_string):
        """Create a new RepositoryFacade.

        This will create the self._engine and self.session variables from
        the passed connection string. It also saves self._connection_string
        for reference.

        Args:
          connection_string: String, the connection string to the database.
        """
        super(SQLFacade, self).__init__()
        self._connection_string = connection_string
        self.session = create_sql_session(connection_string)

    def __enter__(self):
        self.__init__(self._connection_string)
        return self

    def commit(self):
        """Save changes to the database."""
        self.session.commit()

    def dispose(self):
        """Dispose of this class - close the database connection."""
        self.session.close()


class SQLRepository(base.Repository):
    """Generic wrapper for db access methods for a table."""

    def __init__(self, primary_keys, class_type, orm_module,
                 connection_string=None, session=None, **kwargs):
        """Create a new Repository.

        Args:
          class_type: Type, the type of the object this repository will handle.
            It should be one of the orm classes.
          orm_module: String, the name of the module containing orm classes
            that this Repository will work on. E.g. 'db.orm'.
          primary_keys: List of strings, identify the attribute names that
            represent the primary keys for this class.
          connection_string: String, optional, but must pass one of either
            connection_string or session.
          session: SQLAlchemy session object, optional, but must pass one of
            either connection_string or session.
        """
        super(SQLRepository, self).__init__(**kwargs)
        self._class_type = class_type
        self._orm_module = orm_module
        self._orm_module_key = orm_module.replace('.', '_')
        globals()[self._orm_module_key] = importlib.import_module(orm_module)
        self._primary_keys = primary_keys
        self._connection_string = connection_string
        self._session = session
        if connection_string:
            self._connection_string = connection_string
            self._session = create_sql_session(self._connection_string)
        elif session:
            self._session = session
        else:
            raise ValueError('You must pass either a session or '
                             'connection string.')

    def __enter__(self):
        self.__init__(self._primary_keys, self._class_type, self._orm_module,
                      self._connection_string, self._session, **self._kwargs)
        return self

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
          List of items of the relevant type; or Tuple of values if projected.
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
            .query(func.count(eval('%s.%s.%s' % (self._orm_module_key,
                                                 self._class_type.__name__,
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
          kwargs: can specify the primary key name(s) and value(s).

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

    def get(self, expect=True, projection=None, debug=False, **kwargs):
        """Get an item from the database.

        Pass the primary key values in as keyword arguments.

        Args:
          expect: whether or not to expect the result. Will raise an exception
            if not found if True.
          projection: List of String attribute names to project, optional.
          kwargs: can specify the primary key attribute name(s) and value(s).
          debug: Boolean, if true will print the SQL of the query for
            debugging purposes.

        Raises:
          TypeError: if any primary key values are missing from the keyword
            arguments received.
          NotFoundError: if the item is not found in the database and the expect
            flag is set to True.
        """
        for pk in self._primary_keys:
            if pk not in kwargs.keys():
                raise TypeError('Missing keyword argument: %s' % pk)
        query = self._session.query(self._class_type)
        for attr, value in kwargs.items():
            query = query.filter(
                eval('%s.%s.%s == "%s"'
                     % (self._orm_module_key,
                        self._class_type.__name__, attr, value)))
        if projection:
            query = self.project(query, projection)
        if debug:
            print_sql(query)
        result = query.one_or_none()
        if not result and expect:
            raise errors.NotFoundError(pk=kwargs, table=self._class_type)
        if projection:
            return result[0]
        else:
            return result

    def project(self, query, projection, debug=False):
        """Perfoms a projection on the given query.

        Args:
          query: the query object to project.
          projection: List of String attributes to project.
          debug: Boolean, if true will print the SQL of the query for
            debugging purposes.

        Returns:
          Query object.

        Raises:
          ValueError if projection is not a list. It is easy to pass a string
            here, so we will catch this case for quick debugging.
        """
        if not isinstance(projection, list):
            raise ValueError('projection must be a list.')
        projection = ['%s.%s.%s' % (self._orm_module_key,
                                    self._class_type.__name__, attr)
                      for attr in projection]
        query = query.with_entities(eval(', '.join(projection)))
        if debug:
            print_sql(query)
        return query

    def search(self, projection=None, debug=False, **kwargs):
        """Attempt to get item(s) from the database.

        Pass whatever attributes you want as keyword arguments.

        Args:
          projection: List of String attribute names to project, optional.
          debug: Boolean, if true will print the SQL of the query for
            debugging purposes.

        Returns:
          List of matching results; or Tuple if projected.
        """
        query = self._session.query(self._class_type)
        for attr, value in kwargs.items():
            query = query.filter(
                eval('%s.%s.%s == "%s"'
                     % (self._orm_module_key,
                        self._class_type.__name__,
                        attr,
                        value)))
        if projection:
            query = self.project(query, projection)
        if debug:
            print_sql(query)
        return query.all()
