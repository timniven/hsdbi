"""MongoDB implementations."""
from hsdbi import base
import pymongo
from hsdbi import errors


# Static Functions


def get_connection(server='localhost', port=27017):
    """Get a connection to a mongo server.

    Args:
      server: String, the address of the server, e.g. 'localhost' (the default).
      port: Integer, the port number to connect to, e.g. 27017 (the default).

    Returns:
      pymongo.mongo_client.MongoClient, connection to the database server.
    """
    return pymongo.MongoClient(host=server, port=port)


def projection_dict(projection):
    """Get a projection dictionary from a list of attribute names to project.

    Args:
      projection: List of string names of attributes to project.

    Returns:
      Dictionary like {'attr1': 1, 'attr': 1, ... }.
    """
    if projection:
        return dict(zip(projection, [1] * len(projection)))
    else:
        return {}


# MongoDB Implementations


class MongoFacade(base.RepositoryFacade):
    """"Meta" Facade implementation for MongoDB.

    With an SQL Repository facade, we connect to a single database instance.
    With a Mongo Repository facade however, we connect to the database server.
    So we will not specify a database to connect to. We leave this to the
    DBFacade class. This takes the connection from the MongoFacade class,
    creates a connection to the db required, and then passes this to the
    Repository class for further sub-connection to a collection. This will
    avoid the need to make multiple connections to the database server, and
    still provide a nice clean interface.

    Attributes:
      connection: pymongo.mongo_client.MongoClient, connection to the database
        server.
    """

    def __init__(self, server, port, **kwargs):
        """Create a new MongoRepositoryFacade.

        Args:
          server: String, the address of the server. E.g. 'localhost'.
          port: Integer, the port number to connect to. E.g. 27017.
        """
        super(MongoFacade, self).__init__(**kwargs)
        self._server = server
        self._port = port
        self.connection = get_connection(self._server, self._port)

    def __enter__(self):
        self.__init__(self._server, self._port, **self._kwargs)
        return self

    def dispose(self):
        self.connection.close()


class MongoDbFacade:
    """Facade for a database in a mongo server.

    Attributes:
      db: pymongo.database.Database, the connection to the database.
    """

    def __init__(self, connection, db_name, collections=None, **kwargs):
        """Create a new MongoDbFacade.

        Args:
          connection: pymongo.mongo_client.MongoClient, connection to the
            database server.
          db_name: String, the name of the db to connect to.
          collections: optional list of Strings, the names of the collections.
            If specified MongoRepository objects will be automatically added
            to this class on initialization using these collection names.
        """
        self._connection = connection
        self._db_name = db_name
        self.db = connection.get_database(db_name)
        if collections:
            for collection_name in collections:
                exec('self.%s = self.db.%s' % (collection_name,
                                               collection_name))


class MongoRepository(base.Repository):
    """Repository implementation for MongoDB."""

    def __init__(self, db, collection_name, **kwargs):
        """Create a new MongoRepository.

        Args:
          db: pymongo.database.Database, the connection to the database.
          collection_name: String, the name of the collection we intend to
            connect to.
        """
        super(MongoRepository, self).__init__(**kwargs)
        self._db = db
        self._collection_name = collection_name
        self._collection = self._db.get_collection(self._collection_name)

    def __enter__(self):
        self.__init__(self._db, self._collection_name, **self._kwargs)
        return self

    def add(self, items=None, **kwargs):
        """Add one or more items to the database.

        Args:
          items: one or more objects of the intended type; can be a list or
            a single object.
          kwargs: if items is not specified, the kwargs dictionary is used
            to create the record.

        Raises:
          ValueError: if neither items nor kwargs are specified.
        """
        if items:
            if isinstance(items, list):
                for item in items:
                    self._collection.insert_one(item)
            else:
                self._collection.insert_one(items)
        elif len(kwargs) > 0:
            self._collection.insert_one(kwargs)
        else:
            raise ValueError('You must specify either items or kwargs')

    def all(self, projection=None):
        """Retrieve all items of this kind from the database.

        Args:
          projection: List, optional, of attributes to project.

        Returns:
          pymongo.cursor.Cursor with results.
        """
        return self._collection.find({}, projection_dict(projection))

    def commit(self):
        """Does nothing for a MongoRepository."""
        pass

    def count(self):
        """Count the number of records in the collection.

        Returns:
          Integer, the number of records in the collection.
        """
        return self._collection.count()

    def delete(self, items=None, **kwargs):
        """Delete item(s) from the database.

        Can either specify items directly as a single object of the expected
        type, or as a list of such objects; or specify a record to delete
        by keyword arguments.

        Args:
          items: one or more objects of the intended type; can be a list or
            a single object.
          kwargs: specify the attribute name(s) and value(s) of a document to
            be deleted.

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
                    self._collection.delete_one(item)
            else:
                self._collection.delete_one(items)
        else:
            self._collection.delete_one(kwargs)

    def delete_all_records(self):
        """Delete all records from this collection."""
        self._collection.drop()

    def dispose(self):
        """Dispose of the database server connection."""
        self._db.client.close()

    def get(self, expect=True, projection=None, **kwargs):
        """Get an item from the database.

        Pass the primary key values in as keyword arguments.

        Args:
          expect: whether or not to expect the result. Will raise an exception
            if not found if True.
          projection: List of String attribute names to project, optional.
          kwargs: dictionary of search values.

        Returns:
          Dictionary: representing the retrieved document, if found.

        Raises:
          NotFoundError: if the item is not found in the database, but it is
            expected.
        """
        item = next(self._collection.find(kwargs,
                                          projection_dict(projection)),
                    None)
        if not item and expect:
            raise errors.NotFoundError(pk=kwargs, table=self._collection_name)
        return item

    def search(self, projection=None, **kwargs):
        """Attempt to get item(s) from the database.

        Pass whatever attributes you want as keyword arguments.

        Args:
          projection: List of String attribute names to project, optional.
          kwargs: the attribute name(s) and value(s) to be used for search.
            If empty this function is equivalent to all().

        Returns:
          pymongo.cursor.Cursor with matching results (if any).
        """
        return self._collection.find(kwargs, projection_dict(projection))
