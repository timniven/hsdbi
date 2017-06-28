import base
import pymongo
import errors


# Static Functions


def projection_dict(projection):
    return dict(zip(projection, [1] * len(projection)))


# MongoDB Implementations


class MongoRepositoryFacade(base.RepositoryFacade):
    """Repository Facade implementation for MongoDB."""

    def __init__(self, **kwargs):
        """Create a new MongoRepositoryFacade.

        The following keyword arguments are expected:
          server
          port
          db_name
        """
        super(MongoRepositoryFacade, self).__init__(**kwargs)
        self._server = kwargs['server']
        self._port = kwargs['port']
        self._db_name = kwargs['db_name']
        self._connection = pymongo.MongoClient(
            host=self._server, port=self._port)
        exec('self._db = self.connection.%s' % self._db_name)

    def dispose(self):
        self._connection.close()


class MongoRepository(base.Repository):
    """Repository implementation for MongoDB."""

    def __init__(self, **kwargs):
        """Create a new MongoRepository.

        Args (expected keyword args):
          collection_name: String, the name of the collection for this
            Repository to access.
          db: pymongo.database.Database with the connection.
        """
        super(MongoRepository, self).__init__(**kwargs)
        self._collection_name = kwargs['collection_name']
        self._db = kwargs['db']
        self._collection = self._db.get_collection(self._collection_name)

    def add(self, items):
        """Add one or more items to the database.

        Args:
          items: one or more objects of the intended type; can be a list or
            a single object.
        """
        if isinstance(items, list):
            for item in items:
                self._collection.insert_one(item)
        else:
            self._collection.insert_one(items)

    def all(self, projection=None):
        """Retrieve all items of this kind from the database.

        Args:
          projection: List, optional, of attributes to project.

        Returns:
          pymongo.cursor.Cursor with results.
        """
        return self._collection.find({}, projection_dict(projection))

    def commit(self):
        """Override is forced, so we ensure what logic we want."""
        pass

    def count(self):
        """Count the number of records in the collection."""
        return self._collection.count()

    def delete(self, items=None, **kwargs):
        """Delete item(s) from the database.

        Can either specify items directly as a single object of the expected
        type, or as a list of such objects; or specify a record to delete
        by keyword arguments.

        Args:
          items: one or more objects of the intended type; can be a list or
            a single object.
          kwargs: is a document to select.

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
        """Delete all records from this table.
        """
        self._collection.drop()

    def dispose(self):
        """Dipose of the MongoRepository."""
        self._db.logout()

    def get(self, expect=True, projection=None, **kwargs):
        """Get an item from the database.

        Pass the primary key values in as keyword arguments.

        Args:
          expect: whether or not to expect the result. Will raise an exception
            if not found if True.
          projection: List of String attribute names to project, optional.
          kwargs: dictionary of search values.

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
        """Attempt to get items (plural) from the database.

        Pass whatever attributes you want to keyword arguments.

        Args:
          projection: List of String attribute names to project, optional.

        Returns:
          pymongo.cursor.Cursor with matching results (if any).
        """
        return self._collection.find(kwargs, projection_dict(projection))
