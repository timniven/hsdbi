import unittest
import sqlalchemy as sa
from sqlalchemy.ext import declarative
from hsdbi import sql
from hsdbi import errors
from hsdbi import mongo
import pymongo


LTEST_CONN_STR = 'mysql+pymysql://root:TimN7367#@localhost/ltest'
Base = declarative.declarative_base()
ORM_MODULE = 'testing.tests'


class Foo(Base):
    __tablename__ = 'foos'
    abbr = sa.Column(sa.String(3), primary_key=True)
    name = sa.Column(sa.String(45), nullable=False)

    def __repr__(self):
        return 'abbr: %s; name: %s' % (self.abbr, self.name)


class TestSQLFacade(sql.SQLFacade):
    def __init__(self, connection_string, **kwargs):
        super(TestSQLFacade, self).__init__(connection_string, **kwargs)
        self.foos = sql.SQLRepository(
            class_type=Foo, orm_module=ORM_MODULE, primary_keys=['abbr'],
            session=self.session)


class SQLRepositoryFacadeTests(unittest.TestCase):
    def setUp(self):
        self.repository = sql.SQLRepository(
            class_type=Foo, orm_module=ORM_MODULE, primary_keys=['abbr'],
            connection_string=LTEST_CONN_STR)
        self.test_attrs = [('ABC', 'abc'), ('DEF', 'def'), ('GHI', 'def')]
        for test_attr in self.test_attrs:
            if self.repository.exists(abbr=test_attr[0]):
                self.repository.delete(
                    self.repository.get(abbr=test_attr[0]))
        self.repository.commit()

    def tearDown(self):
        for test_attr in self.test_attrs:
            if self.repository.exists(abbr=test_attr[0]):
                self.repository.delete(
                    self.repository.get(abbr=test_attr[0]))
        self.repository.commit()
        self.repository.dispose()

    # Tests implementation on the base class.
    # This should also indirectly test the __init__ method.
    def test_enter_exit_implementation(self):
        with TestSQLFacade(connection_string=LTEST_CONN_STR) as _:
            self.assertTrue(True)
            # that's all we need here

    def test_commit(self):
        with TestSQLFacade(connection_string=LTEST_CONN_STR) as db:
            db.foos.add(Foo(abbr='ABC', name='abc'))
            db.commit()
        with TestSQLFacade(connection_string=LTEST_CONN_STR) as db:
            self.assertTrue(db.foos.exists(abbr='ABC'))


class SQLRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.repository = sql.SQLRepository(
            class_type=Foo, orm_module=ORM_MODULE, primary_keys=['abbr'],
            connection_string=LTEST_CONN_STR)
        self.test_attrs = [('ABC', 'abc'), ('DEF', 'def'), ('GHI', 'def')]
        for test_attr in self.test_attrs:
            if self.repository.exists(abbr=test_attr[0]):
                self.repository.delete(
                    self.repository.get(abbr=test_attr[0]))
        self.repository.commit()

    def tearDown(self):
        for test_attr in self.test_attrs:
            if self.repository.exists(abbr=test_attr[0]):
                self.repository.delete(
                    self.repository.get(abbr=test_attr[0]))
        self.repository.commit()
        self.repository.dispose()

    def _insert_one(self):
        foo = Foo(abbr='ABC', name='abc')
        self.repository.add(foo)
        self.repository.commit()

    def _insert_two(self):
        foos = [Foo(abbr='ABC', name='abc'),
                Foo(abbr='DEF', name='def')]
        self.repository.add(foos)
        self.repository.commit()

    def _insert_three(self):
        foos = [Foo(abbr='ABC', name='abc'),
                Foo(abbr='DEF', name='def'),
                Foo(abbr='GHI', name='def')]
        self.repository.add(foos)
        self.repository.commit()

    def test_add_one(self):
        self._insert_one()
        self.assertTrue(self.repository.exists(abbr='ABC'))

    def test_add_list(self):
        self._insert_two()
        self.assertTrue(self.repository.exists(abbr='ABC'))
        self.assertTrue(self.repository.exists(abbr='DEF'))

    def test_all(self):
        self._insert_two()
        foos = self.repository.all()
        self.assertIsInstance(foos, list)
        self.assertTrue(len(foos) >= 2)

    def test_all_with_projection(self):
        self._insert_two()
        foos = self.repository.all(projection=['name'])
        self.assertIsInstance(foos, list)
        self.assertTrue(len(foos) >= 2)
        self.assertEqual(foos[0], ('abc',))
        self.assertEqual(foos[1], ('def',))

    def test_commit(self):
        with sql.SQLRepository(
                class_type=Foo, orm_module=ORM_MODULE, primary_keys=['abbr'],
                connection_string=LTEST_CONN_STR) \
                as repository:
            foo = Foo(abbr='ABC', name='abc')
            repository.add(foo)
            repository.commit()
        with sql.SQLRepository(
                class_type=Foo, orm_module=ORM_MODULE, primary_keys=['abbr'],
                connection_string=LTEST_CONN_STR) \
                as repository:
            self.assertTrue(repository.exists(abbr='ABC'))

    def test_count(self):
        self._insert_two()
        self.assertTrue(self.repository.count() >= 2)

    def test_delete_one(self):
        self._insert_one()
        foo = self.repository.get(abbr='ABC')
        self.repository.delete(foo)
        self.repository.commit()
        self.assertFalse(self.repository.exists(abbr='ABC'))

    def test_delete_one_from_kwargs(self):
        self._insert_one()
        self.repository.delete(abbr='ABC')
        self.repository.commit()
        self.assertFalse(self.repository.exists(abbr='ABC'))

    def test_delete_list(self):
        self._insert_three()
        foos = self.repository.search(name='def')
        self.repository.delete(foos)
        self.assertFalse(self.repository.exists(abbr='DEF'))
        self.assertFalse(self.repository.exists(abbr='GHI'))

    def test_delete_all_records(self):
        self._insert_three()
        self.repository.delete_all_records()
        self.assertFalse(self.repository.exists(abbr='ABC'))
        self.assertFalse(self.repository.exists(abbr='DEF'))
        self.assertFalse(self.repository.exists(abbr='GHI'))

    def test_enter_exit_implementation(self):
        with sql.SQLRepository(
                class_type=Foo, orm_module=ORM_MODULE, primary_keys=['abbr'],
                connection_string=LTEST_CONN_STR) \
                as _:
            # getting this far without an exception is a pass
            self.assertTrue(True)

    def test_exists(self):
        self._insert_one()
        self.assertTrue(self.repository.exists(abbr='ABC'))

    def test_get_returns_if_exists(self):
        self._insert_one()
        foo = self.repository.get(abbr='ABC')
        self.assertIsNotNone(foo)

    def test_get_raises_if_not_exists_and_expect(self):
        with self.assertRaises(errors.NotFoundError):
            self.repository.get(abbr='ABC')

    def test_get_does_not_raise_if_not_exists_and_not_expect(self):
        foo = self.repository.get(abbr='ABC', expect=False)
        self.assertIsNone(foo)

    def test_get_with_projection(self):
        self._insert_one()
        name = self.repository.get(abbr='ABC', projection=['name'])
        self.assertEqual(name, 'abc')

    def test_init_throws_exception_if_no_session_and_conn_string(self):
        with self.assertRaises(ValueError):
            sql.SQLRepository(
                class_type=Foo, orm_module=ORM_MODULE, primary_keys=['abbr'])

    def test_search(self):
        self._insert_three()
        foos = self.repository.search(name='def')
        self.assertIsInstance(foos, list)
        self.assertEqual(len(foos), 2)

    def test_search_with_projection(self):
        self._insert_three()
        abbrs = self.repository.search(name='def', projection=['abbr'])
        self.assertTrue(len(abbrs) == 2)
        self.assertEqual(abbrs[0], ('DEF',))
        self.assertEqual(abbrs[1], ('GHI',))

    def test_search_with_debug(self):
        self._insert_three()
        _ = self.repository.search(name='def', projection=['abbr'], debug=True)


class TestMongoFacade(mongo.MongoFacade):
    def __init__(self):
        super(TestMongoFacade, self).__init__('localhost', 27017)
        self.snli = mongo.MongoDbFacade(
            self.connection,
            db_name='snli',
            collections=['train', 'dev', 'test'])
        self.mnli = mongo.MongoDbFacade(
            self.connection,
            db_name='mnli',
            collections=['train', 'dev_matched', 'dev_mismatched'])

    def __enter__(self):
        self.__init__()
        return self


class MongoFacadeTests(unittest.TestCase):
    def test_enter_exit_implementation(self):
        with mongo.MongoFacade(
                server='localhost',
                port=27017) as _:
            self.assertTrue(True)

    def test_getitem_implementation(self):
        with TestMongoFacade() as db:
            self.assertIsInstance(db['snli'], mongo.MongoDbFacade)
            self.assertIsInstance(db['mnli'], mongo.MongoDbFacade)
            self.assertIsInstance(db['snli']['train'], mongo.MongoRepository)
            self.assertIsInstance(db['mnli']['train'], mongo.MongoRepository)


class MongoDbFacadeTests(unittest.TestCase):
    def test_init_OK(self):
        with mongo.get_connection(server='localhost', port=27017) as connection:
            _ = mongo.MongoDbFacade(
                connection=connection,
                db_name='snli')
            self.assertTrue(True)

    def test_init_with_collections(self):
        with mongo.get_connection(server='localhost', port=27017) as connection:
            db = mongo.MongoDbFacade(
                connection=connection,
                db_name='snli',
                collections=['train', 'dev', 'test'])
            self.assertIsInstance(db.train, mongo.MongoRepository)
            self.assertIsInstance(db.dev, mongo.MongoRepository)
            self.assertIsInstance(db.test, mongo.MongoRepository)


class MongoRepositoryTests(unittest.TestCase):
    def setUp(self):
        db = mongo.get_connection().test
        self.repository = mongo.MongoRepository(
            db=db,
            collection_name='foo')
        self.test_cases = [('ABC', 'abc'), ('DEF', 'def'), ('GHI', 'def')]
        self.repository.delete_all_records()

    def tearDown(self):
        self.repository.delete_all_records()

    def _insert_one(self):
        self.repository.add(_id='ABC', name='abc')

    def _insert_two(self):
        self.repository.add(_id='ABC', name='abc')
        self.repository.add(_id='DEF', name='def')

    def _insert_three(self):
        self.repository.add(_id='ABC', name='abc')
        self.repository.add(_id='DEF', name='def')
        self.repository.add(_id='GHI', name='def')

    def test_add(self):
        self.repository.add(_id='ABC', name='abc')
        self.assertTrue(self.repository.exists(_id='ABC'))

    def test_all(self):
        self._insert_three()
        all = list(self.repository.all())
        self.assertTrue(len(all) >= 3)

    def test_all_with_projection(self):
        self._insert_three()
        all = list(self.repository.all(projection=['_id']))
        self.assertTrue(len(all) >= 3)
        self.assertEqual(len(all[0]), 1)
        self.assertEqual(len(all[1]), 1)
        self.assertEqual(len(all[2]), 1)
        self.assertEqual(all[0]['_id'], 'ABC')
        self.assertEqual(all[1]['_id'], 'DEF')
        self.assertEqual(all[2]['_id'], 'GHI')

    def test_count(self):
        self._insert_three()
        self.assertTrue(self.repository.count() >= 3)

    def test_delete_single(self):
        self._insert_one()
        item = self.repository.get(_id='ABC')
        self.repository.delete(item)
        self.assertFalse(self.repository.exists(_id='ABC'))

    def test_delete_list(self):
        self._insert_two()
        items = [self.repository.get(_id='ABC'),
                 self.repository.get(_id='DEF')]
        self.repository.delete(items)
        self.assertFalse(self.repository.exists(_id='ABC'))
        self.assertFalse(self.repository.exists(_id='DEF'))

    def test_delete_from_kwargs(self):
        self._insert_one()
        self.repository.delete(_id='ABC')
        self.assertFalse(self.repository.exists(_id='ABC'))

    def test_delete_all_records(self):
        self._insert_three()
        self.repository.delete_all_records()
        self.assertEqual(self.repository.count(), 0)

    def test_get(self):
        self._insert_one()
        item = self.repository.get(_id='ABC')
        self.assertIsNotNone(item)

    def test_get_with_projection(self):
        self._insert_one()
        item = self.repository.get(_id='ABC', projection=['_id'])
        self.assertEqual(len(item), 1)
        self.assertEqual(item['_id'], 'ABC')

    def test_get_where_not_exists_and_expected_raises_exception(self):
        with self.assertRaises(errors.NotFoundError):
            self.repository.get(_id='ABC')

    def test_get_where_not_exists_not_expected_does_not_raise_exception(self):
        self.repository.get(_id='ABC', expect=False)
        self.assertTrue(True)

    def test_search(self):
        self._insert_three()
        items = list(self.repository.search(name='def'))
        self.assertEqual(len(items), 2)

    def test_search_with_projection(self):
        self._insert_three()
        items = list(self.repository.search(name='def', projection=['_id']))
        self.assertEqual(len(items), 2)
        self.assertEqual(len(items[0]), 1)
        self.assertEqual(len(items[1]), 1)
        self.assertEqual(items[0]['_id'], 'DEF')
        self.assertEqual(items[1]['_id'], 'GHI')

    def test_update(self):
        self._insert_one()
        doc = self.repository.get(_id='ABC')
        doc['new_attr'] = 123
        self.repository.update(doc)
        doc2 = self.repository.get(_id='ABC')
        self.assertEqual(doc2['new_attr'], 123)
        self.assertEqual(doc2['_id'], doc['_id'])
