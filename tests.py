import unittest
import sqlalchemy as sa
from sqlalchemy.ext import declarative
import sql
import errors


LTEST_CONN_STR = 'mysql+pymysql://root:TimN7367#@localhost/ltest'
Base = declarative.declarative_base()
ORM_MODULE = 'tests'


class Foo(Base):
    __tablename__ = 'foos'
    abbr = sa.Column(sa.String(3), primary_key=True)
    name = sa.Column(sa.String(45), nullable=False)

    def __repr__(self):
        return 'abbr: %s; name: %s' % (self.abbr, self.name)


class TestFacade(sql.SQLRepositoryFacade):
    def __init__(self, **kwargs):
        super(TestFacade, self).__init__(**kwargs)
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
        with TestFacade(connection_string=LTEST_CONN_STR) as _:
            self.assertTrue(True)
            # that's all we need here

    def test_commit(self):
        with TestFacade(connection_string=LTEST_CONN_STR) as db:
            db.foos.add(Foo(abbr='ABC', name='abc'))
            db.commit()
        with TestFacade(connection_string=LTEST_CONN_STR) as db:
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


if __name__ == '__main__':
    unittest.main()
