import unittest
from testing import tests


# > python -m unittest discover


test_cases = [
    tests.SQLRepositoryFacadeTests,
    tests.SQLRepositoryTests,
    tests.MongoFacadeTests,
    tests.MongoRepositoryTests,
    tests.MongoDbFacadeTests,

]


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_case in test_cases:
        tests = loader.loadTestsFromTestCase(test_case)
        suite.addTests(tests)
    return suite
