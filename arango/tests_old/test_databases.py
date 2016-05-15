"""Tests for managing ArangoDB databases."""

import unittest

from arango import Arango
from arango.utils import is_str
from arango.tests_old.utils import (
    generate_db_name
)


class DatabaseManagementTest(unittest.TestCase):
    """Tests for managing ArangoDB databases."""

    def setUp(self):
        self.arango = Arango()
        self.db_name = generate_db_name(self.arango)

        # Test database cleanup
        self.addCleanup(self.arango.drop_database,
                        name=self.db_name, safe_delete=True)

    def test_database_create_and_delete(self):
        self.arango.create_database(self.db_name)
        self.assertIn(self.db_name, self.arango.list_databases())

        # Check the properties of the new database
        self.assertEqual(self.arango.db(self.db_name).name,
                         self.db_name)
        self.assertEqual(self.arango.db(self.db_name).is_system, False)

        # Delete the test database
        self.arango.drop_database(self.db_name)
        self.assertNotIn(self.db_name, self.arango.list_databases())

    def test_database_properties(self):
        db = self.arango.db("_system")
        self.assertEqual(db.name, "_system")
        self.assertTrue(isinstance(db.details(), dict))
        self.assertTrue(is_str(db.id))
        self.assertTrue(is_str(db.file_path))
        self.assertEqual(db.is_system, True)


if __name__ == "__main__":
    unittest.main()
