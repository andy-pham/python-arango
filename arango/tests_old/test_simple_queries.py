"""Tests for managing ArangoDB documents."""

import unittest

from arango import Arango
from arango.tests_old.utils import (
    generate_col_name,
    generate_db_name,
    strip_system_keys,
)


class SimpleQueriesTest(unittest.TestCase):
    """Tests for managing ArangoDB documents."""

    def setUp(self):
        self.arango = Arango()
        self.db_name = generate_db_name(self.arango)
        self.db = self.arango.create_database(self.db_name)
        self.col_name = generate_col_name(self.db)
        self.col = self.db.create_collection(self.col_name)
        self.col.add_geo_index(["coord"])
        self.col.add_skiplist_index(["value"])
        self.col.add_fulltext_index(["text"])

        # Test database cleanup
        self.addCleanup(self.arango.drop_database,
                        name=self.db_name, safe_delete=True)

    def test_first(self):
        self.assertEqual(strip_system_keys(self.col.first(1)), [])
        self.col.insert_many([
            {"name": "test_doc_01"},
            {"name": "test_doc_02"},
            {"name": "test_doc_03"}
        ])
        self.assertEqual(len(self.col), 3)
        self.assertEqual(
            strip_system_keys(self.col.first(1)),
            [{"name": "test_doc_01"}]
        )
        self.assertEqual(
            strip_system_keys(self.col.first(2)),
            [{"name": "test_doc_01"}, {"name": "test_doc_02"}]
        )

    def test_last(self):
        self.assertEqual(strip_system_keys(self.col.last(1)), [])
        self.col.insert_many([
            {"name": "test_doc_01"},
            {"name": "test_doc_02"},
            {"name": "test_doc_03"}
        ])
        self.assertEqual(len(self.col), 3)
        self.assertEqual(
            strip_system_keys(self.col.last(1)),
            [{"name": "test_doc_03"}]
        )
        docs = strip_system_keys(self.col.last(2))
        self.assertIn({"name": "test_doc_03"}, docs)
        self.assertIn({"name": "test_doc_02"}, docs)

    def test_all(self):
        self.assertEqual(list(self.col.all()), [])
        self.col.insert_many([
            {"name": "test_doc_01"},
            {"name": "test_doc_02"},
            {"name": "test_doc_03"}
        ])
        self.assertEqual(len(self.col), 3)

        docs = strip_system_keys(self.col.all())
        self.assertIn({"name": "test_doc_01"}, docs)
        self.assertIn({"name": "test_doc_02"}, docs)
        self.assertIn({"name": "test_doc_03"}, docs)

    def test_any(self):
        self.assertEqual(strip_system_keys(self.col.all()), [])
        self.col.insert_many([
            {"name": "test_doc_01"},
            {"name": "test_doc_02"},
            {"name": "test_doc_03"}
        ])
        self.assertIn(
            strip_system_keys(self.col.random()),
            [
                {"name": "test_doc_01"},
                {"name": "test_doc_02"},
                {"name": "test_doc_03"}
            ]
        )

    def test_get_first_example(self):
        self.assertEqual(
            self.col.find_one({"value": 1}), None
        )
        self.col.insert_many([
            {"name": "test_doc_01", "value": 1},
            {"name": "test_doc_02", "value": 1},
            {"name": "test_doc_03", "value": 3}
        ])
        self.assertIn(
            strip_system_keys(self.col.find_one({"value": 1})),
            [
                {"name": "test_doc_01", "value": 1},
                {"name": "test_doc_02", "value": 1}
            ]
        )

    def test_get_by_example(self):
        self.col.insert_many([
            {"name": "test_doc_01", "value": 1},
            {"name": "test_doc_02", "value": 1},
            {"name": "test_doc_03", "value": 3}
        ])
        docs = strip_system_keys(self.col.find_many({"value": 1}))
        self.assertIn({"name": "test_doc_01", "value": 1}, docs)
        self.assertIn({"name": "test_doc_02", "value": 1}, docs)
        self.assertEqual(
            strip_system_keys(self.col.find_many({"value": 2})), []
        )
        self.assertTrue(
            len(list(self.col.find_many({"value": 1}, limit=1))), 1
        )

    def test_update_by_example(self):
        self.col.insert_many([
            {"name": "test_doc_01", "value": 1},
            {"name": "test_doc_02", "value": 1},
            {"name": "test_doc_03", "value": 3}
        ])
        self.col.find_and_update({"value": 1}, {"value": 2})
        docs = strip_system_keys(self.col.all())
        self.assertIn({"name": "test_doc_01", "value": 2}, docs)
        self.assertIn({"name": "test_doc_02", "value": 2}, docs)
        self.assertIn({"name": "test_doc_03", "value": 3}, docs)

    def test_replace_by_example(self):
        self.col.insert_many([
            {"name": "test_doc_01", "value": 1},
            {"name": "test_doc_02", "value": 1},
            {"name": "test_doc_03", "value": 3}
        ])
        self.col.find_and_replace({"value": 1}, {"foo": "bar"})

        docs = strip_system_keys(self.col.all())
        self.assertIn({"foo": "bar"}, docs)
        self.assertIn({"name": "test_doc_03", "value": 3}, docs)

    def test_remove_by_example(self):
        self.col.insert_many([
            {"name": "test_doc_01", "value": 1},
            {"name": "test_doc_02", "value": 1},
            {"name": "test_doc_03", "value": 3}
        ])
        self.col.find_and_delete({"value": 1})
        self.col.find_and_delete({"value": 2})
        self.assertEqual(
            strip_system_keys(self.col.all()),
            [{"name": "test_doc_03", "value": 3}]
        )

    def test_range(self):
        self.col.insert_many([
            {"name": "test_doc_01", "value": 1},
            {"name": "test_doc_02", "value": 2},
            {"name": "test_doc_03", "value": 3},
            {"name": "test_doc_04", "value": 4},
            {"name": "test_doc_05", "value": 5}
        ])
        self.assertEqual(
            strip_system_keys(
                self.col.range(
                    attribute="value",
                    left=2,
                    right=5,
                    closed=True,
                    skip=1,
                    limit=2
                )
            ),
            [
                {"name": "test_doc_03", "value": 3},
                {"name": "test_doc_04", "value": 4},
            ]
        )

    def test_near(self):
        self.col.insert_many([
            {"name": "test_doc_01", "coord": [1, 1]},
            {"name": "test_doc_02", "coord": [1, 4]},
            {"name": "test_doc_03", "coord": [4, 1]},
            {"name": "test_doc_03", "coord": [4, 4]},
        ])
        self.assertEqual(
            strip_system_keys(
                self.col.near(
                    latitude=1,
                    longitude=1,
                    limit=1,
                )
            ),
            [
                {"name": "test_doc_01", "coord": [1, 1]}
            ]
        )

    def test_fulltext(self):
        self.col.insert_many([
            {"name": "test_doc_01", "text": "Hello World!"},
            {"name": "test_doc_02", "text": "foo"},
            {"name": "test_doc_03", "text": "bar"},
            {"name": "test_doc_03", "text": "baz"},
        ])
        self.assertEqual(
            strip_system_keys(self.col.find_text("text", "foo,|bar")),
            [
                {"name": "test_doc_02", "text": "foo"},
                {"name": "test_doc_03", "text": "bar"},
            ]
        )

    def test_lookup_by_keys(self):
        self.col.insert_many([
            {"_key": "key01", "value": 1},
            {"_key": "key02", "value": 2},
            {"_key": "key03", "value": 3},
            {"_key": "key04", "value": 4},
            {"_key": "key05", "value": 5},
            {"_key": "key06", "value": 6},
        ])
        self.assertEqual(
            strip_system_keys(
                self.col.get_many(["key01", "key03", "key06"])
            ),
            [
                {"value": 1},
                {"value": 3},
                {"value": 6},
            ]
        )
        self.assertEqual(len(self.col), 6)

    def test_remove_by_keys(self):
        self.col.insert_many([
            {"_key": "key01", "value": 1},
            {"_key": "key02", "value": 2},
            {"_key": "key03", "value": 3},
            {"_key": "key04", "value": 4},
            {"_key": "key05", "value": 5},
            {"_key": "key06", "value": 6},
        ])
        self.assertEqual(
            self.col.delete_many(["key01", "key03", "key06"]),
            {"removed": 3, "ignored": 0}
        )
        leftover = strip_system_keys(self.col.all())
        self.assertEqual(len(leftover), 3)
        self.assertIn({"value": 2}, leftover)
        self.assertIn({"value": 4}, leftover)
        self.assertIn({"value": 5}, leftover)


if __name__ == "__main__":
    unittest.main()
