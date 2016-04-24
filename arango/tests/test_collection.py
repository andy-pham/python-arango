from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from arango import Arango
from arango.constants import COLLECTION_STATUSES
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name
)


def setup_module(*_):
    global driver, db_name, db, col_name, collection

    driver = Arango()
    db_name = generate_db_name(driver)
    db = driver.create_database(db_name)
    col_name = generate_col_name(db)
    collection = db.create_collection(col_name)


def teardown_module(*_):
    driver.drop_database(db_name, ignore_missing=True)


def test_properties():
    assert collection.name == col_name
    assert collection.database == db_name
    assert repr(collection) == "<ArangoDB collection '{}'>".format(col_name)


def test_rename():
    assert collection.name == col_name
    new_col_name = generate_col_name(driver)

    result = collection.rename(new_col_name)
    assert result is True
    assert collection.name == new_col_name
    assert collection.database == db_name
    assert repr(collection) == "<ArangoDB collection '{}'>".format(new_col_name)

    # Try again (the operation should be idempotent)
    result = collection.rename(new_col_name)
    assert result is True
    assert collection.name == new_col_name
    assert collection.database == db_name
    assert repr(collection) == "<ArangoDB collection '{}'>".format(new_col_name)


def test_statistics():
    stats = collection.statistics()
    assert 'alive' in stats
    assert 'compactors' in stats
    assert 'dead' in stats
    assert 'document_refs' in stats
    assert 'journals' in stats


def test_revision():
    revision = collection.revision()
    assert isinstance(revision, string_types)


def test_options():
    options = collection.options()
    assert 'id' in options
    assert options['status'] in COLLECTION_STATUSES.values()
    assert options['name'] == col_name
    assert options['edge'] == False
    assert options['system'] == False
    assert isinstance(options['sync'], bool)
    assert isinstance(options['compact'], bool)
    assert isinstance(options['volatile'], bool)
    assert isinstance(options['journal_size'], int)
    assert options['keygen'] in ('autoincrement', 'traditional')
    assert isinstance(options['user_keys'], bool)
    if 'key_increment' in options:
        assert isinstance(options['key_increment'], int)
    if 'key_offset' in options:
        assert isinstance(options['key_offset'], int)


def test_set_options():
    options = collection.options()
    old_sync = options['sync']
    old_journal_size = options['journal_size']

    new_sync = not old_sync
    new_journal_size = old_journal_size + 1
    result = collection.set_options(
        sync=new_sync, journal_size=new_journal_size
    )
    assert isinstance(result, bool)
    new_options = collection.options()
    assert new_options['sync'] == new_sync
    assert new_options['journal_size'] == new_journal_size


def test_load():
    status = collection.load()
    assert status in ('loaded', 'loading')


def test_unload():
    status = collection.unload()
    assert status in ('unloaded', 'unloading')


def test_rotate():
    # No journal should exist yet
    with pytest.raises(CollectionRotateError):
        collection.rotate()


def test_checksum():
    assert collection.checksum(revision=True, data=False) == 0
    assert collection.checksum(revision=True, data=True) == 0
    assert collection.checksum(revision=False, data=False) == 0
    assert collection.checksum(revision=False, data=True) == 0

    collection.insert({'foo': 'bar'})
    assert collection.checksum(revision=True, data=False) > 0
    assert collection.checksum(revision=True, data=True) > 0
    assert collection.checksum(revision=False, data=False) > 0
    assert collection.checksum(revision=False, data=True) > 0


def test_truncate():
    collection.insert({'foo': 'bar'})
    collection.insert({'foo': 'bar'})
    assert len(collection) > 1

    result = collection.truncate()
    assert isinstance(result, bool)
    assert len(collection) == 0


def test_insert():
    collection.truncate()
    assert len(collection) == 0
    collection.insert({'_key': '1', 'value': 100})
    collection.insert({'_key': '2', 'value': 200})
    collection.insert({'_key': '3', 'value': 300})
    collection.insert({'_key': '4', 'value': 400})
    collection.insert({'_key': '5', 'value': 500})
    assert len(collection) == 5
    for key in range(1, 6):
        assert key in collection
        document = collection.get(key)
        assert document['_key'] == str(key)
        assert document['value'] == key * 100


def test_insert_many():
    collection.truncate()
    assert len(collection) == 0
    collection.insert_many([
        {'_key': '1', 'value': 100},
        {'_key': '2', 'value': 200},
        {'_key': '3', 'value': 300},
        {'_key': '4', 'value': 400},
        {'_key': '5', 'value': 500},
    ])
    assert len(collection) == 5
    for key in range(1, 6):
        assert key in collection
        document = collection.get(key)
        assert document['_key'] == str(key)
        assert document['value'] == key * 100


def test_get():
    collection.truncate()
    assert collection.get(1) is None

    collection.insert({'_key': '1', 'value': 100})
    document = collection.get('1')
    assert document is not None
    assert document['_key'] == '1'
    assert document['value'] == 100


def test_get_many():
    collection.truncate()
    assert collection.get_many(['1', '2', '3', '4', '5']) == []
    expected = [
        {'_key': '1', 'value': 100},
        {'_key': '2', 'value': 200},
        {'_key': '3', 'value': 300},
        {'_key': '4', 'value': 400},
        {'_key': '5', 'value': 500},
    ]
    collection.insert_many(expected)
    assert collection.get_many(['1', '2', '3', '4', '5']) == expected


def test_list_indexes():
    expected_index = {
        "selectivity_estimate": 1,
        "sparse": False,
        "type": "primary",
        "fields": ["_key"],
        "unique": True
    }
    indexes = collection.list_indexes()
    assert isinstance(indexes, dict)
    assert expected_index in indexes.values()


def test_create_hash_index():
    collection.add_hash_index(["attr1", "attr2"], unique=True)
    expected_index = {
        "selectivity_estimate": 1,
        "sparse": False,
        "type": "hash",
        "fields": ["attr1", "attr2"],
        "unique": True
    }
    assert expected_index in collection.list_indexes().values()


def test_create_cap_constraint():
    collection.add_cap_constraint(size=10, byte_size=40000)
    expected_index = {
        "type": "cap",
        "size": 10,
        "byte_size": 40000,
        "unique": False
    }
    assert expected_index in collection.list_indexes().values()


def test_create_skiplist_index():
    collection.add_skiplist_index(["attr1", "attr2"], unique=True)
    expected_index = {
        "sparse": False,
        "type": "skiplist",
        "fields": ["attr1", "attr2"],
        "unique": True
    }
    assert expected_index in collection.list_indexes().values()


def test_create_geo_index():
    # With one attribute
    collection.add_geo_index(
        fields=["attr1"],
        geo_json=False,
    )
    expected_index = {
        "sparse": True,
        "type": "geo1",
        "fields": ["attr1"],
        "unique": False,
        "geo_json": False,
        "ignore_none": True,
        "constraint": False
    }
    assert expected_index in collection.list_indexes().values()

    # With two attributes
    collection.add_geo_index(
        fields=["attr1", "attr2"],
        geo_json=False,
    )
    expected_index = {
        "sparse": True,
        "type": "geo2",
        "fields": ["attr1", "attr2"],
        "unique": False,
        "ignore_none": True,
        "constraint": False
    }
    assert expected_index in collection.list_indexes().values()

    # With more than two attributes (should fail)
    with pytest.raises(IndexCreateError):
        collection.add_geo_index(fields=["attr1", "attr2", "attr3"])


def test_fulltext_index():
    # With two attributes (should fail)
    with pytest.raises(IndexCreateError):
        collection.add_fulltext_index(fields=["attr1", "attr2"])

    collection.add_fulltext_index(
        fields=["attr1"],
        min_length=10,
    )
    expected_index = {
        "sparse": True,
        "type": "fulltext",
        "fields": ["attr1"],
        "min_length": 10,
        "unique": False,
    }
    assert expected_index in collection.list_indexes().values()


def test_delete_index():
    old_indexes = set(collection.list_indexes())
    collection.add_hash_index(["attr1", "attr2"], unique=True)
    collection.add_skiplist_index(["attr1", "attr2"], unique=True)
    collection.add_fulltext_index(fields=["attr1"], min_length=10)

    new_indexes = set(collection.list_indexes())
    assert new_indexes.issuperset(old_indexes)

    for index_id in new_indexes - old_indexes:
        collection.delete_index(index_id)
    assert set(collection.list_indexes()) == old_indexes



