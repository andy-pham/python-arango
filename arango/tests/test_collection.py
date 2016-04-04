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

    collection.insert_one({'foo': 'bar'})
    assert collection.checksum(revision=True, data=False) > 0
    assert collection.checksum(revision=True, data=True) > 0
    assert collection.checksum(revision=False, data=False) > 0
    assert collection.checksum(revision=False, data=True) > 0


def test_truncate():
    collection.insert_one({'foo': 'bar'})
    collection.insert_one({'foo': 'bar'})
    assert len(collection) > 1

    result = collection.truncate()
    assert isinstance(result, bool)
    assert len(collection) == 0


def test_index_management():
    pass
