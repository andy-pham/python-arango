from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from arango.connection import Connection
from arango.constants import COLLECTION_STATUSES
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name,
    clean_keys
)


def setup_module(*_):
    global conn, db_name, db, col_name, collection

    conn = Connection()
    db_name = generate_db_name(conn)
    db = conn.create_database(db_name)
    col_name = generate_col_name(db)
    collection = db.create_collection(col_name)
    collection.add_geo_index(['coordinates'])


def teardown_module(*_):
    conn.drop_database(db_name, ignore_missing=True)


def setup_function(*_):
    collection.truncate()
    # for index_id in collection.list_indexes():
    #     collection.delete_index(index_id)


def test_properties():
    assert collection.name == col_name
    assert repr(collection) == "<ArangoDB collection '{}'>".format(col_name)


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


def test_rename():
    assert collection.name == col_name
    new_name = generate_col_name(db)

    result = collection.rename(new_name)
    assert result is True
    assert collection.name == new_name
    assert repr(collection) == "<ArangoDB collection '{}'>".format(new_name)

    # Try again (the operation should be idempotent)
    result = collection.rename(new_name)
    assert result is True
    assert collection.name == new_name
    assert repr(collection) == "<ArangoDB collection '{}'>".format(new_name)


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


def test_insert():
    for i in range(1, 6):
        doc = collection.insert_one({'_key': str(i), 'foo': i * 100})
        assert doc['_id'] == '{}/{}'.format(collection.name, str(i))
        assert doc['_key'] == str(i)

    assert len(collection) == 5
    for key in range(1, 6):
        assert key in collection
        document = collection.get_one(key)
        assert document['_key'] == str(key)
        assert document['foo'] == key * 100

    assert '6' not in collection
    collection.insert_one({'_key': '6', 'foo': 200}, sync=True)
    assert '6' in collection
    assert collection.get_one('6')['foo'] == 200

    with pytest.raises(DocumentInsertError):
        collection.insert_one({'_key': '1', 'foo': 300})
    assert collection['1']['foo'] == 100


def test_insert_many():
    result = collection.insert_many([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
        {'_key': '4', 'foo': 400},
        {'_key': '5', 'foo': 500},
    ])
    assert result['created'] == 5
    assert result['errors'] == 0
    assert 'details' in result
    assert len(collection) == 5
    for key in range(1, 6):
        assert key in collection
        document = collection.get_one(key)
        assert document['_key'] == str(key)
        assert document['foo'] == key * 100

    with pytest.raises(DocumentInsertError):
        collection.insert_many([
            {'_key': '1', 'foo': 100},
            {'_key': '1', 'foo': 200},
            {'_key': '1', 'foo': 300},
        ])

    result = collection.insert_many([
        {'_key': '6', 'foo': 100},
        {'_key': '7', 'foo': 200},
        {'_key': '8', 'foo': 300},
    ], details=False)
    assert 'details' not in result


def test_get():
    collection.insert_one({'_key': '1', 'foo': 100})
    doc = collection.get_one('1')
    assert doc['foo'] == 100

    old_rev = doc['_rev']
    new_rev = str(int(old_rev) + 1)

    assert collection.get_one('2') is None
    assert collection.get_one('1', revision=old_rev) == doc

    with pytest.raises(DocumentRevisionError):
        collection.get_one('1', revision=new_rev)


def test_get_many():
    assert collection.get_many(['1', '2', '3', '4', '5']) == []
    expected = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
        {'_key': '4', 'foo': 400},
        {'_key': '5', 'foo': 500},
    ]
    collection.insert_many(expected)
    assert collection.get_many([]) == []
    assert expected == [
        {'_key': doc['_key'], 'foo': doc['foo']}
        for doc in collection.get_many(['1', '2', '3', '4', '5'])
    ]
    assert expected == [
        {'_key': doc['_key'], 'foo': doc['foo']}
        for doc in collection.get_many(['1', '2', '3', '4', '5', '6'])
    ]


def test_update():
    collection.insert_one({'_key': '1', 'foo': 100})
    assert collection['1']['foo'] == 100

    doc = collection.update_one('1', {'foo': 200})
    assert doc['_id'] == '{}/1'.format(collection.name)
    assert doc['_key'] == '1'
    assert collection['1']['foo'] == 200

    doc = collection.update_one('1', {'foo': None}, keep_none=True)
    assert doc['_id'] == '{}/1'.format(collection.name)
    assert doc['_key'] == '1'
    assert collection['1']['foo'] is None
    
    doc = collection.update_one('1', {'foo': {'bar': 1}}, sync=True)
    assert doc['_id'] == '{}/1'.format(collection.name)
    assert doc['_key'] == '1'
    assert collection['1']['foo'] == {'bar': 1}

    doc = collection.update_one('1', {'foo': {'baz': 2}}, merge=True)
    assert doc['_id'] == '{}/1'.format(collection.name)
    assert doc['_key'] == '1'
    assert collection['1']['foo'] == {'bar': 1, 'baz': 2}

    doc = collection.update_one('1', {'foo': None}, keep_none=False)
    assert doc['_id'] == '{}/1'.format(collection.name)
    assert doc['_key'] == '1'
    assert 'foo' not in collection['1']

    old_rev = doc['_rev']
    new_rev = str(int(old_rev) + 1)

    with pytest.raises(DocumentRevisionError):
        collection.update_one('1', {'foo': 300, '_rev': new_rev})
    assert 'foo' not in collection['1']

    with pytest.raises(DocumentUpdateError):
        collection.update_one('2', {'foo': 300})
    assert 'foo' not in collection['1']


def test_replace():
    doc = collection.insert_one({'_key': '1', 'foo': 100})
    assert doc['_id'] == '{}/1'.format(collection.name)
    assert doc['_key'] == '1'
    assert collection['1']['foo'] == 100

    doc = collection.replace('1', {'foo': 200})
    assert doc['_id'] == '{}/1'.format(collection.name)
    assert doc['_key'] == '1'
    assert collection['1']['foo'] == 200

    doc = collection.replace('1', {'foo': 300}, sync=True)
    assert doc['_id'] == '{}/1'.format(collection.name)
    assert doc['_key'] == '1'
    assert collection['1']['foo'] == 300

    doc = collection.replace('1', {'foo': 400}, revision=doc['_rev'])
    assert doc['_id'] == '{}/1'.format(collection.name)
    assert doc['_key'] == '1'
    assert collection['1']['foo'] == 400

    old_rev = doc['_rev']
    new_rev = str(int(old_rev) + 1)

    with pytest.raises(DocumentRevisionError):
        collection.replace('1', {'foo': 500, '_rev': new_rev})
    assert collection['1']['foo'] == 400

    with pytest.raises(DocumentReplaceError):
        collection.replace('2', {'foo': 600})
    assert collection['1']['foo'] == 400


def test_delete():
    collection.insert_many([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
    ])

    doc = collection.delete('1')
    assert doc['id'] == '{}/1'.format(collection.name)
    assert doc['key'] == '1'
    assert '1' not in collection
    assert len(collection) == 2

    doc = collection.delete('2', sync=True)
    assert doc['id'] == '{}/2'.format(collection.name)
    assert doc['key'] == '2'
    assert '2' not in collection
    assert len(collection) == 1

    old_rev = collection['3']['_rev']
    new_rev = str(int(old_rev) + 1)

    with pytest.raises(DocumentRevisionError):
        collection.delete('3', revision=new_rev)
    assert '3' in collection
    assert len(collection) == 1

    assert collection.delete('4') == False
    with pytest.raises(DocumentDeleteError):
        collection.delete('4', ignore_missing=False)
    assert len(collection) == 1


def test_delete_many():
    result = collection.delete_many(['1', '2', '3'])
    assert result['removed'] == 0
    assert result['ignored'] == 3

    collection.insert_many([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
    ])
    result = collection.delete_many([])
    assert result['removed'] == 0
    assert result['ignored'] == 0
    for key in ['1', '2', '3']:
        assert key in collection

    result = collection.delete_many(['1'])
    assert result['removed'] == 1
    assert result['ignored'] == 0
    assert '1' not in collection
    assert len(collection) == 2

    result = collection.delete_many(['4'])
    assert result['removed'] == 0
    assert result['ignored'] == 1
    assert '2' in collection and '3' in collection
    assert len(collection) == 2

    result = collection.delete_many(['1', '2', '3'])
    assert result['removed'] == 2
    assert result['ignored'] == 1
    assert len(collection) == 0


def test_first():
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
    ]
    collection.insert_many(inserted)
    doc = collection.first(0)
    assert doc['_key'] == '1'
    assert doc['foo'] == 100

    docs = collection.first(1)
    assert len(docs) == 1
    assert docs[0]['_key'] == '1'
    assert docs[0]['foo'] == 100

    docs = collection.first(2)
    assert len(docs) == 2
    assert docs[0]['_key'] == '1'
    assert docs[0]['foo'] == 100
    assert docs[1]['_key'] == '2'
    assert docs[1]['foo'] == 200

    docs = collection.first(10)
    assert len(docs) == 3
    for doc in [{'_key': doc['_key'], 'foo': doc['foo']} for doc in docs]:
        assert doc in inserted
    with pytest.raises(DocumentGetFirstError):
        assert collection.first(-1)


def test_last():
    inserted = [
        {'_key': '3', 'foo': 300},
        {'_key': '2', 'foo': 200},
        {'_key': '1', 'foo': 100},
    ]
    for doc in inserted:
        collection.insert_one(doc)
    doc = collection.last(0)
    assert doc['_key'] == '1'
    assert doc['foo'] == 100

    docs = collection.last(1)
    assert len(docs) == 1
    assert docs[0]['_key'] == '1'
    assert docs[0]['foo'] == 100

    docs = collection.last(2)
    assert len(docs) == 2
    assert docs[0]['_key'] == '1'
    assert docs[0]['foo'] == 100
    assert docs[1]['_key'] == '2'
    assert docs[1]['foo'] == 200

    docs = collection.last(10)
    assert len(docs) == 3
    for doc in [{'_key': doc['_key'], 'foo': doc['foo']} for doc in docs]:
        assert doc in inserted
    with pytest.raises(DocumentGetLastError):
        assert collection.last(-1)


def test_all():
    assert len(list(collection.all())) == 0
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
        {'_key': '4', 'foo': 400},
        {'_key': '5', 'foo': 500},
    ]
    for doc in inserted:
        collection.insert_one(doc)
    fetched = list(collection.all())
    assert len(fetched) == len(inserted)
    for doc in fetched:
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted

    # TODO ordering is strange
    assert len(list(collection.all(skip=5))) == 0
    fetched = list(collection.all(skip=3))
    assert len(fetched) == 2

    # TODO ordering is strange
    assert len(list(collection.all(limit=0))) == 0
    fetched = list(collection.all(limit=2))
    assert len(fetched) == 2


def test_random():
    assert len(list(collection.all())) == 0
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
        {'_key': '4', 'foo': 400},
        {'_key': '5', 'foo': 500},
    ]
    collection.insert_many(inserted)
    for attempt in range(10):
        doc = collection.random()
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted


def test_find_one():
    assert collection.find_one({'foo': 100}) is None
    assert collection.find_one({}) is None
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ]
    collection.insert_many(inserted)

    assert collection.find_one({'_key': '6'}) is None
    assert collection.find_one({'foo': 400}) is None
    assert collection.find_one({'baz': 100}) is None
    assert collection.find_one({}) is not None

    for i in [100, 200, 300]:
        assert collection.find_one({'foo': i})['foo'] == i
    for i in range(1, 6):
        assert collection.find_one({'_key': str(i)})['_key'] == str(i)


def test_find_many():
    assert list(collection.find_many({'foo': 100})) == []
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ]
    collection.insert_many(inserted)

    found = list(collection.find_many({'foo': 100}))
    assert len(found) == 3
    for doc in found:
        assert doc['_key'] in ['1', '2', '3']
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted

    found = list(collection.find_many({'foo': 100}, skip=1))
    assert len(found) == 2
    for doc in found:
        assert doc['_key'] in ['1', '2', '3']
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted

    found = list(collection.find_many({}, limit=4))
    assert len(found) == 4
    for doc in found:
        assert doc['_key'] in ['1', '2', '3', '4', '5']
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted

    found = list(collection.find_many({'foo': 200}))
    assert len(found) == 1
    assert found[0]['_key'] == '4'


def test_find_and_update():
    assert collection.update_matches({'foo': 100}, {'bar': 100}) == 0
    collection.insert_many([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ])

    assert collection.update_matches({'foo': 200}, {'bar': 100}) == 1
    assert collection['4']['foo'] == 200
    assert collection['4']['bar'] == 100

    assert collection.update_matches({'foo': 100}, {'bar': 100}) == 3
    for key in ['1', '2', '3']:
        assert collection[key]['foo'] == 100
        assert collection[key]['bar'] == 100

    assert collection['5']['foo'] == 300
    assert 'bar' not in collection['5']

    assert collection.update_matches(
        {'foo': 300}, {'foo': None}, sync=True, keep_none=True
    ) == 1
    assert collection['5']['foo'] is None
    assert collection.update_matches(
        {'foo': 200}, {'foo': None}, sync=True, keep_none=False
    ) == 1
    assert 'foo' not in collection['4']


def test_find_and_replace():
    assert collection.replace_matches({'foo': 100}, {'bar': 100}) == 0
    collection.insert_many([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ])

    assert collection.replace_matches({'foo': 200}, {'bar': 100}) == 1
    assert 'foo' not in collection['4']
    assert collection['4']['bar'] == 100

    assert collection.replace_matches({'foo': 100}, {'bar': 100}) == 3
    for key in ['1', '2', '3']:
        assert 'foo' not in collection[key]
        assert collection[key]['bar'] == 100

    assert collection['5']['foo'] == 300
    assert 'bar' not in collection['5']


def test_find_and_delete():
    assert collection.delete_matches({'foo': 100}) == 0
    collection.insert_many([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ])

    assert collection.delete_matches({'foo': 200}) == 1
    assert '4' not in collection

    assert collection.delete_matches({'foo': 300}, sync=True) == 1
    assert '4' not in collection

    assert collection.delete_matches({'foo': 100}, limit=2) == 2
    count = 0
    for key in ['1', '2', '3']:
        if key in collection:
            assert collection[key]['foo'] == 100
            count += 1
    assert count == 1


def test_find_near():
    collection.insert_many([
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '4', 'coordinates': [4, 4]},
        {'_key': '2', 'coordinates': [2, 2]},
        {'_key': '3', 'coordinates': [3, 3]},
    ])
    result = collection.find_near(
        latitude=1,
        longitude=1,
        limit=2
    )
    expected = [
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '2', 'coordinates': [2, 2]}
    ]
    assert clean_keys(list(result)) == expected

    result = collection.find_near(
        latitude=4,
        longitude=4,
    )
    expected = [
        {'_key': '4', 'coordinates': [4, 4]},
        {'_key': '3', 'coordinates': [3, 3]},
        {'_key': '2', 'coordinates': [2, 2]},
        {'_key': '1', 'coordinates': [1, 1]},
    ]
    assert clean_keys(list(result)) == expected


def test_find_in_range():
    collection.add_skiplist_index(['value'])
    collection.insert_many([
        {'_key': '1', 'value': 1},
        {'_key': '2', 'value': 2},
        {'_key': '3', 'value': 3},
        {'_key': '4', 'value': 4},
        {'_key': '5', 'value': 5}
    ])
    result = collection.find_in_range(
        field='value',
        lower=2,
        upper=5,
        skip=1,
        limit=2,
    )
    expected = [
        {'_key': '3', 'value': 3},
        {'_key': '4', 'value': 4},
    ]
    assert clean_keys(list(result)) == expected


# TODO the WITHIN geo function does not seem to work properly
def test_find_in_radius():
    collection.insert_many([
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '2', 'coordinates': [1, 4]},
        {'_key': '3', 'coordinates': [4, 1]},
        {'_key': '4', 'coordinates': [4, 4]},
    ])
    result = list(collection.find_in_radius(3, 3, 10, 'distance'))
    for doc in result:
        assert 'distance' in doc


def test_find_in_rectangle():
    collection.insert_many([
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '2', 'coordinates': [1, 5]},
        {'_key': '3', 'coordinates': [5, 1]},
        {'_key': '4', 'coordinates': [5, 5]},
    ])
    result = collection.find_in_rectangle(
        latitude1=0,
        longitude1=0,
        latitude2=6,
        longitude2=3
    )
    expected = [
        {'_key': '3', 'coordinates': [5, 1]},
        {'_key': '1', 'coordinates': [1, 1]}
    ]
    assert clean_keys(list(result)) == expected

    result = collection.find_in_rectangle(
        latitude1=0,
        longitude1=0,
        latitude2=6,
        longitude2=3,
        limit=1
    )
    expected = [
        {'_key': '3', 'coordinates': [5, 1]}
    ]
    assert clean_keys(list(result)) == expected

    result = collection.find_in_rectangle(
        latitude1=0,
        longitude1=0,
        latitude2=6,
        longitude2=3,
        skip=1
    )
    expected = [
        {'_key': '1', 'coordinates': [1, 1]}
    ]
    assert clean_keys(list(result)) == expected


def test_find_text():
    collection.add_fulltext_index(['text'])
    collection.insert_many([
        {'_key': '1', 'text': 'foo'},
        {'_key': '2', 'text': 'bar'},
        {'_key': '3', 'text': 'baz'}
    ])
    result = collection.find_text(
        field='text', query='foo,|bar'
    )
    expected = [
        {'_key': '1', 'text': 'foo'},
        {'_key': '2', 'text': 'bar'}
    ]
    assert clean_keys(list(result)) == expected

    # Bad parameter
    with pytest.raises(DocumentFindTextError):
        collection.find_text(field='text', query='+')

    with pytest.raises(DocumentFindTextError):
        collection.find_text(field='text', query='|')


def test_list_indexes():
    expected_index = {
        'selectivity': 1,
        'sparse': False,
        'type': 'primary',
        'fields': ['_key'],
        'unique': True
    }
    indexes = collection.list_indexes()
    assert isinstance(indexes, dict)
    assert expected_index in indexes.values()


def test_add_hash_index():
    collection.add_hash_index(['attr1', 'attr2'], unique=True)
    expected_index = {
        'selectivity': 1,
        'sparse': False,
        'type': 'hash',
        'fields': ['attr1', 'attr2'],
        'unique': True
    }
    assert expected_index in collection.list_indexes().values()


def test_add_cap_constraint():
    collection.add_cap_constraint(size=10, byte_size=40000)
    expected_index = {
        'type': 'cap',
        'size': 10,
        'byte_size': 40000,
        'unique': False
    }
    assert expected_index in collection.list_indexes().values()


def test_add_skiplist_index():
    collection.add_skiplist_index(['attr1', 'attr2'], unique=True)
    expected_index = {
        'sparse': False,
        'type': 'skiplist',
        'fields': ['attr1', 'attr2'],
        'unique': True
    }
    assert expected_index in collection.list_indexes().values()


def test_add_geo_index():
    # With one attribute
    collection.add_geo_index(
        fields=['attr1'],
        geo_json=False,
    )
    expected_index = {
        'sparse': True,
        'type': 'geo1',
        'fields': ['attr1'],
        'unique': False,
        'geo_json': False,
        'ignore_none': True,
        'constraint': False
    }
    assert expected_index in collection.list_indexes().values()

    # With two attributes
    collection.add_geo_index(
        fields=['attr1', 'attr2'],
        geo_json=False,
    )
    expected_index = {
        'sparse': True,
        'type': 'geo2',
        'fields': ['attr1', 'attr2'],
        'unique': False,
        'ignore_none': True,
        'constraint': False
    }
    assert expected_index in collection.list_indexes().values()

    # With more than two attributes (should fail)
    with pytest.raises(IndexCreateError):
        collection.add_geo_index(fields=['attr1', 'attr2', 'attr3'])


def test_add_fulltext_index():
    # With two attributes (should fail)
    with pytest.raises(IndexCreateError):
        collection.add_fulltext_index(fields=['attr1', 'attr2'])

    collection.add_fulltext_index(
        fields=['attr1'],
        min_length=10,
    )
    expected_index = {
        'sparse': True,
        'type': 'fulltext',
        'fields': ['attr1'],
        'min_length': 10,
        'unique': False,
    }
    assert expected_index in collection.list_indexes().values()


def test_delete_index():
    old_indexes = set(collection.list_indexes())
    collection.add_hash_index(['attr1', 'attr2'], unique=True)
    collection.add_skiplist_index(['attr1', 'attr2'], unique=True)
    collection.add_fulltext_index(fields=['attr1'], min_length=10)

    new_indexes = set(collection.list_indexes())
    assert new_indexes.issuperset(old_indexes)

    for index_id in new_indexes - old_indexes:
        collection.delete_index(index_id)
    assert set(collection.list_indexes()) == old_indexes
