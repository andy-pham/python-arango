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

conn = Connection()
db_name = generate_db_name(conn)
db = conn.create_database(db_name)
ecol_name = generate_col_name(db)
ecol = db.create_collection(ecol_name, edge=True)
ecol.add_geo_index(['coordinates'])

# Set up test collection and edges
col = generate_col_name(db)
db.create_collection(col).insert_many([
    {'_key': '1'}, {'_key': '2'}, {'_key': '3'}, {'_key': '4'}
])
edge1 = {'_key': '1', '_from': '{}/1'.format(col), '_to': '{}/2'.format(col)}
edge2 = {'_key': '2', '_from': '{}/2'.format(col), '_to': '{}/3'.format(col)}
edge3 = {'_key': '3', '_from': '{}/3'.format(col), '_to': '{}/4'.format(col)}
edge4 = {'_key': '4', '_from': '{}/4'.format(col), '_to': '{}/1'.format(col)}


def teardown_module(*_):
    conn.drop_database(db_name, ignore_missing=True)


def setup_function(*_):
    ecol.truncate()


def test_properties():
    assert ecol.name == ecol_name
    assert repr(ecol) == ("<ArangoDB edge collection '{}'>".format(ecol_name))


def test_options():
    options = ecol.options()
    assert 'id' in options
    assert options['status'] in COLLECTION_STATUSES.values()
    assert options['name'] == ecol_name
    assert options['edge'] == True
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
    options = ecol.options()
    old_sync = options['sync']
    old_journal_size = options['journal_size']

    new_sync = not old_sync
    new_journal_size = old_journal_size + 1
    result = ecol.set_options(
        sync=new_sync, journal_size=new_journal_size
    )
    assert isinstance(result, bool)
    new_options = ecol.options()
    assert new_options['sync'] == new_sync
    assert new_options['journal_size'] == new_journal_size


def test_rename():
    assert ecol.name == ecol_name
    new_name = generate_col_name(db)

    result = ecol.rename(new_name)
    assert result is True
    assert ecol.name == new_name
    assert repr(ecol) == "<ArangoDB edge collection '{}'>".format(new_name)

    # Try again (the operation should be idempotent)
    result = ecol.rename(new_name)
    assert result is True
    assert ecol.name == new_name
    assert repr(ecol) == "<ArangoDB edge collection '{}'>".format(new_name)


def test_statistics():
    stats = ecol.statistics()
    assert 'alive' in stats
    assert 'compactors' in stats
    assert 'dead' in stats
    assert 'document_refs' in stats
    assert 'journals' in stats


def test_revision():
    revision = ecol.revision()
    assert isinstance(revision, string_types)


def test_load():
    status = ecol.load()
    assert status in ('loaded', 'loading')


def test_unload():
    status = ecol.unload()
    assert status in ('unloaded', 'unloading')


def test_rotate():
    # No journal should exist yet
    with pytest.raises(CollectionRotateError):
        ecol.rotate()


def test_checksum():
    assert ecol.checksum(revision=True, data=False) == 0
    assert ecol.checksum(revision=True, data=True) == 0
    assert ecol.checksum(revision=False, data=False) == 0
    assert ecol.checksum(revision=False, data=True) == 0

    ecol.insert_one(edge1)
    assert ecol.checksum(revision=True, data=False) > 0
    assert ecol.checksum(revision=True, data=True) > 0
    assert ecol.checksum(revision=False, data=False) > 0
    assert ecol.checksum(revision=False, data=True) > 0


def test_truncate():
    ecol.insert_one(edge1)
    ecol.insert_one(edge2)
    assert len(ecol) > 1

    result = ecol.truncate()
    assert isinstance(result, bool)
    assert len(ecol) == 0


def test_insert():
    assert '1' not in ecol
    edge = ecol.insert_one(edge1)
    assert edge['_key'] == '1'
    assert '1' in ecol
    assert len(ecol) == 1

    edge = ecol.get_one('1')
    assert edge['_key'] == '1'
    assert edge['_from'] == edge1['_from']
    assert edge['_to'] == edge1['_to']

    assert '2' not in ecol
    edge = ecol.insert_one(edge2, sync=True)
    assert edge['_key'] == '2'
    assert '2' in ecol
    assert len(ecol) == 2

    edge = ecol.get_one('2')
    assert edge['_key'] == '2'
    assert edge['_from'] == edge2['_from']
    assert edge['_to'] == edge2['_to']

    with pytest.raises(DocumentInsertError):
        ecol.insert_one(edge1)

    with pytest.raises(DocumentInsertError):
        ecol.insert_one(edge2)


def test_insert_many():
    result = ecol.insert_many([edge1, edge2, edge3])
    assert result['created'] == 3
    assert result['errors'] == 0
    assert 'details' in result
    assert len(ecol) == 3
    for key in range(1, 4):
        assert key in ecol
        edge = ecol.get_one(key)
        assert edge['_key'] == str(key)

    with pytest.raises(DocumentInsertError):
        ecol.insert_many([edge1, edge2], halt_on_error=True)

    result = ecol.insert_many([edge1, edge2], halt_on_error=False)
    assert result['created'] == 0
    assert result['errors'] == 2
    assert 'details' in result

    result = ecol.insert_many([edge4], details=False)
    assert result['created'] == 1
    assert result['errors'] == 0
    assert 'details' not in result


def test_get():
    ecol.insert_one(edge1)
    edge = ecol.get_one('1')
    assert edge['_key'] == '1'
    assert edge['_from'] == edge1['_from']
    assert edge['_to'] == edge1['_to']
    assert ecol.get_one('2') is None

    old_rev = edge['_rev']
    new_rev = str(int(old_rev) + 1)
    assert ecol.get_one('1', revision=old_rev) == edge

    with pytest.raises(DocumentRevisionError):
        ecol.get_one('1', revision=new_rev)


def test_get_many():
    assert ecol.get_many(['1', '2', '3', '4', '5']) == []
    expected = [edge1, edge2, edge3, edge4]
    ecol.insert_many(expected)
    assert ecol.get_many([]) == []
    assert expected == [
        {'_key': edge['_key'], '_from': edge['_from'], '_to': edge['_to']}
        for edge in ecol.get_many(['1', '2', '3', '4'])
    ]
    assert expected == [
        {'_key': edge['_key'], '_from': edge['_from'], '_to': edge['_to']}
        for edge in ecol.get_many(['1', '2', '3', '4', '5', '6'])
    ]


def test_update():
    ecol.insert_one(edge1)
    edge = ecol.update_one('1', {'foo': 200})
    assert edge['_key'] == '1'
    assert ecol['1']['foo'] == 200

    edge = ecol.update_one('1', {'foo': None}, keep_none=True)
    assert edge['_id'] == '{}/1'.format(ecol.name)
    assert edge['_key'] == '1'
    assert ecol['1']['foo'] is None

    edge = ecol.update_one('1', {'foo': {'bar': 1}}, sync=True)
    assert edge['_id'] == '{}/1'.format(ecol.name)
    assert edge['_key'] == '1'
    assert ecol['1']['foo'] == {'bar': 1}

    edge = ecol.update_one('1', {'foo': {'baz': 2}}, merge=True)
    assert edge['_id'] == '{}/1'.format(ecol.name)
    assert edge['_key'] == '1'
    assert ecol['1']['foo'] == {'bar': 1, 'baz': 2}

    edge = ecol.update_one('1', {'foo': None}, keep_none=False)
    assert edge['_id'] == '{}/1'.format(ecol.name)
    assert edge['_key'] == '1'
    assert 'foo' not in ecol['1']

    old_rev = edge['_rev']
    new_rev = str(int(old_rev) + 1)

    with pytest.raises(DocumentRevisionError):
        ecol.update_one('1', {'foo': 300, '_rev': new_rev})
    assert 'foo' not in ecol['1']

    with pytest.raises(DocumentUpdateError):
        ecol.update_one('2', {'foo': 300})
    assert 'foo' not in ecol['1']


def test_replace():
    doc = ecol.insert_one({'_key': '1', 'foo': 100})
    assert doc['_id'] == '{}/1'.format(ecol.name)
    assert doc['_key'] == '1'
    assert ecol['1']['foo'] == 100

    doc = ecol.replace('1', {'foo': 200})
    assert doc['_id'] == '{}/1'.format(ecol.name)
    assert doc['_key'] == '1'
    assert ecol['1']['foo'] == 200

    doc = ecol.replace('1', {'foo': 300}, sync=True)
    assert doc['_id'] == '{}/1'.format(ecol.name)
    assert doc['_key'] == '1'
    assert ecol['1']['foo'] == 300

    doc = ecol.replace('1', {'foo': 400}, revision=doc['_rev'])
    assert doc['_id'] == '{}/1'.format(ecol.name)
    assert doc['_key'] == '1'
    assert ecol['1']['foo'] == 400

    old_rev = doc['_rev']
    new_rev = str(int(old_rev) + 1)

    with pytest.raises(DocumentRevisionError):
        ecol.replace('1', {'foo': 500, '_rev': new_rev})
    assert ecol['1']['foo'] == 400

    with pytest.raises(DocumentReplaceError):
        ecol.replace('2', {'foo': 600})
    assert ecol['1']['foo'] == 400


def test_delete():
    ecol.insert_many([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
    ])

    doc = ecol.delete('1')
    assert doc['id'] == '{}/1'.format(ecol.name)
    assert doc['key'] == '1'
    assert '1' not in ecol
    assert len(ecol) == 2

    doc = ecol.delete('2', sync=True)
    assert doc['id'] == '{}/2'.format(ecol.name)
    assert doc['key'] == '2'
    assert '2' not in ecol
    assert len(ecol) == 1

    old_rev = ecol['3']['_rev']
    new_rev = str(int(old_rev) + 1)

    with pytest.raises(DocumentRevisionError):
        ecol.delete('3', revision=new_rev)
    assert '3' in ecol
    assert len(ecol) == 1

    assert ecol.delete('4') == False
    with pytest.raises(DocumentDeleteError):
        ecol.delete('4', ignore_missing=False)
    assert len(ecol) == 1


def test_delete_many():
    result = ecol.delete_many(['1', '2', '3'])
    assert result['removed'] == 0
    assert result['ignored'] == 3

    ecol.insert_many([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
    ])
    result = ecol.delete_many([])
    assert result['removed'] == 0
    assert result['ignored'] == 0
    for key in ['1', '2', '3']:
        assert key in ecol

    result = ecol.delete_many(['1'])
    assert result['removed'] == 1
    assert result['ignored'] == 0
    assert '1' not in ecol
    assert len(ecol) == 2

    result = ecol.delete_many(['4'])
    assert result['removed'] == 0
    assert result['ignored'] == 1
    assert '2' in ecol and '3' in ecol
    assert len(ecol) == 2

    result = ecol.delete_many(['1', '2', '3'])
    assert result['removed'] == 2
    assert result['ignored'] == 1
    assert len(ecol) == 0


def test_first():
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
    ]
    ecol.insert_many(inserted)
    doc = ecol.first(0)
    assert doc['_key'] == '1'
    assert doc['foo'] == 100

    docs = ecol.first(1)
    assert len(docs) == 1
    assert docs[0]['_key'] == '1'
    assert docs[0]['foo'] == 100

    docs = ecol.first(2)
    assert len(docs) == 2
    assert docs[0]['_key'] == '1'
    assert docs[0]['foo'] == 100
    assert docs[1]['_key'] == '2'
    assert docs[1]['foo'] == 200

    docs = ecol.first(10)
    assert len(docs) == 3
    for doc in [{'_key': doc['_key'], 'foo': doc['foo']} for doc in docs]:
        assert doc in inserted
    with pytest.raises(DocumentGetFirstError):
        assert ecol.first(-1)


def test_last():
    inserted = [
        {'_key': '3', 'foo': 300},
        {'_key': '2', 'foo': 200},
        {'_key': '1', 'foo': 100},
    ]
    for doc in inserted:
        ecol.insert_one(doc)
    doc = ecol.last(0)
    assert doc['_key'] == '1'
    assert doc['foo'] == 100

    docs = ecol.last(1)
    assert len(docs) == 1
    assert docs[0]['_key'] == '1'
    assert docs[0]['foo'] == 100

    docs = ecol.last(2)
    assert len(docs) == 2
    assert docs[0]['_key'] == '1'
    assert docs[0]['foo'] == 100
    assert docs[1]['_key'] == '2'
    assert docs[1]['foo'] == 200

    docs = ecol.last(10)
    assert len(docs) == 3
    for doc in [{'_key': doc['_key'], 'foo': doc['foo']} for doc in docs]:
        assert doc in inserted
    with pytest.raises(DocumentGetLastError):
        assert ecol.last(-1)


def test_all():
    assert len(list(ecol.all())) == 0
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
        {'_key': '4', 'foo': 400},
        {'_key': '5', 'foo': 500},
    ]
    for doc in inserted:
        ecol.insert_one(doc)
    fetched = list(ecol.all())
    assert len(fetched) == len(inserted)
    for doc in fetched:
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted

    # TODO ordering is strange
    assert len(list(ecol.all(offset=5))) == 0
    fetched = list(ecol.all(offset=3))
    assert len(fetched) == 2

    # TODO ordering is strange
    assert len(list(ecol.all(limit=0))) == 0
    fetched = list(ecol.all(limit=2))
    assert len(fetched) == 2


def test_random():
    assert len(list(ecol.all())) == 0
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
        {'_key': '4', 'foo': 400},
        {'_key': '5', 'foo': 500},
    ]
    ecol.insert_many(inserted)
    for attempt in range(10):
        doc = ecol.random()
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted


def test_find_one():
    assert ecol.find_one({'foo': 100}) is None
    assert ecol.find_one({}) is None
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ]
    ecol.insert_many(inserted)

    assert ecol.find_one({'_key': '6'}) is None
    assert ecol.find_one({'foo': 400}) is None
    assert ecol.find_one({'baz': 100}) is None
    assert ecol.find_one({}) is not None

    for i in [100, 200, 300]:
        assert ecol.find_one({'foo': i})['foo'] == i
    for i in range(1, 6):
        assert ecol.find_one({'_key': str(i)})['_key'] == str(i)


def test_find_many():
    assert list(ecol.find_many({'foo': 100})) == []
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ]
    ecol.insert_many(inserted)

    found = list(ecol.find_many({'foo': 100}))
    assert len(found) == 3
    for doc in found:
        assert doc['_key'] in ['1', '2', '3']
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted

    found = list(ecol.find_many({'foo': 100}, offset=1))
    assert len(found) == 2
    for doc in found:
        assert doc['_key'] in ['1', '2', '3']
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted

    found = list(ecol.find_many({}, limit=4))
    assert len(found) == 4
    for doc in found:
        assert doc['_key'] in ['1', '2', '3', '4', '5']
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted

    found = list(ecol.find_many({'foo': 200}))
    assert len(found) == 1
    assert found[0]['_key'] == '4'


def test_find_and_update():
    assert ecol.find_and_update({'foo': 100}, {'bar': 100}) == 0
    ecol.insert_many([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ])

    assert ecol.find_and_update({'foo': 200}, {'bar': 100}) == 1
    assert ecol['4']['foo'] == 200
    assert ecol['4']['bar'] == 100

    assert ecol.find_and_update({'foo': 100}, {'bar': 100}) == 3
    for key in ['1', '2', '3']:
        assert ecol[key]['foo'] == 100
        assert ecol[key]['bar'] == 100

    assert ecol['5']['foo'] == 300
    assert 'bar' not in ecol['5']

    assert ecol.find_and_update(
        {'foo': 300}, {'foo': None}, sync=True, keep_none=True
    ) == 1
    assert ecol['5']['foo'] is None
    assert ecol.find_and_update(
        {'foo': 200}, {'foo': None}, sync=True, keep_none=False
    ) == 1
    assert 'foo' not in ecol['4']


def test_find_and_replace():
    assert ecol.find_and_replace({'foo': 100}, {'bar': 100}) == 0
    ecol.insert_many([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ])

    assert ecol.find_and_replace({'foo': 200}, {'bar': 100}) == 1
    assert 'foo' not in ecol['4']
    assert ecol['4']['bar'] == 100

    assert ecol.find_and_replace({'foo': 100}, {'bar': 100}) == 3
    for key in ['1', '2', '3']:
        assert 'foo' not in ecol[key]
        assert ecol[key]['bar'] == 100

    assert ecol['5']['foo'] == 300
    assert 'bar' not in ecol['5']


def test_find_and_delete():
    assert ecol.find_and_delete({'foo': 100}) == 0
    ecol.insert_many([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ])

    assert ecol.find_and_delete({'foo': 200}) == 1
    assert '4' not in ecol

    assert ecol.find_and_delete({'foo': 300}, sync=True) == 1
    assert '4' not in ecol

    assert ecol.find_and_delete({'foo': 100}, limit=2) == 2
    count = 0
    for key in ['1', '2', '3']:
        if key in ecol:
            assert ecol[key]['foo'] == 100
            count += 1
    assert count == 1


def test_find_near():
    ecol.insert_many([
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '4', 'coordinates': [4, 4]},
        {'_key': '2', 'coordinates': [2, 2]},
        {'_key': '3', 'coordinates': [3, 3]},
    ])
    result = ecol.find_near(
        latitude=1,
        longitude=1,
        limit=2
    )
    expected = [
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '2', 'coordinates': [2, 2]}
    ]
    assert clean_keys(list(result)) == expected

    result = ecol.find_near(
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
    ecol.add_skiplist_index(['value'])
    ecol.insert_many([
        {'_key': '1', 'value': 1},
        {'_key': '2', 'value': 2},
        {'_key': '3', 'value': 3},
        {'_key': '4', 'value': 4},
        {'_key': '5', 'value': 5}
    ])
    result = ecol.find_in_range(
        field='value',
        lower=2,
        upper=5,
        offset=1,
        limit=2,
    )
    expected = [
        {'_key': '3', 'value': 3},
        {'_key': '4', 'value': 4},
    ]
    assert clean_keys(list(result)) == expected


# TODO the WITHIN geo function does not seem to work properly
def test_find_in_radius():
    ecol.insert_many([
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '2', 'coordinates': [1, 4]},
        {'_key': '3', 'coordinates': [4, 1]},
        {'_key': '4', 'coordinates': [4, 4]},
    ])
    result = list(ecol.find_in_radius(3, 3, 10, 'distance'))
    for doc in result:
        assert 'distance' in doc


def test_find_in_rectangle():
    ecol.insert_many([
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '2', 'coordinates': [1, 5]},
        {'_key': '3', 'coordinates': [5, 1]},
        {'_key': '4', 'coordinates': [5, 5]},
    ])
    result = ecol.find_in_rectangle(
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

    result = ecol.find_in_rectangle(
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

    result = ecol.find_in_rectangle(
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
    ecol.add_fulltext_index(['text'])
    ecol.insert_many([
        {'_key': '1', 'text': 'foo'},
        {'_key': '2', 'text': 'bar'},
        {'_key': '3', 'text': 'baz'}
    ])
    result = ecol.find_text(
        field='text', query='foo,|bar'
    )
    expected = [
        {'_key': '1', 'text': 'foo'},
        {'_key': '2', 'text': 'bar'}
    ]
    assert clean_keys(list(result)) == expected

    # Bad parameter
    with pytest.raises(DocumentFindTextError):
        ecol.find_text(field='text', query='+')

    with pytest.raises(DocumentFindTextError):
        ecol.find_text(field='text', query='|')


def test_list_indexes():
    expected_index = {
        'selectivity': 1,
        'sparse': False,
        'type': 'primary',
        'fields': ['_key'],
        'unique': True
    }
    indexes = ecol.list_indexes()
    assert isinstance(indexes, dict)
    assert expected_index in indexes.values()


def test_add_hash_index():
    ecol.add_hash_index(['attr1', 'attr2'], unique=True)
    expected_index = {
        'selectivity': 1,
        'sparse': False,
        'type': 'hash',
        'fields': ['attr1', 'attr2'],
        'unique': True
    }
    assert expected_index in ecol.list_indexes().values()


def test_add_cap_constraint():
    ecol.add_cap_constraint(size=10, byte_size=40000)
    expected_index = {
        'type': 'cap',
        'size': 10,
        'byte_size': 40000,
        'unique': False
    }
    assert expected_index in ecol.list_indexes().values()


def test_add_skiplist_index():
    ecol.add_skiplist_index(['attr1', 'attr2'], unique=True)
    expected_index = {
        'sparse': False,
        'type': 'skiplist',
        'fields': ['attr1', 'attr2'],
        'unique': True
    }
    assert expected_index in ecol.list_indexes().values()


def test_add_geo_index():
    # With one attribute
    ecol.add_geo_index(
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
    assert expected_index in ecol.list_indexes().values()

    # With two attributes
    ecol.add_geo_index(
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
    assert expected_index in ecol.list_indexes().values()

    # With more than two attributes (should fail)
    with pytest.raises(IndexCreateError):
        ecol.add_geo_index(fields=['attr1', 'attr2', 'attr3'])


def test_add_fulltext_index():
    # With two attributes (should fail)
    with pytest.raises(IndexCreateError):
        ecol.add_fulltext_index(fields=['attr1', 'attr2'])

    ecol.add_fulltext_index(
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
    assert expected_index in ecol.list_indexes().values()


def test_delete_index():
    old_indexes = set(ecol.list_indexes())
    ecol.add_hash_index(['attr1', 'attr2'], unique=True)
    ecol.add_skiplist_index(['attr1', 'attr2'], unique=True)
    ecol.add_fulltext_index(fields=['attr1'], min_length=10)

    new_indexes = set(ecol.list_indexes())
    assert new_indexes.issuperset(old_indexes)

    for index_id in new_indexes - old_indexes:
        ecol.delete_index(index_id)
    assert set(ecol.list_indexes()) == old_indexes
