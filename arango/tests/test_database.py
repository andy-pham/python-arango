from __future__ import absolute_import, unicode_literals

import pytest

from arango.connection import Connection
from arango.collection import Collection
from arango.graph import Graph
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name,
    generate_graph_name
)


def setup_module(*_):
    global conn, db_name, system_db, db, col_name, graph_name

    conn = Connection()
    db_name = generate_db_name(conn)
    db = conn.create_database(db_name)
    col_name = generate_col_name(db)
    db.create_collection(col_name)
    graph_name = generate_graph_name(db)
    db.create_graph(graph_name)


def teardown_module(*_):
    conn.drop_database(db_name, ignore_missing=True)


def test_properties():
    assert db.name == db_name
    assert repr(db) == "<ArangoDB database '{}'>".format(db_name)


def test_options():
    options = db.options()
    assert 'id' in options
    assert 'path' in options
    assert options['system'] == False
    assert options['name'] == db_name


def test_collection_management():
    # Test list all collections
    cols = db.list_collections()
    assert all(c == col_name or c.startswith('_') for c in cols)

    # Test get collection
    for col in [db.collection(col_name), db[col_name]]:
        assert isinstance(col, Collection)
        assert col.name == col_name

    # Test create duplicate collection
    with pytest.raises(CollectionCreateError):
        db.create_collection(col_name)

    # Test create collection with parameters
    new_col_name = generate_col_name(db)
    col = db.create_collection(
        name=new_col_name,
        sync=True,
        compact=False,
        journal_size=7774208,
        system=False,
        volatile=False,
        key_generator="autoincrement",
        user_keys=False,
        key_increment=9,
        key_offset=100,
        edge=True,
        shard_count=2,
        shard_fields=["test_attr"]
    )
    options = col.options()
    assert 'id' in options
    assert options['name'] == new_col_name
    assert options['sync'] == True
    assert options['compact'] == False
    assert options['journal_size'] == 7774208
    assert options['system'] == False
    assert options['volatile'] == False
    assert options['edge'] == True
    assert options['keygen'] == 'autoincrement'
    assert options['user_keys'] == False
    assert options['key_increment'] == 9
    assert options['key_offset'] == 100

    # Test drop collection
    result = db.drop_collection(new_col_name)
    assert result is True
    assert new_col_name not in db.list_collections()

    # Test drop missing collection
    with pytest.raises(CollectionDropError):
        db.drop_collection(new_col_name)

    # Test drop missing collection (ignore_missing)
    result = db.drop_collection(new_col_name, ignore_missing=True)
    assert result is False


def test_graph_management():
    # Test list all graphs
    assert db.list_graphs() == [graph_name]

    # Test get graph
    graph = db.graph(graph_name)
    assert isinstance(graph, Graph)
    assert graph.name == graph_name

    # Test create duplicate graph
    with pytest.raises(GraphCreateError):
        db.create_graph(graph_name)

    # Test drop graph
    result = db.drop_graph(graph_name)
    assert result is True
    assert graph_name not in db.list_graphs()

    # Test drop missing graph
    with pytest.raises(GraphDropError):
        db.drop_graph(graph_name)

    # Test drop missing graph (ignore_missing)
    result = db.drop_graph(graph_name, ignore_missing=True)
    assert result is False



