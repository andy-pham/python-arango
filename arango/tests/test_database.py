from __future__ import absolute_import, unicode_literals

import pytest

from arango import Arango
from arango.collection import Collection
from arango.graph import Graph
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name,
    generate_graph_name
)


def setup_module(*_):
    global driver, db_name, db, col_name, graph_name

    driver = Arango()
    db_name = generate_db_name(driver)
    db = driver.create_database(db_name)
    col_name = generate_col_name(db)
    db.create_collection(col_name)
    graph_name = generate_graph_name(driver)
    db.create_graph(graph_name)


def teardown_module(*_):
    driver.drop_database(db_name, ignore_missing=True)


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
        keygen="autoincrement",
        user_keys=False,
        key_increment=9,
        key_offset=100,
        edge=True,
        shard_count=2,
        shard_keys=["test_attr"]
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


def test_explain_query():
    fields_to_check = [
        'estimatedNrItems',
        'estimatedCost',
        'rules',
        'variables',
        'collections',
    ]

    # Test invalid query
    with pytest.raises(AQLQueryExplainError):
        db.explain_query('THIS IS AN INVALID QUERY')

    # Test valid query (all_plans=True)
    plans = db.explain_query(
        "FOR d IN {} RETURN d".format(col_name),
        all_plans=True,
        optimizer_rules=["-all", "+use-index-range"]
    )
    for plan in plans:
        for field in fields_to_check:
            assert field in plan

    # Test valid query (all_plans=False)
    plan = db.explain_query(
        "FOR d IN {} RETURN d".format(col_name),
        all_plans=False,
        optimizer_rules=["-all", "+use-index-range"]
    )
    for field in fields_to_check:
        assert field in plan


def test_validate_query():
    # Test invalid query
    with pytest.raises(AQLQueryValidateError):
        db.validate_query('THIS IS AN INVALID QUERY')

    # Test valid query
    result = db.validate_query("FOR d IN {} RETURN d".format(col_name))
    assert 'ast' in result
    assert 'bindVars' in result
    assert 'collections' in result
    assert 'parsed' in result
    assert 'warnings'in result


def test_execute_query():
    # Test invalid AQL query
    with pytest.raises(AQLQueryExecuteError):
        db.execute_query('THIS IS AN INVALID QUERY')

    # Test valid AQL query #1
    db.collection(col_name).insert_many([
        {"_key": "doc01"},
        {"_key": "doc02"},
        {"_key": "doc03"},
    ])
    result = db.execute_query(
        "FOR d IN {} RETURN d".format(col_name),
        count=True,
        batch_size=1,
        ttl=10,
        optimizer_rules=["+all"]
    )
    assert set(d['_key'] for d in result) == {'doc01', 'doc02', 'doc03'}

    # Test valid AQL query #2
    db.collection(col_name).insert_many([
        {"_key": "doc04", "value": 1},
        {"_key": "doc05", "value": 1},
        {"_key": "doc06", "value": 3},
    ])
    result = db.execute_query(
        "FOR d IN {} FILTER d.value == @value RETURN d".format(col_name),
        bind_vars={'value': 1}
    )
    assert set(d['_key'] for d in result) == {'doc04', 'doc05'}


def test_aql_function_management():
    # Test list AQL functions
    assert db.list_functions() == {}

    function_name = 'myfunctions::temperature::celsiustofahrenheit'
    function_body = 'function (celsius) { return celsius * 1.8 + 32; }'

    # Test create AQL function
    db.create_function(function_name, function_body)
    assert db.list_functions() == {function_name: function_body}

    # Test create AQL function again (idempotency)
    db.create_function(function_name, function_body)
    assert db.list_functions() == {function_name: function_body}

    # Test create invalid AQL function
    function_body = 'function (celsius) { invalid syntax }'
    with pytest.raises(AQLFunctionCreateError):
        result = db.create_function(function_name, function_body)
        assert result is True

    # Test delete AQL function
    result = db.delete_function(function_name)
    assert result is True

    # Test delete missing AQL function
    with pytest.raises(AQLFunctionDeleteError):
        db.delete_function(function_name)

    # Test delete missing AQL function (ignore_missing)
    result = db.delete_function(function_name, ignore_missing=True)
    assert result is False


def test_get_query_cache():
    options = db.cache_options()
    assert 'mode' in options
    assert 'limit' in options


def test_set_query_cache():
    options = db.set_cache_options(
        mode='on', limit=100
    )
    assert options['mode'] == 'on'
    assert options['limit'] == 100

    options = db.cache_options()
    assert options['mode'] == 'on'
    assert options['limit'] == 100


def test_clear_query_cache():
    result = db.clear_cache()
    assert isinstance(result, bool)
