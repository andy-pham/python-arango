"""Test the database.py module."""

from __future__ import absolute_import, unicode_literals

import pytest

from arango import Arango
from arango.collection import Collection
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name,
    generate_graph_name
)


def setup_module(*_):
    global driver, db_name, db, col_name, collection, graph_name
    driver = Arango()
    db_name = generate_db_name(driver)
    db = driver.create_database(db_name)
    col_name = generate_col_name(db)
    collection = db.create_collection(col_name)


def teardown_module(*_):
    driver.drop_database(db_name, ignore_missing=True)


def test_get_properties():
    assert db.name == db_name

    properties = db.get_properties()
    assert 'id' in properties
    assert 'path' in properties
    assert properties['is_system'] == False
    assert properties['name'] == db_name


def test_explain_query():
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
        assert 'rules' in plan
        assert 'variables' in plan
        assert 'collections' in plan
        assert 'estimatedCost' in plan
        assert 'estimatedNrItems' in plan

    # Test valid query (all_plans=False)
    plan = db.explain_query(
        "FOR d IN {} RETURN d".format(col_name),
        all_plans=False,
        optimizer_rules=["-all", "+use-index-range"]
    )
    assert 'rules' in plan
    assert 'variables' in plan
    assert 'collections' in plan
    assert 'estimatedCost' in plan
    assert 'estimatedNrItems' in plan


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
    collection.import_documents([
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
    collection.import_documents([
        {"_key": "doc04", "value": 1},
        {"_key": "doc05", "value": 1},
        {"_key": "doc06", "value": 3},
    ])
    result = db.execute_query(
        "FOR d IN {} FILTER d.value == @value RETURN d".format(col_name),
        bind_vars={'value': 1}
    )
    assert set(d['_key'] for d in result) == {'doc04', 'doc05'}


def test_list_collections():
    # Test list user collections only
    assert db.list_collections(user_only=True) == [col_name]

    # Test list all collections
    cols = db.list_collections(user_only=False)
    assert all(c == col_name or c.startswith('_') for c in cols)


def test_get_collection():
    for c in [db.collection(col_name), db.col(col_name), db[col_name]]:
        assert isinstance(c, Collection)
        assert c.name == col_name


def test_collection_mgnt():
    # Test create duplicate collection
    with pytest.raises(CollectionCreateError):
        db.create_collection(col_name)

    # Test create collection with parameters
    new_col_name = generate_col_name(db)
    col = db.create_collection(
        name=new_col_name,
        wait_for_sync=True,
        do_compact=False,
        journal_size=7774208,
        is_system=False,
        is_volatile=False,
        key_generator_type="autoincrement",
        allow_user_keys=False,
        key_increment=9,
        key_offset=100,
        is_edge=True,
        number_of_shards=2,
        shard_keys=["test_attr"]
    )
    properties = col.get_properties()
    assert 'id' in properties
    assert properties['name'] == new_col_name
    assert properties['wait_for_sync'] == True
    assert properties['do_compact'] == False
    assert properties['journal_size'] == 7774208
    assert properties['is_system'] == False
    assert properties['is_volatile'] == False
    assert properties['is_edge'] == True

    # Test create


def test_aql_function_mgnt():
    # Test list AQL functions
    assert db.list_aql_functions() == {}

    function_name = 'myfunctions::temperature::celsiustofahrenheit'
    function_body = 'function (celsius) { return celsius * 1.8 + 32; }'

    # Test create AQL function
    db.create_aql_function(function_name, function_body)
    assert db.list_aql_functions() == {function_name: function_body}

    # Test create AQL function again (idempotency)
    db.create_aql_function(function_name, function_body)
    assert db.list_aql_functions() == {function_name: function_body}

    # Test create invalid AQL function
    function_body = 'function (celsius) { invalid syntax }'
    with pytest.raises(AQLFunctionCreateError):
        result = db.create_aql_function(function_name, function_body)
        assert result is True

    # Test delete AQL function
    result = db.delete_aql_function(function_name)
    assert result is True

    # Test delete missing AQL function
    with pytest.raises(AQLFunctionDeleteError):
        db.delete_aql_function(function_name)

    # Test delete missing AQL function (ignore_missing)
    result = db.delete_aql_function(function_name, ignore_missing=True)
    assert result is False


def test_get_query_cache():
    cache = db.get_query_cache()
    assert 'mode' in cache
    assert 'max_results' in cache


def test_set_query_cache():
    cache = db.set_query_cache(
        mode='on',
        max_results=100
    )
    assert cache['mode'] == 'on'
    assert cache['max_results'] == 100

    cache = db.get_query_cache()
    assert cache['mode'] == 'on'
    assert cache['max_results'] == 100


def test_clear_query_cache():
    result = db.clear_query_cache()
    assert isinstance(result, bool)


def test_execute_transaction():
    # Test execute transaction with no params
    action = """
        function () {{
            var db = require('internal').db;
            db.{col}.save({{ _key: 'doc1'}});
            db.{col}.save({{ _key: 'doc2'}});
            return 'success!';
        }}
    """.format(col=col_name)

    result = db.execute_transaction(
        action=action,
        read_collections=[col_name],
        write_collections=[col_name],
        wait_for_sync=True,
        lock_timeout=10000
    )
    assert result == 'success!'
    assert 'doc1' in collection
    assert 'doc2' in collection

    # Test execute transaction with params
    action = """
        function (params) {{
            var db = require('internal').db;
            db.{col}.save({{ _key: 'doc3', val: params.val1 }});
            db.{col}.save({{ _key: 'doc4', val: params.val2 }});
            return 'success!';
        }}
    """.format(col=col_name)

    result = db.execute_transaction(
        action=action,
        read_collections=[col_name],
        write_collections=[col_name],
        params={"val1": 1, "val2": 2},
        wait_for_sync=True,
        lock_timeout=10000
    )
    assert result == 'success!'
    assert 'doc3' in collection
    assert 'doc4' in collection
    assert collection["doc3"]["val"] == 1
    assert collection["doc4"]["val"] == 2


def test_graph_mgnt():
    assert db.list_graphs() == []

    graph_name = generate_graph_name(db)
    graph = db.create_graph(graph_name)
    assert graph.name == graph_name
    assert graph_name in db.list_graphs()


