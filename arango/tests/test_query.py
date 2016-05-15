from __future__ import absolute_import, unicode_literals

import pytest

from arango import Connection
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name
)


def setup_module(*_):
    global conn, db_name, db, query, col_name

    conn = Connection()

    db_name = generate_db_name(conn)
    db = conn.create_database(db_name)
    query = db.query
    col_name = generate_col_name(db)
    db.create_collection(col_name)


def teardown_module(*_):
    conn.drop_database(db_name, ignore_missing=True)


def test_aql_explain():
    fields_to_check = [
        'estimatedNrItems',
        'estimatedCost',
        'rules',
        'variables',
        'collections',
    ]

    # Test invalid query
    with pytest.raises(QueryExplainError):
        query.explain('THIS IS AN INVALID QUERY')

    # Test valid query (all_plans=True)
    plans = query.explain(
        "FOR d IN {} RETURN d".format(col_name),
        all_plans=True,
        optimizer_rules=["-all", "+use-index-range"]
    )
    for plan in plans:
        for field in fields_to_check:
            assert field in plan

    # Test valid query (all_plans=False)
    plan = query.explain(
        "FOR d IN {} RETURN d".format(col_name),
        all_plans=False,
        optimizer_rules=["-all", "+use-index-range"]
    )
    for field in fields_to_check:
        assert field in plan


def test_aql_validate():
    # Test invalid query
    with pytest.raises(QueryValidateError):
        query.validate('THIS IS AN INVALID QUERY')

    # Test valid query
    result = query.validate("FOR d IN {} RETURN d".format(col_name))
    assert 'ast' in result
    assert 'bindVars' in result
    assert 'collections' in result
    assert 'parsed' in result
    assert 'warnings'in result


def test_aql_execute():
    # Test invalid AQL query
    with pytest.raises(AQLQueryExecuteError):
        query.execute('THIS IS AN INVALID QUERY')

    # Test valid AQL query #1
    db.collection(col_name).insert_many([
        {"_key": "doc01"},
        {"_key": "doc02"},
        {"_key": "doc03"},
    ])
    result = query.execute(
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
    result = query.execute(
        "FOR d IN {} FILTER d.value == @value RETURN d".format(col_name),
        bind_vars={'value': 1}
    )
    assert set(d['_key'] for d in result) == {'doc04', 'doc05'}


def test_aql_function_management():
    # Test list AQL functions
    assert query.list_functions() == {}

    function_name = 'myfunctions::temperature::celsiustofahrenheit'
    function_body = 'function (celsius) { return celsius * 1.8 + 32; }'

    # Test create AQL function
    query.create_function(function_name, function_body)
    assert query.list_functions() == {function_name: function_body}

    # Test create AQL function again (idempotency)
    query.create_function(function_name, function_body)
    assert query.list_functions() == {function_name: function_body}

    # Test create invalid AQL function
    function_body = 'function (celsius) { invalid syntax }'
    with pytest.raises(AQLFunctionCreateError):
        result = query.create_function(function_name, function_body)
        assert result is True

    # Test delete AQL function
    result = query.delete_function(function_name)
    assert result is True

    # Test delete missing AQL function
    with pytest.raises(AQLFunctionDeleteError):
        query.delete_function(function_name)

    # Test delete missing AQL function (ignore_missing)
    result = query.delete_function(function_name, ignore_missing=True)
    assert result is False


def test_get_aql_cache_options():
    options = query.cache.options()
    assert 'mode' in options
    assert 'limit' in options


def test_set_aql_cache_options():
    options = query.cache.set_options(
        mode='on', limit=100
    )
    assert options['mode'] == 'on'
    assert options['limit'] == 100

    options = query.cache.options()
    assert options['mode'] == 'on'
    assert options['limit'] == 100


def test_clear_aql_cache():
    result = query.cache.clear()
    assert isinstance(result, bool)
