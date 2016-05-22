from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from arango.connection import Connection
from arango.constants import COLLECTION_STATUSES
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name,
    generate_graph_name,
    clean_keys
)

conn = Connection()
db_name = generate_db_name(conn)
db = conn.create_database(db_name)
col_name = generate_col_name(db)
col = db.create_collection(col_name)
graph_name = generate_graph_name(db)
graph = db.create_graph(graph_name)


def teardown_module(*_):
    conn.drop_database(db_name, ignore_missing=True)


def setup_function(*_):
    col.truncate()


def test_properties():
    assert graph.name == graph_name
    assert repr(graph) == (
        "<ArangoDB graph '{}'>".format(graph_name)
    )


def test_options():
    options = graph.options()
    assert options['id'] == '_graphs/{}'.format(graph_name)
    assert options['name'] == graph_name
    assert options['revision'].isdigit()


def test_create_vertex_collection():
    assert graph.vertex_collections() == []
    assert graph.create_vertex_collection('v1') == True
    assert graph.vertex_collections() == ['v1']
    assert graph.orphan_collections() == ['v1']
    assert 'v1' in db.collections()

    # Test create duplicate vertex collection
    with pytest.raises(VertexCollectionCreateError):
        graph.create_vertex_collection('v1')
    assert graph.vertex_collections() == ['v1']
    assert graph.orphan_collections() == ['v1']
    assert 'v1' in db.collections()

    assert graph.create_vertex_collection('v2') == True
    assert sorted(graph.vertex_collections()) == ['v1', 'v2']
    assert graph.orphan_collections() == ['v1', 'v2']
    assert 'v1' in db.collections()
    assert 'v2' in db.collections()


def test_list_vertex_collections():
    assert graph.vertex_collections() == ['v1', 'v2']


def test_delete_vertex_collection():
    assert sorted(graph.vertex_collections()) == ['v1', 'v2']
    assert graph.delete_vertex_collection('v1') == True
    assert graph.vertex_collections() == ['v2']
    assert 'v1' in db.collections()

    # Test delete missing vertex collection
    with pytest.raises(VertexCollectionDeleteError):
        graph.delete_vertex_collection('v1')

    assert graph.delete_vertex_collection('v2', purge=True) == True
    assert graph.vertex_collections() == []
    assert 'v1' in db.collections()
    assert 'v2' not in db.collections()


def test_create_edge_definition():
    assert graph.edge_definitions() == []
    assert graph.create_edge_definition('e1', [], []) == True
    assert graph.edge_definitions() == [{
        'name': 'e1',
        'from_collections': [],
        'to_collections': []
    }]
    assert 'e1' in db.collections()

    # Test create duplicate edge definition
    with pytest.raises(EdgeDefinitionCreateError):
        assert graph.create_edge_definition('e1', [], [])
    assert graph.edge_definitions() == [{
        'name': 'e1',
        'from_collections': [],
        'to_collections': []
    }]

    # Test create edge definition with existing vertex collection
    assert graph.create_vertex_collection('v1') == True
    assert graph.create_vertex_collection('v2') == True
    assert graph.create_edge_definition('e2', ['v1'], ['v2']) == True
    assert graph.edge_definitions() == [
        {
            'name': 'e1',
            'from_collections': [],
            'to_collections': []
        },
        {
            'name': 'e2',
            'from_collections': ['v1'],
            'to_collections': ['v2']
        }
    ]
    assert 'e2' in db.collections()

    # Test create edge definition with missing vertex collection
    assert graph.create_edge_definition('e3', ['v3'], ['v3']) == True
    assert graph.edge_definitions() == [
        {
            'name': 'e1',
            'from_collections': [],
            'to_collections': []
        },
        {
            'name': 'e2',
            'from_collections': ['v1'],
            'to_collections': ['v2']
        },
        {
            'name': 'e3',
            'from_collections': ['v3'],
            'to_collections': ['v3']
        }
    ]
    assert 'v3' in graph.vertex_collections()
    assert 'v3' not in graph.orphan_collections()
    assert 'v3' in db.collections()
    assert 'e3' in db.collections()


def test_list_edge_definitions():
    assert graph.edge_definitions() == [
        {
            'name': 'e1',
            'from_collections': [],
            'to_collections': []
        },
        {
            'name': 'e2',
            'from_collections': ['v1'],
            'to_collections': ['v2']
        },
        {
            'name': 'e3',
            'from_collections': ['v3'],
            'to_collections': ['v3']
        }
    ]


def test_replace_edge_definition():
    assert graph.replace_edge_definition(
        name='e1',
        from_collections=['v3'],
        to_collections=['v2']
    ) == True
    assert graph.orphan_collections() == ['v1']
    assert graph.edge_definitions() == [
        {
            'name': 'e1',
            'from_collections': ['v3'],
            'to_collections': ['v2']
        },
        {
            'name': 'e2',
            'from_collections': ['v1'],
            'to_collections': ['v2']
        },
        {
            'name': 'e3',
            'from_collections': ['v3'],
            'to_collections': ['v3']
        }
    ]
    assert graph.replace_edge_definition(
        name='e2',
        from_collections=['v1'],
        to_collections=[]
    ) == True
    assert graph.orphan_collections() == []
    assert 'v3' not in graph.orphan_collections()
    assert graph.replace_edge_definition(
        name='e3',
        from_collections=['v4'],
        to_collections=['v4']
    ) == True
    with pytest.raises(EdgeDefinitionReplaceError):
        graph.replace_edge_definition(
            name='e4',
            from_collections=[],
            to_collections=['v1']
        )
    assert graph.edge_definitions() == [
        {
            'name': 'e1',
            'from_collections': ['v3'],
            'to_collections': ['v2']
        },
        {
            'name': 'e2',
            'from_collections': ['v1'],
            'to_collections': []
        },
        {
            'name': 'e3',
            'from_collections': ['v4'],
            'to_collections': ['v4']
        }
    ]
    assert graph.orphan_collections() == []


def test_delete_edge_definition():
    assert graph.delete_edge_definition('e3') == True
    assert graph.edge_definitions() == [
        {
            'name': 'e1',
            'from_collections': ['v3'],
            'to_collections': ['v2']
        },
        {
            'name': 'e2',
            'from_collections': ['v1'],
            'to_collections': []
        }
    ]
    assert graph.orphan_collections() == ['v4']
    assert 'v4' in graph.vertex_collections()
    assert 'v4' in db.collections()
    assert 'e3' in db.collections()

    with pytest.raises(EdgeDefinitionDeleteError):
        graph.delete_edge_definition('e3')

    assert graph.delete_edge_definition('e1', purge=True) == True
    assert graph.edge_definitions() == [
        {
            'name': 'e2',
            'from_collections': ['v1'],
            'to_collections': []
        }
    ]
    assert sorted(graph.orphan_collections()) == ['v2', 'v3', 'v4']
    assert 'e1' not in db.collections()
    assert 'e2' in db.collections()
    assert 'e3' in db.collections()


def test_get_vertex():
    pass


def test_insert_vertex():
    pass


def test_update_vertex():
    pass


def test_replace_vertex():
    pass


def test_delete_vertex():
    pass
