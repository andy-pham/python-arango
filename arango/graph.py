"""ArangoDB Graph."""

from arango.exceptions import *
from arango.constants import HTTP_OK


class Graph(object):
    """Wrapper for ArangoDB's graph-specific APIs:

    1. Graph Properties
    2. Vertex Collection Management
    3. Edge Definition Management
    4. Vertex Management
    5. Edge Management
    6. Graph Traversals
    """

    def __init__(self, connection, name):
        """Initialize the wrapper object.

        :param connection: ArangoDB API connection object
        :type connection: arango.connection.Connection
        :param name: the name of the graph
        :type name: str
        """
        self._conn = connection
        self._name = name

    def __repr__(self):
        """Return a descriptive string of the instance."""
        return "<ArangoDB graph '{}'>".format(self._name)

    @property
    def name(self):
        """Return the name of the graph.

        :returns: the name of the graph
        :rtype: str
        """
        return self._name

    def options(self):
        """Return the properties of the graph.

        :returns: the properties of the graph
        :rtype: dict
        :raises: GraphPropertiesError
        """
        res = self._conn.get(
            "/_api/gharial/{}".format(self._name)
        )
        if res.status_code not in HTTP_OK:
            raise GraphPropertyError(res)
        return res.body["graph"]

    ################################
    # Vertex Collection Management #
    ################################

    def list_orphan_collections(self):
        """Return the orphan collections of the graph.

        :returns: the string names of the orphan collections
        :rtype: list
        :raises: GraphPropertiesError
        """
        return self.options["orphan_collections"]

    def list_vertex_collections(self):
        """Return the vertex collections of the graph.

        :returns: the string names of the vertex collections
        :rtype: list
        :raises: VertexCollectionListError
        """
        res = self._conn.get("/_api/gharial/{}/vertex".format(self._name))
        if res.status_code not in HTTP_OK:
            raise VertexCollectionListError(res)
        return res.body["collections"]

    def create_vertex_collection(self, name):
        """Create a vertex collection to the graph.

        :param name: the name of the vertex collection to create
        :type name: str
        :returns: the updated list of the vertex collection names
        :rtype: list
        :raises: VertexCollectionCreateError
        """
        res = self._conn.post(
            "/_api/gharial/{}/vertex".format(self._name),
            data={"collection": name}
        )
        if res.status_code not in HTTP_OK:
            raise VertexCollectionCreateError(res)
        return self.list_vertex_collections

    def delete_vertex_collection(self, name, drop_collection=False):
        """Delete a vertex collection from the graph.

        :param name: the name of the vertex collection to delete
        :type name: str
        :param drop_collection: whether or not to drop the collection also
        :type drop_collection: bool
        :returns: the updated list of the vertex collection names
        :rtype: list
        :raises: VertexCollectionDropError
        """
        res = self._conn.delete(
            "/_api/gharial/{}/vertex/{}".format(self._name, name),
            params={"dropCollection": drop_collection}
        )
        if res.status_code not in HTTP_OK:
            raise VertexCollectionDropError(res)
        return self.list_vertex_collections

    ##############################
    # Edge Definition Management #
    ##############################

    def list_edge_definitions(self):
        """Return the edge definitions of the graph.

        :returns: the edge definitions of this graph
        :rtype: list
        :raises: GraphPropertiesError
        """
        return self.options["edge_definitions"]

    def add_edge_definition(self, edge_collection, from_vertex_collections,
                            to_vertex_collections):
        """Create a edge definition to this graph.

        :param edge_collection: the name of the edge collection
        :type edge_collection: str
        :param from_vertex_collections: names of ``from`` vertex collections
        :type from_vertex_collections: list
        :param to_vertex_collections: the names of ``to`` vertex collections
        :type to_vertex_collections: list
        :returns: the updated list of edge definitions
        :rtype: list
        :raises: EdgeDefinitionCreateError
        """
        res = self._conn.post(
            "/_api/gharial/{}/edge".format(self._name),
            data={
                "collection": edge_collection,
                "from": from_vertex_collections,
                "to": to_vertex_collections
            }
        )
        if res.status_code not in HTTP_OK:
            raise EdgeDefinitionCreateError(res)
        return res.body["graph"]["edgeDefinitions"]

    def replace_edge_definition(self, edge_collection,
                                from_vertex_collections,
                                to_vertex_collections):
        """Replace an edge definition in this graph.

        :param edge_collection: the name of the edge collection
        :type edge_collection: str
        :param from_vertex_collections: names of ``from`` vertex collections
        :type from_vertex_collections: list
        :param to_vertex_collections: the names of ``to`` vertex collections
        :type to_vertex_collections: list
        :returns: the updated list of edge definitions
        :rtype: list
        :raises: EdgeDefinitionReplaceError
        """
        res = self._conn.put(
            "/_api/gharial/{}/edge/{}".format(
                self._name, edge_collection
            ),
            data={
                "collection": edge_collection,
                "from": from_vertex_collections,
                "to": to_vertex_collections
            }
        )
        if res.status_code not in HTTP_OK:
            raise EdgeDefinitionReplaceError(res)
        return res.body["graph"]["edgeDefinitions"]

    def delete_edge_definition(self, collection,
                               drop_collection=False):
        """Delete the specified edge definition from this graph.

        :param collection: the name of the edge collection to delete
        :type collection: str
        :param drop_collection: whether or not to drop the collection also
        :type drop_collection: bool
        :returns: the updated list of edge definitions
        :rtype: list
        :raises: EdgeDefinitionDeleteError
        """
        res = self._conn.delete(
            "/_api/gharial/{}/edge/{}".format(self._name, collection),
            params={"dropCollection": drop_collection}
        )
        if res.status_code not in HTTP_OK:
            raise EdgeDefinitionDeleteError(res)
        return res.body["graph"]["edgeDefinitions"]

    #####################
    # Vertex Management #
    #####################

    def get_vertex(self, vertex_id, rev=None):
        """Return the vertex of the specified ID in this graph.

        If the vertex revision ``rev`` is specified, it must match against
        the revision of the retrieved vertex.

        :param vertex_id: the ID of the vertex to retrieve
        :type vertex_id: str
        :param rev: the vertex revision must match this value
        :type rev: str | None
        :returns: the requested vertex | None if not found
        :rtype: dict | None
        :raises: VertexRevisionError, VertexGetError
        """
        res = self._conn.get(
            "/_api/gharial/{}/vertex/{}".format(self._name, vertex_id),
            params={"rev": rev} if rev is not None else {}
        )
        if res.status_code == 412:
            raise VertexRevisionError(res)
        elif res.status_code == 404:
            return None
        elif res.status_code not in HTTP_OK:
            raise VertexGetError(res)
        return res.body["vertex"]

    def create_vertex(self, collection, data, sync=False,
                      _batch=False):
        """Create a vertex to the specified vertex collection if this graph.

        If ``data`` contains the ``_key`` key, its value must be unused
        in the collection.

        :param collection: the name of the vertex collection
        :type collection: str
        :param data: the body of the new vertex
        :type data: dict
        :param sync: wait for the create to sync to disk
        :type sync: bool
        :returns: the id, rev and key of the new vertex
        :rtype: dict
        :raises: VertexCreateError
        """
        path = "/_api/gharial/{}/vertex/{}".format(self._name, collection)
        params = {"waitForSync": sync}
        if _batch:
            return {
                "method": "post",
                "path": path,
                "data": data,
                "params": params,
            }
        res = self._conn.post(endpoint=path, data=data, params=params)
        if res.status_code not in HTTP_OK:
            raise VertexCreateError(res)
        return res.body["vertex"]

    def update_vertex(self, vertex_id, data, rev=None, keep_none=True,
                      sync=False, _batch=False):
        """Update a vertex of the specified ID in this graph.

        If ``keep_none`` is set to True, then attributes with values None
        are retained. Otherwise, they are deleted from the vertex.

        If ``data`` contains the ``_key`` key, it is ignored.

        If the ``_rev`` key is in ``data``, the revision of the target
        vertex must match against its value. Otherwise a VertexRevision
        error is thrown. If ``rev`` is also provided, its value is preferred.

        :param vertex_id: the ID of the vertex to be updated
        :type vertex_id: str
        :param data: the body to update the vertex with
        :type data: dict
        :param rev: the vertex revision must match this value
        :type rev: str | None
        :param keep_none: whether or not to keep the keys with value None
        :type keep_none: bool
        :param sync: wait for the update to sync to disk
        :type sync: bool
        :returns: the id, rev and key of the updated vertex
        :rtype: dict
        :raises: VertexRevisionError, VertexUpdateError
        """
        path = "/_api/gharial/{}/vertex/{}".format(self._name, vertex_id)
        params = {
            "waitForSync": sync,
            "keepNull": keep_none
        }
        if rev is not None:
            params["rev"] = rev
        elif "_rev" in data:
            params["rev"] = data["_rev"]
        if _batch:
            return {
                "method": "patch",
                "path": path,
                "data": data,
                "params": params,
            }
        res = self._conn.patch(endpoint=path, data=data, params=params)
        if res.status_code == 412:
            raise VertexRevisionError(res)
        elif res.status_code not in {200, 202}:
            raise VertexUpdateError(res)
        return res.body["vertex"]

    def replace_vertex(self, vertex_id, data, rev=None, sync=False,
                       _batch=False):
        """Replace a vertex of the specified ID in this graph.

        If ``data`` contains the ``_key`` key, it is ignored.

        If the ``_rev`` key is in ``data``, the revision of the target
        vertex must match against its value. Otherwise a VertexRevision
        error is thrown. If ``rev`` is also provided, its value is preferred.

        :param vertex_id: the ID of the vertex to be replaced
        :type vertex_id: str
        :param data: the body to replace the vertex with
        :type data: dict
        :param rev: the vertex revision must match this value
        :type rev: str | None
        :param sync: wait for replace to sync to disk
        :type sync: bool
        :returns: the id, rev and key of the replaced vertex
        :rtype: dict
        :raises: VertexRevisionError, VertexReplaceError
        """
        path = "/_api/gharial/{}/vertex/{}".format(self._name, vertex_id)
        params = {"waitForSync": sync}
        if rev is not None:
            params["rev"] = rev
        if "_rev" in data:
            params["rev"] = data["_rev"]
        if _batch:
            return {
                "method": "put",
                "path": path,
                "data": data,
                "params": params,
            }
        res = self._conn.put(endpoint=path, params=params, data=data)
        if res.status_code == 412:
            raise VertexRevisionError(res)
        elif res.status_code not in {200, 202}:
            raise VertexReplaceError(res)
        return res.body["vertex"]

    def delete_vertex(self, vertex_id, rev=None, sync=False,
                      _batch=False):
        """Delete the vertex of the specified ID from this graph.

        :param vertex_id: the ID of the vertex to be deleted
        :type vertex_id: str
        :param rev: the vertex revision must match this value
        :type rev: str | None
        :raises: VertexRevisionError, VertexDeleteError
        """
        path = "/_api/gharial/{}/vertex/{}".format(self._name, vertex_id)
        params = {"waitForSync": sync}
        if rev is not None:
            params["rev"] = rev
        if _batch:
            return {
                "method": "delete",
                "path": path,
                "params": params,
            }
        res = self._conn.delete(endpoint=path, params=params)
        if res.status_code == 412:
            raise VertexRevisionError(res)
        if res.status_code not in {200, 202}:
            raise VertexDeleteError(res)

    ###################
    # Edge Management #
    ###################

    def get_edge(self, edge_id, rev=None):
        """Return the edge of the specified ID in this graph.

        If the edge revision ``rev`` is specified, it must match against
        the revision of the retrieved edge.

        :param edge_id: the ID of the edge to retrieve
        :type edge_id: str
        :param rev: the edge revision must match this value
        :type rev: str | None
        :returns: the requested edge | None if not found
        :rtype: dict | None
        :raises: EdgeRevisionError, EdgeGetError
        """
        res = self._conn.get(
            "/_api/gharial/{}/edge/{}".format(self._name, edge_id),
            params={} if rev is None else {"rev": rev}
        )
        if res.status_code == 412:
            raise EdgeRevisionError(res)
        elif res.status_code == 404:
            return None
        elif res.status_code not in HTTP_OK:
            raise EdgeGetError(res)
        return res.body["edge"]

    def create_edge(self, collection, data, sync=False, _batch=False):
        """Create an edge to the specified edge collection of this graph.

        The ``data`` must contain ``_from`` and ``_to`` keys with valid
        vertex IDs as their values. If ``data`` contains the ``_key`` key,
        its value must be unused in the collection.

        :param collection: the name of the edge collection
        :type collection: str
        :param data: the body of the new edge
        :type data: dict
        :param sync: wait for the create to sync to disk
        :type sync: bool
        :returns: the id, rev and key of the new edge
        :rtype: dict
        :raises: DocumentInvalidError, EdgeCreateError
        """
        if "_to" not in data:
            raise DocumentInvalidError(
                "the new edge data is missing the '_to' key")
        if "_from" not in data:
            raise DocumentInvalidError(
                "the new edge data is missing the '_from' key")
        path = "/_api/gharial/{}/edge/{}".format(self._name, collection)
        params = {"waitForSync": sync}
        if _batch:
            return {
                "method": "post",
                "path": path,
                "data": data,
                "params": params,
            }
        res = self._conn.post(endpoint=path, data=data, params=params)
        if res.status_code not in HTTP_OK:
            raise EdgeCreateError(res)
        return res.body["edge"]

    def update_edge(self, edge_id, data, rev=None, keep_none=True,
                    sync=False, _batch=False):
        """Update the edge of the specified ID in this graph.

        If ``keep_none`` is set to True, then attributes with values None
        are retained. Otherwise, they are deleted from the edge.

        If ``data`` contains the ``_key`` key, it is ignored.

        If the ``_rev`` key is in ``data``, the revision of the target
        edge must match against its value. Otherwise a EdgeRevision
        error is thrown. If ``rev`` is also provided, its value is preferred.

        The ``_from`` and ``_to`` attributes are immutable, and they are
        ignored if present in ``data``

        :param edge_id: the ID of the edge to be deleted
        :type edge_id: str
        :param data: the body to update the edge with
        :type data: dict
        :param rev: the edge revision must match this value
        :type rev: str | None
        :param keep_none: whether or not to keep the keys with value None
        :type keep_none: bool
        :param sync: wait for the update to sync to disk
        :type sync: bool
        :returns: the id, rev and key of the updated edge
        :rtype: dict
        :raises: EdgeRevisionError, EdgeUpdateError
        """
        path = "/_api/gharial/{}/edge/{}".format(self._name, edge_id)
        params = {
            "waitForSync": sync,
            "keepNull": keep_none
        }
        if rev is not None:
            params["rev"] = rev
        elif "_rev" in data:
            params["rev"] = data["_rev"]
        if _batch:
            return {
                "method": "patch",
                "path": path,
                "data": data,
                "params": params,
            }
        res = self._conn.patch(endpoint=path, data=data, params=params)
        if res.status_code == 412:
            raise EdgeRevisionError(res)
        elif res.status_code not in {200, 202}:
            raise EdgeUpdateError(res)
        return res.body["edge"]

    def replace_edge(self, edge_id, data, rev=None, sync=False,
                     _batch=False):
        """Replace the edge of the specified ID in this graph.

        If ``data`` contains the ``_key`` key, it is ignored.

        If the ``_rev`` key is in ``data``, the revision of the target
        edge must match against its value. Otherwise a EdgeRevision
        error is thrown. If ``rev`` is also provided, its value is preferred.

        The ``_from`` and ``_to`` attributes are immutable, and they are
        ignored if present in ``data``

        :param edge_id: the ID of the edge to be deleted
        :type edge_id: str
        :param data: the body to replace the edge with
        :type data: dict
        :param rev: the edge revision must match this value
        :type rev: str | None
        :param sync: wait for the replace to sync to disk
        :type sync: bool
        :returns: the id, rev and key of the replaced edge
        :rtype: dict
        :raises: EdgeRevisionError, EdgeReplaceError
        """
        path = "/_api/gharial/{}/edge/{}".format(self._name, edge_id)
        params = {"waitForSync": sync}
        if rev is not None:
            params["rev"] = rev
        elif "_rev" in data:
            params["rev"] = data["_rev"]
        if _batch:
            return {
                "method": "put",
                "path": path,
                "data": data,
                "params": params,
            }
        res = self._conn.put(endpoint=path, params=params, data=data)
        if res.status_code == 412:
            raise EdgeRevisionError(res)
        elif res.status_code not in {200, 202}:
            raise EdgeReplaceError(res)
        return res.body["edge"]

    def delete_edge(self, edge_id, rev=None, sync=False,
                    _batch=False):
        """Delete the edge of the specified ID from this graph.

        :param edge_id: the ID of the edge to be deleted
        :type edge_id: str
        :param rev: the edge revision must match this value
        :type rev: str | None
        :raises: EdgeRevisionError, EdgeDeleteError
        """
        path = "/_api/gharial/{}/edge/{}".format(self._name, edge_id)
        params = {"waitForSync": sync}
        if _batch:
            return {
                "method": "delete",
                "path": path,
                "params": params,
            }
        if rev is not None:
            params["rev"] = rev
        res = self._conn.delete(endpoint=path, params=params)
        if res.status_code == 412:
            raise EdgeRevisionError(res)
        elif res.status_code not in {200, 202}:
            raise EdgeDeleteError(res)

    ####################
    # Graph Traversals #
    ####################

    def traverse(self, start_vertex, direction=None, strategy=None, order=None,
                 item_order=None, uniqueness=None, max_iterations=None,
                 min_depth=None, max_depth=None, init=None, filters=None,
                 visitor=None, expander=None, sort=None):
        """Execute a graph traversal and return the visited vertices.

        For more details on ``init``, ``filter``, ``visitor``, ``expander``
        and ``sort`` please refer to the ArangoDB HTTP API documentation:
        https://docs.arangodb.com/HttpTraversal/README.html

        :param start_vertex: the ID of the start vertex
        :type start_vertex: str
        :param direction: "outbound" or "inbound" or "any"
        :type direction: str
        :param strategy: "depthfirst" or "breadthfirst"
        :type strategy: str
        :param order: "preorder" or "postorder"
        :type order: str
        :param item_order: "forward" or "backward"
        :type item_order: str
        :param uniqueness: uniqueness of vertices and edges visited
        :type uniqueness: dict
        :param max_iterations: max number of iterations in each traversal
        :type max_iterations: int
        :param min_depth: minimum traversal depth
        :type min_depth: int
        :param max_depth: maximum traversal depth
        :type max_depth: int
        :param init: custom init function in Javascript
        :type init: str
        :param filters: custom filter function in Javascript
        :type filters: str
        :param visitor: custom visitor function in Javascript
        :type visitor: str
        :param expander: custom expander function in Javascript
        :type expander: str
        :param sort: custom sorting function in Javascript
        :type sort: str
        :returns: the traversal results
        :rtype: dict
        :raises: GraphTraversalError
        """
        data = {
            "startVertex": start_vertex,
            "graphName": self._name,
            "direction": direction,
            "strategy": strategy,
            "order": order,
            "itemOrder": item_order,
            "uniqueness": uniqueness,
            "maxIterations": max_iterations,
            "minDepth": min_depth,
            "maxDepth": max_depth,
            "init": init,
            "filter": filters,
            "visitor": visitor,
            "expander": expander,
            "sort": sort
        }
        data = {k: v for k, v in data.items() if v is not None}
        res = self._conn.post("/_api/traversal", data=data)
        if res.status_code not in HTTP_OK:
            raise GraphTraversalError(res)
        return res.body["result"]
