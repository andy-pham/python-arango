from __future__ import absolute_import, unicode_literals

from arango.collection import (
    Collection,
    EdgeCollection
)
from arango.graph import Graph
from arango.constants import HTTP_OK
from arango.exceptions import (
    BatchExecuteError,
    ArangoError
)
from arango.response import Response


class Batch(object):

    def __init__(self, connection):
        self._conn = connection
        self._requests = []
        self._handlers = []
        self.type = 'batch'

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        return self.commit()

    def __repr__(self):
        return '<ArangoDB batch query ({} requests)>'.format(
            len(self._requests)
        )

    def add(self, request, handler):
        self._requests.append(request)
        self._handlers.append(handler)

    def commit(self):
        try:
            if not self._requests:
                return []
            raw_data = ''
            for content_id, request in enumerate(self._requests, start=1):
                raw_data += '--XXXsubpartXXX\r\n'
                raw_data += 'Content-Type: application/x-arango-batchpart\r\n'
                raw_data += 'Content-Id: {}\r\n\r\n'.format(content_id)
                raw_data += '{}\r\n'.format(request.stringify())
            raw_data += '--XXXsubpartXXX--\r\n\r\n'

            res = self._conn.post(
                endpoint='/_api/batch',
                headers={
                    'Content-Type': (
                        'multipart/form-data; boundary=XXXsubpartXXX'
                    )
                },
                data=raw_data,
            )
            if res.status_code not in HTTP_OK:
                raise BatchExecuteError(res)

            url_prefix = self._conn.url_prefix
            responses = []
            # TODO do something about this ugly ass parsing
            for index, raw_res in enumerate(
                res.raw_body.split('--XXXsubpartXXX')[1:-1]
            ):
                request = self._requests[index]
                handler = self._handlers[index]
                res_parts = raw_res.strip().split('\r\n')
                raw_status, raw_body = res_parts[3], res_parts[-1]
                _, status_code, status_text = raw_status.split(' ', 2)

                try:
                    result = handler(Response(
                        method=request.method,
                        url=url_prefix + request.endpoint,
                        headers=request.headers,
                        status_code=int(status_code),
                        status_text=status_text,
                        body=raw_body
                    ))
                except ArangoError as err:
                    responses.append(err)
                else:
                    responses.append(result)
            return responses
        finally:
            self._requests, self._handlers = [], []

    def collection(self, name):
        """Return the Collection object of the specified name.

        :param name: the name of the collection
        :type name: str
        :returns: the requested collection object
        :rtype: arango.collection.Collection
        :raises: TypeError
        """
        return Collection(self, name)

    def edge_collection(self, name):
        """Return the EdgeCollection object of the specified name.

        :param name: the name of the edge collection
        :type name: str
        :returns: the requested edge collection object
        :rtype: arango.collection.EdgeCollection
        :raises: TypeError
        """
        return EdgeCollection(self, name)

    def graph(self, name):
        """Return the Graph object of the specified name.

        :param name: the name of the graph
        :type name: str
        :returns: the requested graph object
        :rtype: arango.graph.Graph
        :raises: TypeError, GraphNotFound
        """
        return Graph(self, name)