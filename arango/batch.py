from __future__ import absolute_import, unicode_literals

from arango.collection import Collection
from arango.graph import Graph
from arango.constants import HTTP_OK
from arango.exceptions import BatchExecuteError
from arango.response import Response


class Batch(object):
    """ArangoDB batch query object.

    :param connection: ArangoDB connection object
    :type connection: arango.connection.Connection
    """

    def __init__(self, connection, return_result=True):
        self._conn = connection
        self._requests = []
        self._handlers = []
        self._type = 'batch'
        self._return = return_result

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return self.commit()

    def __repr__(self):
        return '<ArangoDB batch query ({} requests)>'.format(
            len(self._requests)
        )

    @property
    def type(self):
        return self._type

    def add(self, request, handler):
        """Add a request to the batch query.

        This method should only be called internally.

        :param request: the ArangoDB request object
        :type request: arango.request.Request
        :param handler: the handler function
        :type handler: callable
        :return:
        """
        self._requests.append(request)
        self._handlers.append(handler)

    def commit(self):
        """Execute the batch query in call.

        If ``return_response`` was is to True, the responses are returned in
        the same order as the requests added.

        :returns: the result
        :rtype: list | None
        :raises: BatchExecuteError
        """
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
            if not self._return:
                return
            responses = []
            url_prefix = self._conn.url_prefix
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
                except Exception as err:
                    responses.append(err)
                else:
                    responses.append(result)
            return responses
        finally:
            self._requests, self._handlers = [], []

    def collection(self, name, edge=False):
        """Return the Collection object of the specified name.

        :param name: the name of the collection
        :type name: str
        :param edge: whether this collection is an edge collection
        :type edge: bool
        :returns: the requested collection object
        :rtype: arango.collection.Collection
        :raises: TypeError
        """
        return Collection(self, name, edge)

    def graph(self, name):
        """Return the Graph object of the specified name.

        :param name: the name of the graph
        :type name: str
        :returns: the requested graph object
        :rtype: arango.graph.Graph
        :raises: TypeError, GraphNotFound
        """
        return Graph(self, name)
