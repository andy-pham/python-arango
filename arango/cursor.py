from __future__ import absolute_import, unicode_literals

from arango.constants import HTTP_OK
from arango.exceptions import (
    CursorGetNextError,
    CursorDeleteError,
)


class Cursor(object):

    def __init__(self, connection, response):
        """Fetch from the server cursor and yield.

        :param connection: ArangoDB connection object
        :type connection: arango.connection.Connection
        :param response: ArangoDB response object
        :type response: arango.response.Response
        :raises: CursorExecuteError, CursorDeleteError
        """
        self._conn = connection
        self._id = response.body.get('id')
        self._items = response.body['result']
        self._has_more = response.body['hasMore']

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if not self._items and self._has_more:
            res = self._conn.put("/_api/cursor/{}".format(self._id))
            if res.status_code not in HTTP_OK:
                raise CursorGetNextError(res)
            self._id = res.body.get('id')
            self._items = res.body['result']
            self._has_more = res.body['hasMore']
        elif not self._items and not self._has_more:
            if self._id is not None:
                res = self._conn.delete("/api/cursor/{}".format(self._id))
                if res.status_code not in {404, 202}:
                    raise CursorDeleteError(res)
            raise StopIteration()
        return self._items.pop(0)
