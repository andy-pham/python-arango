from __future__ import absolute_import, unicode_literals

from json import loads

from arango.constants import HTTP_OK
from arango.exceptions import (
    BatchInvalidError,
    BatchExecuteError
)


class Batch(object):

    def __init__(self, connection):
        self._conn = connection
        self._requests = []

    def add(self, request):
        self._requests.append(request)

    def run(self):
        data = ""
        for content_id, request in enumerate(self._requests, start=1):
            data += "--XXXsubpartXXX\r\n"
            data += "Content-Type: application/x-arango-batchpart\r\n"
            data += "Content-Id: {}\r\n\r\n".format(content_id)
            data += "{}\r\n".format(request)
        data += "--XXXsubpartXXX--\r\n\r\n"

        res = self._conn.post(
            "/_api/batch",
            headers={
                "Content-Type": "multipart/form-data; boundary=XXXsubpartXXX"
            },
            data=data,
        )
        if res.status_code not in HTTP_OK:
            raise BatchExecuteError(res)
        return [
            loads(string) for string in res.body.split("\r\n") if
            string.startswith("{") and string.endswith("}")
        ] if res.body else []
