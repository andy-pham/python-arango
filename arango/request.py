from __future__ import absolute_import, unicode_literals

from json import dumps

from six import moves


class Request(object):

    __slots__ = ('method', 'endpoint', 'headers', 'params', 'data')

    def __init__(self, method, endpoint, headers=None, params=None, data=None):
        self.method = method
        self.endpoint = endpoint
        self.headers = headers
        self.params = params
        self.data = data

    def __str__(self):
        path = self.endpoint
        if self.params is not None:
            path += "?" + moves.urllib.urlencode(self.params)
        request_string = "{} {} HTTP/1.1".format(self.method, path)
        if self.headers:
            for key, value in self.headers.items():
                request_string += "\r\n{key}: {value}".format(
                    key=key, value=value
                )
        if self.data:
            request_string += "\r\n\r\n{}".format(dumps(self.data))
        return request_string

    @property
    def args(self):
        if self.method in {'put', 'post', 'patch'}:
            return {
                'endpoint': self.endpoint,
                'data': self.data,
                'headers': self.headers,
                'params': self.params,
            }
        return {
            'endpoint': self.endpoint,
            'headers': self.headers,
            'params': self.params
        }
