import json

from arango.utils import is_str


class Request(object):

    __slots__ = ('method', 'endpoint', 'headers', 'params', 'data')

    def __init__(self, method, endpoint, headers=None, params=None, data=None):
        self.method = method.lower()
        self.endpoint = endpoint
        self.headers = {} if headers is None else headers
        self.params = {} if params is None else params
        self.data = json.dumps(data) if is_str(data) else None
