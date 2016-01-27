import importlib
try:
    urllib = importlib.import_module('urllib.parse')
except ImportError:
    urllib = importlib.import_module('urllib')


class BatchStep(object):

    __slots__ = ('_name', '_request', '_handler')

    def __init__(self, name, request, handler):
        self._name = name
        endpoint = request.endpoint
        if request.params is not None:
            endpoint += '?' + urllib.urlencode(request.params)
        request_string = '{} {} HTTP/1.1'.format(request.method, endpoint)
        if request.headers is not None:
            request_string += ''.join(
                '\r\n{}: {}'.format(k, v)
                for k, v in request.headers.items()
            )
        if request.data is not None:
            request_string += '\r\n\r\n{}'.format(request.data)
        self._request = request_string
        self._handler = handler

    def __repr__(self):
        return "<ArangoDB batch step '{}'>".format(self._name)

    @property
    def request(self):
        return self._request

    @property
    def handler(self):
        return self._handler
