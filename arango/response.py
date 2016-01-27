"""ArangoDB HTTP response."""

from json import loads


class Response(object):
    """ArangoDB HTTP Response class.

    The clients in arango.clients must return an instance of this class.

    :param method: the HTTP method
    :type method: str
    :param url: the request URL
    :type url: str
    :param status_code: the HTTP status code
    :type status_code: int
    :param content: the HTTP response content
    :type content: basestring or str
    :param status_text: the HTTP status description if any
    :type status_text: str or None
    """

    __slots__ = (
        'method',
        'url',
        'headers',
        'status_code',
        'status_text',
        'body'
    )

    def __init__(self, method, url, headers, status_code, status_text, text):
        self.method = method
        self.url = url
        self.headers = headers
        self.status_code = status_code
        self.status_text = status_text
        try:
            self.body = loads(text) if text else None
        except ValueError:
            self.body = None
