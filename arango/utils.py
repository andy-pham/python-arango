"""Utility Functions."""

from re import sub
from json import dumps

from six import moves, string_types

from collections import Mapping, Iterable


def is_str(obj):
    """Return True iff ``obj`` is an instance of str or unicode.

    :param obj: the object to check
    :type obj: object
    :returns: True iff ``obj`` is an instance of str/unicode
    :rtype: bool
    """
    return isinstance(obj, string_types)


def stringify_request(method, path, params=None, headers=None, data=None):
    """Stringify the HTTP request into a string for batch requests.

    :param method: the HTTP method
    :type method: str
    :param path: the API path (e.g. '/_api/version')
    :type path: str
    :param params: the request parameters
    :type params: dict | None
    :param headers: the request headers
    :type headers: dict | None
    :param data: the request payload
    :type data: dict | None
    :returns: the stringified request
    :rtype: str
    """
    if params is not None:
        path += "?" + moves.urllib.urlencode(params)
    request_string = "{} {} HTTP/1.1".format(method, path)
    if headers:
        for key, value in headers.items():
            request_string += "\r\n{key}: {value}".format(
                key=key, value=value
            )
    if data:
        request_string += "\r\n\r\n{}".format(dumps(data))
    return request_string
