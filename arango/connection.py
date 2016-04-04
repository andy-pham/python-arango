from __future__ import absolute_import, unicode_literals

import json

from six import string_types

from arango.constants import DEFAULT_DB
from arango.clients import DefaultHTTPClient


class APIConnection(object):
    """Connection used to make API calls to ArangoDB.

    :param protocol: the internet transfer protocol (default: 'http')
    :type protocol: str
    :param host: ArangoDB host (default: 'localhost')
    :type host: str
    :param port: ArangoDB port (default: 8529)
    :type port: int or str
    :param username: ArangoDB username (default: 'root')
    :type username: str
    :param password: ArangoDB password (default: '')
    :type password: str
    :param database: the ArangoDB database to point the API calls to
    :type database: str
    :param client: the HTTP client
    :type client: arango.clients.base.BaseHTTPClient or None
    """

    def __init__(self, protocol='http', host='localhost', port=8529,
                 username='root', password='', database=DEFAULT_DB,
                 client=None):
        self._protocol = protocol
        self._host = host
        self._port = port
        self._database = database
        self._url_prefix = '{protocol}://{host}:{port}/_db/{db}'.format(
            protocol=self._protocol,
            host=self._host,
            port=self._port,
            db=self._database,
        )
        self._client = client if client else DefaultHTTPClient()
        self._username = username
        self._password = password

    @property
    def protocol(self):
        """Return the HTTP protocol.

        :returns: the HTTP protocol
        :rtype: str
        """
        return self._protocol

    @property
    def host(self):
        """Return the server host.

        :returns: the server host
        :rtype: str
        """
        return self._host

    @property
    def port(self):
        """Return the server port.

        :returns: the server port
        :rtype: int
        """
        return self._port

    @property
    def db(self):
        """Return the name of the database.

        :returns: the name of the database
        :rtype: str
        """
        return self._database

    def head(self, endpoint, params=None, headers=None):
        """Call a HEAD endpoint in ArangoDB's REST API.

        :param endpoint: the API endpoint
        :type endpoint: str
        :param params: the request parameters
        :type params: dict or None
        :param headers: the request headers
        :type headers: dict or None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self._client.head(
            url=self._url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )

    def get(self, endpoint, params=None, headers=None):
        """Call a GET endpoint in ArangoDB's REST API.

        :param endpoint: the API endpoint
        :type endpoint: str
        :param params: the request parameters
        :type params: dict or None
        :param headers: the request headers
        :type headers: dict or None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self._client.get(
            url=self._url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self._username, self._password),
        )

    def put(self, endpoint, data=None, params=None, headers=None):
        """Call a PUT endpoint in ArangoDB's REST API.

        :param endpoint: the API endpoint
        :type endpoint: str
        :param data: the request payload
        :type data: str or dict or None
        :param params: the request parameters
        :type params: dict or None
        :param headers: the request headers
        :type headers: dict or None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self._client.put(
            url=self._url_prefix + endpoint,
            data=data if isinstance(data, string_types) else json.dumps(data),
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )

    def post(self, endpoint, data=None, params=None, headers=None):
        """Call a POST endpoint in ArangoDB's REST API.

        :param endpoint: the API endpoint
        :type endpoint: str
        :param data: the request payload
        :type data: str or dict or None
        :param params: the request parameters
        :type params: dict or None
        :param headers: the request headers
        :type headers: dict or None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self._client.post(
            url=self._url_prefix + endpoint,
            data=data if isinstance(data, string_types) else json.dumps(data),
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )

    def patch(self, endpoint, data=None, params=None, headers=None):
        """Call a PATCH endpoint in ArangoDB's REST API.

        :param endpoint: the API endpoint
        :type endpoint: str
        :param data: the request payload
        :type data: str or dict or None
        :param params: the request parameters
        :type params: dict or None
        :param headers: the request headers
        :type headers: dict or None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self._client.patch(
            url=self._url_prefix + endpoint,
            data=data if isinstance(data, string_types) else json.dumps(data),
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )

    def delete(self, endpoint, params=None, headers=None):
        """Call a DELETE endpoint in ArangoDB's REST API.

        :param endpoint: the API endpoint
        :type endpoint: str
        :param params: the request parameters
        :type params: dict or None
        :param headers: the request headers
        :type headers: dict or None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self._client.delete(
            url=self._url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )
