"""Wrapper for making REST API calls to ArangoDB."""

import json

from arango.constants import DEFAULT_DATABASE
from arango.clients import DefaultClient
from arango.utils import is_string


class API(object):
    """Wrapper object which makes REST API calls to ArangoDB.

    This wrapper sits right on top of the HTTP client and contains the name
    of the ArangoDB database to direct all API calls to.

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
    :param client: HTTP client for this wrapper to use
    :type client: arango.clients.base.BaseClient or None
    """

    def __init__(self, protocol="http", host="localhost", port=8529,
                 username="root", password="", database=None, client=None):
        self.protocol = protocol
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = DEFAULT_DATABASE if database is None else database
        self.url_prefix = "{protocol}://{host}:{port}/_db/{database}".format(
            protocol=self.protocol,
            host=self.host,
            port=self.port,
            database=self.database,
        )
        self.client = DefaultClient() if client is None else client

    def head(self, endpoint, params=None, headers=None):
        """Call a HEAD method in ArangoDB's REST API.

        :param endpoint: the API endpoint (e.g. '/_api/version')
        :type endpoint: str
        :param params: the request parameters
        :type params: dict or None
        :param headers: the request headers
        :type headers: dict or None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self.client.head(
            url=self.url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self.username, self.password)
        )

    def get(self, endpoint, params=None, headers=None):
        """Call a GET method in ArangoDB's REST API.

        :param endpoint: the API endpoint (e.g. '/_api/version')
        :type endpoint: str
        :param params: the request parameters
        :type params: dict or None
        :param headers: the request headers
        :type headers: dict or None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self.client.get(
            url=self.url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self.username, self.password),
        )

    def put(self, endpoint, data=None, params=None, headers=None):
        """Call a PUT method in ArangoDB's REST API.

        :param endpoint: the API endpoint (e.g. '/_api/version')
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
        return self.client.put(
            url=self.url_prefix + endpoint,
            data=data if is_string(data) else json.dumps(data),
            params=params,
            headers=headers,
            auth=(self.username, self.password)
        )

    def post(self, endpoint, data=None, params=None, headers=None):
        """Call a POST method in ArangoDB's REST API.

        :param endpoint: the API endpoint (e.g. '/_api/version')
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
        return self.client.post(
            url=self.url_prefix + endpoint,
            data=data if is_string(data) else json.dumps(data),
            params=params,
            headers=headers,
            auth=(self.username, self.password)
        )

    def patch(self, endpoint, data=None, params=None, headers=None):
        """Call a PATCH method in ArangoDB's REST API.

        :param endpoint: the API endpoint (e.g. '/_api/version')
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
        return self.client.patch(
            url=self.url_prefix + endpoint,
            data=data if is_string(data) else json.dumps(data),
            params=params,
            headers=headers,
            auth=(self.username, self.password)
        )

    def delete(self, endpoint, params=None, headers=None):
        """Call a DELETE method in ArangoDB's REST API.

        :param endpoint: the API endpoint (e.g. '/_api/version')
        :type endpoint: str
        :param params: the request parameters
        :type params: dict or None
        :param headers: the request headers
        :type headers: dict or None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self.client.delete(
            url=self.url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self.username, self.password)
        )

    def options(self, endpoint, data=None, params=None, headers=None):
        """Call an OPTIONS method in ArangoDB's REST API.

        :param endpoint: the API endpoint (e.g. '/_api/version')
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
        return self.client.options(
            url=self.url_prefix + endpoint,
            data=data if is_string(data) else json.dumps(data),
            params=params,
            headers=headers,
            auth=(self.username, self.password)
        )