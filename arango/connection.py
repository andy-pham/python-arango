"""Connection wrapper for making API calls to ArangoDB."""

import json

from arango.clients import DefaultHTTPClient
from arango.utils import is_str


class Connection(object):
    """Connection which makes API calls to ArangoDB.

    Instance(s) of this class is passed around to different objects to allow
    communication with the ArangoDB server. Each connection is per database.
    """

    def __init__(self, protocol='http', host='localhost', port=8529,
                 database='_system', username='root', password='',
                 http_client=None):
        """Initialize the connection.

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
        :param database: the name of the ArangoDB database
        :type database: str
        :param http_client: HTTP client to be used to make requests
        :type http_client: arango.clients.base.BaseHTTPClient or None
        """
        self._url_prefix = '{protocol}://{host}:{port}/_db/{db}'.format(
            protocol=protocol, host=host, port=port, db=database
        )
        self._auth = (username, password)
        self._http_client = http_client or DefaultHTTPClient()

    def head(self, path, params=None, headers=None):
        """Call a HEAD method in ArangoDB's REST API.

        :param path: the API endpoint (e.g. '/_api/version')
        :type path: str
        :param params: the request parameters
        :type params: dict or None
        :param headers: the request headers
        :type headers: dict or None
        :returns: the HTTP response from ArangoDB
        :rtype: arango.response.Response
        """
        return self._http_client.head(
            url=self._url_prefix + path,
            params=params,
            headers=headers,
            auth=self._auth
        )

    def get(self, endpoint, params=None, headers=None):
        """Call a GET method in ArangoDB's REST API.
        
        :param endpoint: the API endpoint (e.g. '/_api/version')
        :type endpoint: str
        :param params: the request parameters
        :type params: dict or None
        :param headers: the request headers
        :type headers: dict or None
        :returns: the HTTP response from ArangoDB
        :rtype: arango.response.Response
        """
        return self._http_client.get(
            url=self._url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=self._auth,
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
        :returns: the HTTP response from ArangoDB
        :rtype: arango.response.Response
        """
        return self._http_client.put(
            url=self._url_prefix + endpoint,
            data=data if is_str(data) else json.dumps(data),
            params=params,
            headers=headers,
            auth=self._auth
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
        :returns: the HTTP response from ArangoDB
        :rtype: arango.response.Response
        """
        return self._http_client.post(
            url=self._url_prefix + endpoint,
            data=data if is_str(data) else json.dumps(data),
            params=params,
            headers=headers,
            auth=self._auth
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
        :returns: the HTTP response from ArangoDB
        :rtype: arango.response.Response
        """
        return self._http_client.patch(
            url=self._url_prefix + endpoint,
            data=data if is_str(data) else json.dumps(data),
            params=params,
            headers=headers,
            auth=self._auth
        )

    def delete(self, endpoint, params=None, headers=None):
        """Call a DELETE method in ArangoDB's REST API.
        
        :param endpoint: the API endpoint (e.g. '/_api/version')
        :type endpoint: str
        :param params: the request parameters
        :type params: dict or None
        :param headers: the request headers
        :type headers: dict or None
        :returns: the HTTP response from ArangoDB
        :rtype: arango.response.Response
        """
        return self._http_client.delete(
            url=self._url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=self._auth
        )
