import re

from arango.job import Job
from arango.batch import BatchStep


class Method(object):

    def __init__(self, connection, meta=None):
        """Initialize the API method.

        :param connection: the ArangoDB connection object
        :type connection: arango.connection.Connection
        :param meta: meta data (e.g. collection/graph name)
        :type meta: dict or None
        """
        cls = self.__class__
        self._conn = connection
        self._meta = meta if meta else {}
        self._name = re.sub('(?!^)([A-Z]+)', r'_\1', cls.__name__).lower()
        self.__call__.__func__.__doc__ = cls.__doc__

    def _request(self, *args, **kwargs):
        """Return the request to be executed.

        This method must be overridden by subclasses.

        :return: the ArangoDB request object
        :rtype: arango.
        """
        raise NotImplementedError

    def _handler(self, response):
        """Handle the response from the request.

        This method must be overridden by subclasses.

        :param response: the request response
        :type response: arango.response.Response
        :return: the final output of the call
        :rtype: object
        """
        raise NotImplementedError

    def _transaction(self, *args, **kwargs):
        """Return the transaction step of this request.

        It is optional for subclasses to override this method.

        :return: the transaction step object
        :rtype: arango.transaction.TransactionStep
        """
        raise NotImplementedError

    def _execute(self, request):
        """Execute the request.

        :param request: the request to make
        :type request: arango.api.Request
        :return: response from ArangoDB
        :rtype: arango.response.Response
        :raise: ValueError
        """
        method = request.method
        if method in {'get', 'delete', 'head'}:
            return getattr(self._conn, method)(
                endpoint=request.endpoint,
                headers=request.headers,
                params=request.params,
            )
        elif method in {'post', 'put', 'patch'}:
            return getattr(self._conn, method)(
                endpoint=request.endpoint,
                headers=request.headers,
                params=request.params,
                data=request.data,
            )
        raise ValueError('Unsupported HTTP method {}'.format(method))

    def __repr__(self):
        """Return a descriptive string of this ArangoDB API method.

        :return: description
        :rtype: str
        """
        return "<ArangoDB API method '{}'>".format(self._name)

    def __call__(self, *args, **kwargs):
        """Call this ArangoDB API method.

        :return: BatchStep, TransactionStep, Job or Response object
        :rtype:
            arango.async.Job or
            arango.batch.BatchStep or
            arango.response.Response or
            arango.transaction.TransactionStep
        """
        transaction = kwargs.pop('transaction', False)
        async = kwargs.pop('async', False)
        store = kwargs.pop('store', False)
        batch = kwargs.pop('batch', False)
        if transaction:  # This call is part of a transaction
            try:
                return self._transaction(*args, **kwargs)
            except TypeError as err:
                raise TypeError(str(err).replace('_transaction', self._name))
            except NotImplementedError:
                raise TypeError(
                    '{}() does not support transactions'.format(self._name))
        try:
            request = self._request(*args, **kwargs)
        except TypeError as err:
            raise TypeError(str(err).replace('request', self._name))
        if async:
            def handler(response):
                job_id = response.headers.get('x-arango-async-id', '')
                return Job(self._conn, job_id, self._handler)
            request.headers['x-arango-async'] = 'store' if store else 'true'
        else:
            handler = self._handler
        if batch:
            return BatchStep(self._name, request, handler)
        else:
            return handler(self._execute(request))
