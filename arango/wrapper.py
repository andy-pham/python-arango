from __future__ import absolute_import, unicode_literals

from functools import wraps

_getattr = object.__getattribute__


class APIWrapper(object):
    """ArangoDB API wrapper.

    This is the base class for the following:
        - arango.collection.Collection
        - arango.graph.Graph
    """

    def __getattribute__(self, attr):
        conn = _getattr(self, '_conn')
        if conn.type == 'normal':
            if attr.startswith('_') or attr == 'name':
                return _getattr(self, attr)
            method = _getattr(self, attr)

            @wraps(method)
            def wrapped_method(*args, **kwargs):
                req, handler = method(*args, **kwargs)
                res = getattr(conn, req.method)(**req.kwargs)
                return handler(res)
            return wrapped_method

        elif conn.type == 'batch':
            if attr.startswith('_') or attr == 'name':
                return _getattr(self, attr)
            method = _getattr(self, attr)

            @wraps(method)
            def wrapped_method(*args, **kwargs):
                req, handler = method(*args, **kwargs)
                conn.add(req, handler)
                return True
            return wrapped_method
