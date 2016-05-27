from __future__ import absolute_import, unicode_literals

import json

from arango.cursor import Cursor
from arango.constants import (
    HTTP_OK,
    COLLECTION_STATUSES
)
from arango.exceptions import *
from arango.request import Request
from arango.wrapper import APIWrapper


class BaseCollection(APIWrapper):
    """Wrapper for ArangoDB collection-specific API endpoints.

    :param connection: ArangoDB API connection object
    :type connection: arango.connection.Connection | arango.batch.Batch
    :param name: the name of this collection
    :type name: str
    """
    def __init__(self, connection, name, edge=False):
        self._conn = connection
        self._name = name
        self._type = 'edge' if edge else 'document'

    def __iter__(self):
        """Iterate through the documents in this collection.

        :returns: the document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentGetAllError
        """
        res = self._conn.put(
            endpoint='/_api/simple/all',
            data={'collection': self._name}
        )
        if res.status_code not in HTTP_OK:
            raise DocumentGetAllError(res)
        return Cursor(self._conn, res)

    def __len__(self):
        """Return the number of documents in this collection.

        :returns: the number of documents
        :rtype: int
        :raises: CollectionGetCountError
        """
        res = self._conn.get('/_api/collection/{}/count'.format(self._name))
        if res.status_code not in HTTP_OK:
            raise CollectionGetCountError(res)
        return res.body['count']

    def __getitem__(self, key):
        """Return the document of the given key from this collection.

        :param key: the document key
        :type key: str
        :returns: the requested document
        :rtype: dict
        """
        res = self._conn.get(
            '/_api/{}/{}/{}'.format(self._type, self._name, key)
        )
        if res.status_code in {412, 304}:
            raise DocumentRevisionError(res)
        elif res.status_code == 404:
            return None
        elif res.status_code not in HTTP_OK:
            raise DocumentGetError(res)
        return res.body

    def __contains__(self, key):
        """Return True if the document exists in this collection.

        :param key: the document key
        :type key: str
        :returns: True if the document exists, else False
        :rtype: bool
        :raises: DocumentGetError
        """
        res = self._conn.head(
            '/_api/{}/{}/{}'.format(self._type, self._name, key)
        )
        if res.status_code == 200:
            return True
        elif res.status_code == 404:
            return False
        raise DocumentGetError(res)

    def _validate(self, document):
        """Validate the document for correctness.

        :param document: the document to validate
        :type document: dict
        :raises: ValueError
        """
        if self._type == 'edge':
            if '_from' not in document:
                raise ValueError("'_from' field missing in the edge document")
            if '_to' not in document:
                raise ValueError("'_to' field missing in the edge document")
        return document

    def _edge_params(self, document, params):
        """Populate the request parameters with '_from' and '_to' fields.

        :param document: the document
        :type document: dict
        :param params: the request parameters
        :type params: dict
        """
        if self._type == 'edge':
            self._validate(document)
            params['from'] = document['_from']
            params['to'] = document['_to']

    @staticmethod
    def _status(code):
        """Return the collection status text.

        :param code: the status code
        :type code: int
        :returns: the status text
        :rtype: str
        """
        return COLLECTION_STATUSES.get(
            code, 'corrupted ({})'.format(code)
        )

    @property
    def name(self):
        """Return the name of this collection.

        :returns: the name of this collection
        :rtype: str
        """
        return self._name

    def rename(self, new_name):
        """Rename this collection.

        :param new_name: the new name for the collection
        :type new_name: str
        :returns: whether the rename was successful
        :rtype: bool
        :raises: CollectionRenameError
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/rename'.format(self._name),
            data={'name': new_name}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionRenameError(res)
            self._name = new_name
            return True

        return request, handler

    def statistics(self):
        """Return the statistics of this collection.

        :returns: the statistics of this collection
        :rtype: dict
        :raises: CollectionGetStatisticsError
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/figures'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionGetStatisticsError(res)
            stats = res.body['figures']
            stats['compaction_status'] = stats.pop('compactionStatus', None)
            stats['document_refs'] = stats.pop('documentReferences', None)
            stats['last_tick'] = stats.pop('lastTick', None)
            stats['waiting_for'] = stats.pop('waitingFor', None)
            stats['uncollected_logfile_entries'] = stats.pop(
                'uncollectedLogfileEntries', None
            )
            return stats

        return request, handler

    def revision(self):
        """Return the revision of this collection.

        :returns: the collection revision (aka etag)
        :rtype: str
        :raises: CollectionGetRevisionError
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/revision'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionGetRevisionError(res)
            return res.body['revision']

        return request, handler

    def options(self):
        """Return the properties of this collection.

        :returns: the collection properties
        :rtype: dict
        :raises: CollectionGetError
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/properties'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionGetPropertiesError(res)
            result = {
                'id': res.body['id'],
                'name': res.body['name'],
                'edge': res.body['type'] == 3,
                'sync': res.body['waitForSync'],
                'status': self._status(res.body['status']),
                'compact': res.body['doCompact'],
                'system': res.body['isSystem'],
                'volatile': res.body['isVolatile'],
                'journal_size': res.body['journalSize'],
                'keygen': res.body['keyOptions']['type'],
                'user_keys': res.body['keyOptions']['allowUserKeys'],
            }
            if 'increment' in res.body['keyOptions']:
                result['key_increment'] = res.body['keyOptions']['increment']
            if 'offset' in res.body['keyOptions']:
                result['key_offset'] = res.body['keyOptions']['offset']
            return result

        return request, handler

    def set_options(self, sync=None, journal_size=None):
        """Set the options of this collection.

        :param sync: force operations to block until synced to disk
        :type sync: bool | None
        :param journal_size: the size of the journal
        :type journal_size: int
        :returns: whether or not the set operation was successful
        :rtype: bool
        :raises: CollectionSetPropertiesError
        """
        data = {}
        if sync is not None:
            data['waitForSync'] = sync
        if journal_size is not None:
            data['journalSize'] = journal_size

        request = Request(
            method='put',
            endpoint='/_api/collection/{}/properties'.format(self._name),
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionSetPropertiesError(res)
            return not res.body['error']
        return request, handler

    def load(self):
        """Load this collection into memory.

        :returns: the status of the collection
        :rtype: str
        :raises: CollectionLoadError
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/load'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionLoadError(res)
            return self._status(res.body['status'])
        return request, handler

    def unload(self):
        """Unload this collection from memory.

        :returns: the status of the collection
        :rtype: str
        :raises: CollectionUnloadError
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/unload'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionUnloadError(res)
            return self._status(res.body['status'])
        return request, handler

    def rotate(self):
        """Rotate the journal of this collection.

        :raises: CollectionRotateJournalError
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/rotate'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionRotateError(res)
            return res.body['result']
        return request, handler

    def checksum(self, revision=False, data=False):
        """Return the checksum of this collection.

        :param revision: include the revision in the checksum calculation
        :type revision: bool
        :param data: include the data in the checksum calculation
        :type data: bool
        :returns: the checksum
        :rtype: int
        :raises: CollectionGetChecksumError
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/checksum'.format(self._name),
            params={'withRevision': revision, 'withData': data}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionGetPropertiesError(res)
            return res.body['checksum']
        return request, handler

    def truncate(self):
        """Delete all documents from this collection.

        :returns: whether the trucate was successful
        :rtype: bool
        :raises: CollectionTruncateError
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/truncate'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionTruncateError(res)
            return not res.body['error']
        return request, handler

    def count(self):
        """Return the number of documents in this collection.

        :returns: the number of documents
        :rtype: int
        :raises: CollectionGetCountError
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/count'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionGetCountError(res)
            return res.body['count']

        return request, handler

    def has(self, key):
        """Return True if the document exists in this collection.

        :param key: the document key
        :type key: str
        :returns: True if the document exists, else False
        :rtype: bool
        :raises: DocumentGetError
        """
        request = Request(
            method='head',
            endpoint='/_api/{}/{}/{}'.format(self._type, self._name, key),
        )

        def handler(res):
            if res.status_code == 200:
                return True
            elif res.status_code == 404:
                return False
            raise DocumentGetError(res)

        return request, handler

    def find(self, filters):
        """Return the first document matching the given example document body.

        :param filters: the match filters
        :type filters: dict
        :returns: the first matching document
        :rtype: dict | None
        :raises: DocumentFindOneError
        """
        data = {'collection': self._name, 'example': filters}
        request = Request(
            method='put',
            endpoint='/_api/simple/first-example',
            data=data
        )

        def handler(res):
            if res.status_code == 404:
                return None
            elif res.status_code not in HTTP_OK:
                raise DocumentFindOneError(res)
            return res.body['document']
        return request, handler

    def find_many(self, filters, offset=None, limit=None):
        """Return all documents matching the given example document body.

        :param filters: the match filters
        :type filters: dict
        :param offset: the number of documents to skip
        :type offset: int
        :param limit: maximum number of documents to return
        :type limit: int
        :returns: document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFindManyError
        """
        data = {'collection': self._name, 'example': filters}
        if offset is not None:
            data['skip'] = offset
        if limit is not None:
            data['limit'] = limit

        request = Request(
            method='put',
            endpoint='/_api/simple/by-example',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFindManyError(res)
            return Cursor(self._conn, res)
        return request, handler

    ############################
    # Document Import & Export #
    ############################

    # TODO look into this endpoint for better documentation and testing
    def export(self, flush=None, flush_wait=None, count=None,
               batch_size=None, limit=None, ttl=None, restrict=None):
        """"Export all documents from this collection using a cursor.

        :param flush: trigger a WAL flush operation prior to the export
        :type flush: bool | None
        :param flush_wait: the max wait time in sec for flush operation
        :type flush_wait: int | None
        :param count: whether the count is returned in an attribute of result
        :type count: bool | None
        :param batch_size: the max number of result documents in one roundtrip
        :type batch_size: int | None
        :param limit: the max number of documents to be included in the cursor
        :type limit: int | None
        :param ttl: time-to-live for the cursor on the server
        :type ttl: int | None
        :param restrict: object with attributes to be excluded/included
        :type restrict: dict
        :returns: document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentsExportError
        """
        params = {'collection': self._name}
        options = {}
        if flush is not None:
            options['flush'] = flush
        if flush_wait is not None:
            options['flushWait'] = flush_wait
        if count is not None:
            options['count'] = count
        if batch_size is not None:
            options['batchSize'] = batch_size
        if limit is not None:
            options['limit'] = limit
        if ttl is not None:
            options['ttl'] = ttl
        if restrict is not None:
            options['restrict'] = restrict
        data = {'options': options} if options else {}

        request = Request(
            method='post',
            endpoint='/_api/export',
            params=params,
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentsExportError(res)
            return Cursor(self._conn, res)

        return request, handler

    ##################
    # Simple Queries #
    ##################

    def first(self, count=0):
        """Return the first ``count`` number of documents in this collection.

        :param count: the number of documents to return
        :type count: int
        :returns: the list of documents or first document if ``count`` is 0
        :rtype: list | dict
        :raises: DocumentGetFirstError
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/first',
            data={'collection': self._name, 'count': count}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetFirstError(res)
            return res.body['result']

        return request, handler

    def last(self, count=0):
        """Return the last ``count`` number of documents in this collection.

        :param count: the number of documents to return
        :type count: int
        :returns: the list of documents
        :rtype: list | dict
        :raises: DocumentGetLastError
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/last',
            data={'collection': self._name, 'count': count}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetLastError(res)
            return res.body['result']

        return request, handler

    def all(self, offset=None, limit=None):
        """Return all documents in this collection.

        ``skip`` is applied before ``limit`` if both are provided.

        :param offset: the number of documents to skip
        :type offset: int
        :param limit: maximum number of documents to return
        :type limit: int
        :returns: document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentGetAllError
        """
        data = {'collection': self._name}
        if offset is not None:
            data['skip'] = offset
        if limit is not None:
            data['limit'] = limit

        request = Request(
            method='put',
            endpoint='/_api/simple/all',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetAllError(res)
            return Cursor(self._conn, res)

        return request, handler

    def random(self):
        """Return a random document from this collection.

        :returns: the random document
        :rtype: dict
        :raises: DocumentGetRandomError
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/any',
            data={'collection': self._name}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetRandomError(res)
            return res.body['document']

        return request, handler

    def find_near(self, latitude, longitude, limit=None):
        """Return all documents near the given coordinate.

        The default will find at most 100 documents near the given coordinate.
        Documents returned are sorted according to distance, with the nearest
        document being the first. If there are documents of equal distance,
        they will be randomly chosen from the set until the limit is reached.
        In order to execute this query a geo index must be defined for the
        collection.

        :param latitude: the latitude
        :type latitude: int
        :param longitude: the longitude
        :type longitude: int
        :param limit: the maximum number of documents to return
        :type limit: int | None
        :returns: document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFindNearError
        """
        full_query = """
        FOR doc IN NEAR(@collection, @latitude, @longitude{})
            RETURN doc
        """.format(', @limit' if limit is not None else '')

        bind_vars = {
            'collection': self._name,
            'latitude': latitude,
            'longitude': longitude
        }
        if limit is not None:
            bind_vars['limit'] = limit

        request = Request(
            method='post',
            endpoint='/_api/cursor',
            data={'query': full_query, 'bindVars': bind_vars}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFindNearError(res)
            return Cursor(self._conn, res)

        return request, handler

    def find_in_range(self, field, lower, upper, offset=0, limit=100,
                      include=True):
        """Return all documents that are within the given range.

        The returned documents are ordered randomly. If ``distance_field`` is
        specified, the distance between the coordinate and the documents are
        returned using the argument value as the field name. In order to
        execute this query a geo index must be defined for the collection.

        :param field: the name of the field to use
        :type field: str
        :param lower: the lower bound
        :type lower: int
        :param upper: the upper bound
        :type upper: int
        :param offset: the number of documents to skip
        :type offset: int | None
        :param limit: the maximum number of documents to return
        :type limit: int | None
        :param include: whether to include the endpoints or not
        :type include: bool
        :returns: document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFindInRangeError
        """
        if include:
            full_query = """
            FOR doc IN @@collection
                FILTER doc.@field >= @lower && doc.@field <= @upper
                LIMIT @skip, @limit
                RETURN doc
            """
        else:
            full_query = """
            FOR doc IN @@collection
                FILTER doc.@field > @lower && doc.@field < @upper
                LIMIT @skip, @limit
                RETURN doc
            """
        bind_vars = {
            '@collection': self._name,
            'field': field,
            'lower': lower,
            'upper': upper,
            'skip': offset,
            'limit': limit
        }

        request = Request(
            method='post',
            endpoint='/_api/cursor',
            data={'query': full_query, 'bindVars': bind_vars}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFindInRangeError(res)
            return Cursor(self._conn, res)

        return request, handler

    # TODO the WITHIN geo function does not seem to work properly
    def find_in_radius(self, latitude, longitude, radius, distance_field=None):
        """Return all documents that are within the given radius.

        The returned documents are ordered randomly. If ``distance_field`` is
        specified, the distance between the coordinate and the documents are
        returned using the argument value as the field name. In order to
        execute this query a geo index must be defined for the collection.

        :param latitude: the latitude
        :type latitude: int
        :param longitude: the longitude
        :type longitude: int
        :param radius: the maximum radius
        :type radius: int
        :param distance_field: the name of the field containing the distance
        :type distance_field: str
        :returns: document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFindInRadiusError
        """
        full_query = """
        FOR doc IN WITHIN(@collection, @latitude, @longitude, @radius{})
            RETURN doc
        """.format(', @distance' if distance_field is not None else '')

        bind_vars = {
            'collection': self._name,
            'latitude': latitude,
            'longitude': longitude,
            'radius': radius
        }
        if distance_field is not None:
            bind_vars['distance'] = distance_field

        request = Request(
            method='post',
            endpoint='/_api/cursor',
            data={'query': full_query, 'bindVars': bind_vars}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFindInRadiusError(res)
            return Cursor(self._conn, res)

        return request, handler

    def find_in_square(self, latitude1, longitude1, latitude2, longitude2,
                       skip=None, limit=None, geo=None):
        """Return all documents in a rectangle around the given coordinate.

        In order to execute this query a geo index must be defined for the
        collection. If there are more than one geo-spatial index, the ``geo``
        argument can be used to select a particular index.

        :param latitude1: the latitude of the first rectangle coordinate
        :type latitude1: int
        :param longitude1: the longitude of the first rectangle coordinate
        :type longitude1: int
        :param latitude2: the latitude of the second rectangle coordinate
        :type latitude2: int
        :param longitude2: the longitude of the second rectangle coordinate
        :type longitude2: int
        :param skip: the number of documents to skip
        :type skip: int
        :param limit: maximum number of documents to return
        :type limit: int
        :param geo: the field to use (must have geo-index)
        :type geo: str
        :returns: document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFindInRectangleError
        """
        data = {
            'collection': self._name,
            'latitude1': latitude1,
            'longitude1': longitude1,
            'latitude2': latitude2,
            'longitude2': longitude2,
        }
        if skip is not None:
            data['skip'] = skip
        if limit is not None:
            data['limit'] = limit
        if geo is not None:
            data['geo'] = geo

        request = Request(
            method='put',
            endpoint='/_api/simple/within-rectangle',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFindInRectangleError(res)
            return Cursor(self._conn, res)

        return request, handler

    def find_text(self, field, query, limit=None):
        """Return all documents that match the specified fulltext ``query``.

        In order to execute this query a fulltext index must be defined for the
        collection and the specified attribute.

        :param field: the attribute path with a fulltext index
        :type field: str
        :param query: the fulltext query
        :type query: str
        :param limit: maximum number of documents to return
        :type limit: int
        :returns: document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFindTextError
        """
        full_query = """
        FOR doc IN FULLTEXT(@collection, @field, @query{})
            RETURN doc
        """.format(', @limit' if limit is not None else '')

        bind_vars = {
            'collection': self._name,
            'field': field,
            'query': query
        }
        if limit is not None:
            bind_vars['limit'] = limit

        request = Request(
            method='post',
            endpoint='/_api/cursor',
            data={'query': full_query, 'bindVars': bind_vars}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFindTextError(res)
            return Cursor(self._conn, res)

        return request, handler

    ####################
    # Index Management #
    ####################

    def indexes(self):
        """Return the details on the indexes of this collection.

        :returns: the index details
        :rtype: dict
        :raises: IndexListError
        """
        request = Request(
            method='get',
            endpoint='/_api/index?collection={}'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise IndexListError(res)

            indexes = {}
            for index_id, details in res.body['identifiers'].items():
                if 'id' in details:
                    del details['id']
                if 'minLength' in details:
                    details['min_length'] = details.pop('minLength')
                if 'byteSize' in details:
                    details['byte_size'] = details.pop('byteSize')
                if 'geoJson' in details:
                    details['geo_json'] = details.pop('geoJson')
                if 'ignoreNull' in details:
                    details['ignore_none'] = details.pop('ignoreNull')
                if 'selectivityEstimate' in details:
                    details['selectivity'] = details.pop('selectivityEstimate')
                if 'isNewlyCreated' in details:
                    details['new'] = details.pop('isNewlyCreated')
                indexes[index_id.split('/', 1)[1]] = details
            return indexes

        return request, handler

    def _add_index(self, data):
        """Helper method for creating new indexes."""
        request = Request(
            method='post',
            endpoint='/_api/index?collection={}'.format(self._name),
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise IndexCreateError(res)
            details = res.body
            if 'minLength' in details:
                details['min_length'] = details.pop('minLength')
            if 'byteSize' in details:
                details['byte_size'] = details.pop('byteSize')
            if 'geoJson' in details:
                details['geo_json'] = details.pop('geoJson')
            if 'ignoreNull' in details:
                details['ignore_none'] = details.pop('ignoreNull')
            if 'selectivityEstimate' in details:
                details['selectivity'] = details.pop('selectivityEstimate')
            if 'isNewlyCreated' in details:
                details['new'] = details.pop('isNewlyCreated')
            return details

        return request, handler

    def add_hash_index(self, fields, unique=None, sparse=None):
        """Create a new hash index to this collection.

        :param fields: the attribute paths to index
        :type fields: list
        :param unique: whether or not the index is unique
        :type unique: bool | None
        :param sparse: whether to index attr values of null
        :type sparse: bool | None
        :raises: IndexCreateError
        """
        data = {'type': 'hash', 'fields': fields}
        if unique is not None:
            data['unique'] = unique
        if sparse is not None:
            data['sparse'] = sparse
        return self._add_index(data)

    def add_cap_constraint(self, size=None, byte_size=None):
        """Create a cap constraint to this collection.

        :param size: the number for documents allowed in this collection
        :type size: int | None
        :param byte_size: the max size of the active document data (> 16384)
        :type byte_size: int | None
        :raises: IndexCreateError
        """
        data = {'type': 'cap'}
        if size is not None:
            data['size'] = size
        if byte_size is not None:
            data['byteSize'] = byte_size
        return self._add_index(data)

    def add_skiplist_index(self, fields, unique=None, sparse=None):
        """Create a new skiplist index to this collection.

        A skiplist index is used to find ranges of documents (e.g. time).

        :param fields: the attribute paths to index
        :type fields: list
        :param unique: whether or not the index is unique
        :type unique: bool | None
        :param sparse: whether to index attr values of null
        :type sparse: bool | None
        :raises: IndexCreateError
        """
        data = {'type': 'skiplist', 'fields': fields}
        if unique is not None:
            data['unique'] = unique
        if sparse is not None:
            data['sparse'] = sparse
        return self._add_index(data)

    def add_geo_index(self, fields, geo_json=None, unique=None,
                      ignore_none=None):
        """Create a geo index to this collection

        If ``fields`` is a list with ONE attribute path, then a geo-spatial
        index on all documents is created using the value at the path as the
        coordinate. The value must be a list with at least two doubles. The
        list must contain the latitude (first value) and the longitude (second
        value). All documents without the attribute paths or with invalid
        values are ignored.

        If ``fields`` is a list with TWO attribute paths (i.e. latitude and
        longitude, in that order) then a geo-spatial index on all documents is
        created using the two attributes (again, their values must be doubles).
        All documents without the attribute paths or with invalid values are
        ignored.

        :param fields: the attribute paths to index (length must be <= 2)
        :type fields: list
        :param geo_json: whether or not the order is longitude -> latitude
        :type geo_json: bool | None
        :param unique: whether or not to create a geo-spatial constraint
        :type unique: bool | None
        :param ignore_none: ignore docs with None in latitude/longitude
        :type ignore_none: bool | None
        :raises: IndexCreateError
        """
        data = {'type': 'geo', 'fields': fields}
        if geo_json is not None:
            data['geoJson'] = geo_json
        if unique is not None:
            data['unique'] = unique
        if ignore_none is not None:
            data['ignore_null'] = ignore_none
        return self._add_index(data)

    def add_fulltext_index(self, fields, min_length=None):
        """Create a fulltext index to this collection.

        A fulltext index is used to find words or prefixes of words, and can
        be set on one field only. Only words with textual values of minimum
        length are indexed. Word tokenization is done using the word boundary
        analysis provided by libicu, which uses the language selected during
        server startup. Words are indexed in their lower-cased form. The index
        supports complete match queries (full words) and prefix queries.

        :param fields: the fields to index (length must be > 1)
        :type fields: list
        :param min_length: minimum character length of words to index
        :type min_length: int
        :raises: IndexCreateError
        """
        data = {'type': 'fulltext', 'fields': fields}
        if min_length is not None:
            data['minLength'] = min_length
        return self._add_index(data)

    def delete_index(self, index_id):
        """Delete an index from the collection.

        :param index_id: the ID of the index to remove
        :type index_id: str
        :raises: IndexDeleteError
        """
        request = Request(
            method='delete',
            endpoint='/_api/index/{}/{}'.format(self._name, index_id)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise IndexDeleteError(res)
            return res.body

        return request, handler


class Collection(BaseCollection):
    """Wrapper for ArangoDB's collection-specific APIs.

    :param connection: ArangoDB API connection object
    :type connection: arango.connection.Connection | arango.batch.Batch
    :param name: the name of this collection
    :type name: str
    """

    def __init__(self, connection, name, edge=False):
        super(Collection, self).__init__(connection, name, edge)

    def __repr__(self):
        return "<ArangoDB {} collection '{}'>".format(self._type, self._name)

    @property
    def name(self):
        """Return the name of this collection.

        :returns: the name of this collection
        :rtype: str
        """
        return self._name

    def get(self, key, revision=None, match=True):
        """Return the document of with the specified key.

        If ``revision`` is specified, it is compared against the revision of
        the retrieved document. If ``match`` is set to True and the revisions
        do NOT match, or if ``match`` is set to False and the revisions DO
        match, ``DocumentRevisionError`` is raised.

        :param key: the document key
        :type key: str
        :param revision: the document revision
        :type revision: str | None
        :param match: whether or not the revision should match
        :type match: bool
        :returns: the requested document | None if not found
        :rtype: dict | None
        :raises: DocumentRevisionError, DocumentGetError
        """
        request = Request(
            method='get',
            endpoint='/_api/{}/{}/{}'.format(self._type, self._name, key),
            headers={
                'If-Match' if match else 'If-None-Match': revision
            } if revision else {}
        )

        def handler(res):
            if res.status_code in {412, 304}:
                raise DocumentRevisionError(res)
            elif res.status_code == 404:
                return None
            elif res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return res.body
        return request, handler

    def get_many(self, keys):
        """Return all documents whose key is in ``keys``.

        :param keys: the document keys
        :type keys: list
        :returns: the list of documents
        :rtype: list
        :raises: DocumentGetManyError
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/lookup-by-keys',
            data={'collection': self._name, 'keys': keys}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return res.body['documents']
        return request, handler

    def insert(self, document, sync=False):
        """Insert a single document into the collection.

        If ``data`` contains the ``_key`` field, its value will be used as the
        key of the new vertex. The must be unique in the vertex collection.

        :param document: the body of the new document
        :type document: dict
        :param sync: wait for create to sync to disk
        :type sync: bool
        :returns: the ID, rev and key of the new document
        :rtype: dict
        :raises:
            DocumentInsertError,
            DocumentInvalidError,
            CollectionNotFoundError
        """
        params = {
            'collection': self._name,
            'waitForSync': sync,
        }
        self._edge_params(document, params)
        request = Request(
            method='post',
            endpoint='/_api/{}'.format(self._type),
            data=self._validate(document),
            params=params
        )

        def handler(res):
            if res.status_code == 400:
                raise DocumentInvalidError(res)
            elif res.status_code == 404:
                raise CollectionNotFoundError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentInsertError(res)
            return res.body
        return request, handler

    def insert_many(self, documents, halt_on_error=True, details=True):
        """Insert documents into the collection in bulk.

        The insert does not retain the order of the documents.

        If ``complete`` is set to False, valid documents
        will be imported while invalid ones are rejected, meaning only some of
        the uploaded documents might have been imported.

        If ``details`` parameter is set to True, the response will also contain
        ``details`` attribute which is a list of detailed error messages.

        :param documents: list of documents to import
        :type documents: list
        :param halt_on_error: stop the insert on an invalid document
        :type halt_on_error: bool
        :param details: return details about invalid documents
        :type details: bool
        :returns: the result of the bulk insert
        :rtype: dict
        :raises: DocumentsInsertManyError
        """
        request = Request(
            method='post',
            endpoint='/_api/import',
            data='\r\n'.join([
                json.dumps(self._validate(document))
                for document in documents
            ]),
            params={
                'type': 'documents',
                'collection': self._name,
                'complete': halt_on_error,
                'details': details
            }
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentInsertError(res)
            del res.body['error']
            return res.body
        return request, handler

    def update(self, key, data, revision=None, merge=True, keep_none=True,
               sync=False):
        """Update the specified document in the collection.

        If ``keep_none`` is set to True, fields with value None are retained.
        Otherwise, the fields are removed completely from the vertex.

        If ``data`` contains the ``_key`` field, the field is ignored.

        If ``data`` contains the ``_rev`` field, or if ``revision`` is given,
        the revision of the target vertex must match against its value.
        Otherwise, VertexRevision is raised.

        For edge collections, the ``_from`` and ``_to`` attributes are
        immutable, and they are ignored if present in ``data``.

        :param key: the document key
        :type key: str
        :param data: the body of to update the document with
        :type data: dict
        :param revision: the document revision
        :type revision: str | None
        :param merge: whether to merge or overwrite sub-dictionaries
        :type merge: bool | None
        :param keep_none: whether or not to keep the items with value None
        :type keep_none: bool
        :param sync: wait for the update to sync to disk
        :type sync: bool
        :returns: the ID, revision and key of the updated vertex
        :rtype: dict
        :raises: DocumentRevisionError, DocumentUpdateError
        """
        params = {
            'waitForSync': sync,
            'keepNull': keep_none,
            'mergeObjects': merge
        }
        if revision is not None:
            params['rev'] = revision
            params['policy'] = 'error'
        elif '_rev' in data:
            params['rev'] = data['_rev']
            params['policy'] = 'error'

        request = Request(
            method='patch',
            endpoint='/_api/{}/{}/{}'.format(
                self._type, self._name, key
            ),
            data=data,
            params=params
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            if res.status_code not in HTTP_OK:
                raise DocumentUpdateError(res)
            del res.body['error']
            return res.body

        return request, handler

    def find_and_update(self, filters, data, limit=None, keep_none=True,
                        sync=False):
        """Update all documents matching the given example document body.

        :param filters: the match filter
        :type filters: dict
        :param data: the new document body to update with
        :type data: dict
        :param limit: maximum number of documents to return
        :type limit: int
        :param keep_none: whether or not to keep the None values
        :type keep_none: bool
        :param sync: wait for the update to sync to disk
        :type sync: bool
        :returns: the number of documents updated
        :rtype: int
        :raises: DocumentFindAndUpdateError
        """
        data = {
            'collection': self._name,
            'example': filters,
            'newValue': data,
            'keepNull': keep_none,
            'waitForSync': sync,
        }
        if limit is not None:
            data['limit'] = limit

        request = Request(
            method='put',
            endpoint='/_api/simple/update-by-example',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFindAndUpdateError(res)
            return res.body['updated']
        return request, handler

    def replace(self, key, data, revision=None, sync=False):
        """Replace the specified document in this collection.

        If ``data`` contains the ``_key`` field, the field is ignored.

        If ``data`` contains the ``_rev`` field, or if ``revision`` is given,
        the revision of the target document must match against its value.
        Otherwise, DocumentRevisionError is raised.

        For edge collections, the ``_from`` and ``_to`` attributes are
        immutable, and they are ignored if present in ``data``.

        :param key: the key of the document to be replaced
        :type key: str
        :param data: the body to replace the document with
        :type data: dict
        :param revision: the document revision must match this value
        :type revision: str | None
        :param sync: wait for the replace to sync to disk
        :type sync: bool
        :returns: the ID, rev and key of the replaced document
        :rtype: dict
        :raises: DocumentReplaceError
        """
        params = {'waitForSync': sync}
        if revision is not None:
            params['rev'] = revision
            params['policy'] = 'error'
        elif '_rev' in data:
            params['rev'] = data['_rev']
            params['policy'] = 'error'

        request = Request(
            method='put',
            endpoint='/_api/{}/{}/{}'.format(self._type, self._name, key),
            params=params,
            data=data
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentReplaceError(res)
            del res.body['error']
            return res.body

        return request, handler

    def find_and_replace(self, filters, data, limit=None, sync=False):
        """Replace all matching documents.

        :param filters: the match filters
        :type filters: dict
        :param data: the replacement document
        :type data: dict
        :param limit: maximum number of documents to replace
        :type limit: int
        :param sync: wait for the replacement to sync to disk
        :type sync: bool
        :returns: the number of documents replaced
        :rtype: int
        :raises: DocumentReplaceManyError
        """
        data = {
            'collection': self._name,
            'example': filters,
            'newValue': data,
            'waitForSync': sync,
        }
        if limit is not None:
            data['limit'] = limit

        request = Request(
            method='put',
            endpoint='/_api/simple/replace-by-example',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentReplaceManyError(res)
            return res.body['replaced']

        return request, handler

    def delete(self, key, revision=None, sync=False, ignore_missing=True):
        """Delete the specified document from this collection.

        :param key: the key of the document to be deleted
        :type key: str
        :param revision: the document revision must match this value
        :type revision: str | None
        :param sync: wait for the delete to sync to disk
        :type sync: bool
        :param ignore_missing: ignore missing documents
        :type ignore_missing: bool
        :returns: the id, rev and key of the deleted document
        :rtype: dict
        :raises: DocumentRevisionError, DocumentDeleteError
        """
        params = {'waitForSync': sync}
        if revision is not None:
            params['rev'] = revision
            params['policy'] = 'error'

        request = Request(
            method='delete',
            endpoint='/_api/{}/{}/{}'.format(self._type, self._name, key),
            params=params
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code == 404:
                if ignore_missing:
                    return False
                else:
                    raise DocumentDeleteError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentDeleteError(res)
            return {
                'id': res.body['_id'],
                'key': res.body['_key'],
                'revision': res.body['_rev']
            }

        return request, handler

    def delete_many(self, keys):
        """Remove all documents whose key is in ``keys``.

        :param keys: keys of documents to delete
        :type keys: list
        :returns: the number of documents deleted
        :rtype: dict
        :raises: SimpleQueryDeleteByKeysError
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/remove-by-keys',
            data={'collection': self._name, 'keys': keys}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentDeleteError(res)
            return res.body

        return request, handler

    def find_and_delete(self, filters, limit=None, sync=False):
        """Delete all matching documents from the collection.

        :param filters: the match filters
        :type filters: dict
        :param limit: maximum number of documents to delete
        :type limit: int
        :param sync: wait for the deletion to sync to disk
        :type sync: bool
        :returns: the number of documents deleted
        :rtype: int
        :raises: DocumentDeleteManyError
        """
        data = {
            'collection': self._name,
            'example': filters,
            'waitForSync': sync,
        }
        if limit is not None:
            data['limit'] = limit

        request = Request(
            method='put',
            endpoint='/_api/simple/remove-by-example',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentDeleteManyError(res)
            return res.body['deleted']

        return request, handler
