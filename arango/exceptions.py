"""ArangoDB Exceptions."""


class ArangoError(Exception):
    """Base class for ArangoDB request errors.

    :param response: the Response object
    :type response: arango.response.Response
    """

    def __init__(self, response):
        # Get the ArangoDB error message if given
        if response.body is not None and "errorMessage" in response.body:
            message = response.body["errorMessage"]
        elif response.status_text is not None:
            message = response.status_text
        else:
            message = "server error"

        # Get the ArangoDB error number if given
        if response.body is not None and "errorNum" in response.body:
            self.error_code = response.body["errorNum"]
        else:
            self.error_code = None

        # Generate the error message for the exception
        super(ArangoError, self).__init__(message)
        self.method = response.method
        self.url = response.url
        self.http_code = response.status_code


class NotFoundError(KeyError):
    """Base ArangoDB "not found" exception class.

    :param name: the name of the missing ArangoDB object
    :type name: str
    """

    def __init__(self, name):
        self.name = name
        super(NotFoundError, self).__init__(name)


class ServerConnectionError(ArangoError):
    """Failed to connect to ArangoDB."""


class InvalidArgumentError(Exception):
    """The given argument(s) are invalid."""


###########################
# Miscellaneous Functions #
###########################


class EndpointsGetError(ArangoError):
    """Failed to get the endpoints."""


class VersionGetError(ArangoError):
    """Failed to retrieve the server version."""


class DetailsGetError(ArangoError):
    """Failed to retrieve the server details."""


class WriteAheadLogFlushError(ArangoError):
    """Failed to flush the write-ahead log."""


class WriteAheadLogGetError(ArangoError):
    """Failed to get the write-ahead log."""


class WriteAheadLogSetError(ArangoError):
    """Failed to configure the write-ahead log."""


class TimeGetError(ArangoError):
    """Failed to return the current system time."""


class EchoError(ArangoError):
    """Failed to return current request."""


class SleepError(ArangoError):
    """Failed to suspend the execution."""


class RequiredDatabaseVersionGetError(ArangoError):
    """Failed to retrieve the required database version."""


class ShutdownError(ArangoError):
    """Failed to initiate a clean shutdown sequence."""


class TestsRunError(ArangoError):
    """Failed to execute the specified tests on the server."""


class ProgramExecuteError(ArangoError):
    """Failed to execute a the given Javascript program."""


#########
# Tasks #
#########


class TaskGetError(ArangoError):
    """Failed to get the active server tasks."""


class TaskCreateError(ArangoError):
    """Failed to create a server task."""


class TaskDeleteError(ArangoError):
    """Failed to delete a server task."""


#######################
# Database Exceptions #
#######################


class DatabaseNotFoundError(NotFoundError):
    """Failed to find the database."""


class DatabaseListError(ArangoError):
    """Failed to get the list of databases."""


class DatabasePropertyError(ArangoError):
    """Failed to get the database property."""


class DatabaseGetError(ArangoError):
    """Failed to get the database."""


class DatabaseCreateError(ArangoError):
    """Failed to create the database."""


class DatabaseDeleteError(ArangoError):
    """Failed to delete the database."""


###################
# User Exceptions #
###################


class UserNotFoundError(NotFoundError):
    """Failed to get the user."""


class UserListError(ArangoError):
    """Failed to get the list of users."""


class UserCreateError(ArangoError):
    """Failed to create the user."""


class UserUpdateError(ArangoError):
    """Failed to update the user."""


class UserReplaceError(ArangoError):
    """Failed to replace the user."""


class UserDeleteError(ArangoError):
    """Failed to delete the user."""


#########################
# Collection Exceptions #
#########################


class CollectionCorruptedError(Exception):
    """The collection is corrupted (i.e. its status is ``unknown``)."""


class CollectionNotFoundError(NotFoundError):
    """Failed to find the collection."""


class CollectionListError(ArangoError):
    """Failed to get the list of collections."""


class CollectionGetError(ArangoError):
    """Failed to get the collection."""


class CollectionChecksumError(ArangoError):
    """Failed to get the collection checksum."""


class CollectionCreateError(ArangoError):
    """Failed to create the collection."""


class CollectionDeleteError(ArangoError):
    """Failed to delete the collection"""


class CollectionUpdateError(ArangoError):
    """Failed to update the collection."""


class CollectionRenameError(ArangoError):
    """Failed to rename the collection."""


class CollectionTruncateError(ArangoError):
    """Failed to truncate the collection."""


class CollectionLoadError(ArangoError):
    """Failed to load the collection into memory."""


class CollectionUnloadError(ArangoError):
    """Failed to unload the collection from memory."""


class CollectionRotateJournalError(ArangoError):
    """Failed to rotate the journal of the collection."""


########################################
# Documents Import & Export Exceptions #
########################################


class DocumentsImportError(ArangoError):
    """Failed to bulk import documents/edges."""


class DocumentsExportError(ArangoError):
    """Failed to bulk export documents/edges."""


#######################
# Document Exceptions #
#######################


class DocumentInvalidError(Exception):
    """The document is invalid (malformed)."""


class DocumentRevisionError(ArangoError):
    """The expected and actual document revisions do not match."""


class DocumentGetError(ArangoError):
    """Failed to get the document."""


class DocumentCreateError(ArangoError):
    """Failed to create the document."""


class DocumentReplaceError(ArangoError):
    """Failed to replace the document."""


class DocumentUpdateError(ArangoError):
    """Failed to update the document."""


class DocumentDeleteError(ArangoError):
    """Failed to delete the document."""


###################
# Edge Exceptions #
###################


class EdgeInvalidError(Exception):
    """The edge is invalid (malformed)."""


class EdgeRevisionError(ArangoError):
    """The expected and actual edge revisions do not match."""


class EdgeGetError(ArangoError):
    """Failed to get the edge."""


class EdgeCreateError(ArangoError):
    """Failed to create the edge."""


class EdgeReplaceError(ArangoError):
    """Failed to replace the edge."""


class EdgeUpdateError(ArangoError):
    """Failed to update the edge."""


class EdgeDeleteError(ArangoError):
    """Failed to delete the edge."""


#####################
# Vertex Exceptions #
#####################


class VertexInvalidError(ArangoError):
    """The vertex is invalid (malformed)."""


class VertexRevisionError(ArangoError):
    """The expected and actual vertex revisions do not match."""


class VertexGetError(ArangoError):
    """Failed to get the vertex."""


class VertexCreateError(ArangoError):
    """Failed to create the vertex."""


class VertexUpdateError(ArangoError):
    """Failed to update the vertex."""


class VertexReplaceError(ArangoError):
    """Failed to replace the vertex."""


class VertexDeleteError(ArangoError):
    """Failed to delete the vertex."""


####################
# Index Exceptions #
####################


class IndexListError(ArangoError):
    """Failed to get the list of indexes."""


class IndexCreateError(ArangoError):
    """Failed to create the index."""


class IndexDeleteError(ArangoError):
    """Failed to delete the index."""


################################
# AQL Query & Cache Exceptions #
################################


class AQLQueryExplainError(ArangoError):
    """Failed to explain the AQL query."""


class AQLQueryValidateError(ArangoError):
    """Failed to validate the AQL query."""


class AQLQueryExecuteError(ArangoError):
    """Failed to execute the AQL query."""


class AQLQueryCacheDeleteError(ArangoError):
    """Failed to clear the AQL query cache."""


class AQLQueryCacheGetError(ArangoError):
    """Failed to get the AQL query cache properties."""


class AQLQueryCacheSetError(ArangoError):
    """Failed to configure the AQL query cache properties."""


#####################
# Cursor Exceptions #
#####################

class CursorGetNextError(ArangoError):
    """Failed to get the next cursor result."""


class CursorDeleteError(ArangoError):
    """Failed to delete the cursor."""


###########################
# AQL Function Exceptions #
###########################


class AQLFunctionListError(ArangoError):
    """Failed to get the list of AQL functions."""


class AQLFunctionCreateError(ArangoError):
    """Failed to create the AQL function."""


class AQLFunctionDeleteError(ArangoError):
    """Failed to delete the AQL function."""


###########################
# Simple Query Exceptions #
###########################


class SimpleQueryGetByExampleError(ArangoError):
    """Failed to execute the ``by-example`` simple query."""


class SimpleQueryFirstExampleError(ArangoError):
    """Failed to execute the ``first-example`` simple query."""


class SimpleQueryReplaceByExampleError(ArangoError):
    """Failed to execute the ``replace-by-example`` simple query."""


class SimpleQueryUpdateByExampleError(ArangoError):
    """Failed to execute the ``update-by-example`` simple query."""


class SimpleQueryDeleteByExampleError(ArangoError):
    """Failed to execute the ``Delete-by-example`` simple query."""


class SimpleQueryFirstError(ArangoError):
    """Failed to execute the ``first`` simple query."""


class SimpleQueryLastError(ArangoError):
    """Failed to execute the ``last`` simple query."""


class SimpleQueryAllError(ArangoError):
    """Failed to execute the `all`` simple query."""


class SimpleQueryAnyError(ArangoError):
    """Failed to execute the ``any`` simple query."""


class SimpleQueryRangeError(ArangoError):
    """Failed to execute the ``range`` simple query."""


class SimpleQueryNearError(ArangoError):
    """Failed to execute the ``near`` simple query."""


class SimpleQueryWithinError(ArangoError):
    """Failed to execute the ``within`` simple query."""


class SimpleQueryFullTextError(ArangoError):
    """Failed to execute the ``fulltext`` simple query."""


class SimpleQueryLookupByKeysError(ArangoError):
    """Failed to execute the ``lookup-by-keys`` simple query."""


class SimpleQueryDeleteByKeysError(ArangoError):
    """Failed to execute the ``Delete-by-keys`` simple query."""


class SimpleQueryError(ArangoError):
    """Failed to execute a simple query."""


##########################
# Transaction Exceptions #
##########################


class TransactionExecuteError(ArangoError):
    """Failed to execute a transaction."""


class TransactionGetError(ArangoError):
    """Failed to get the running transactions."""


####################
# Batch Exceptions #
####################


class BatchInvalidError(Exception):
    """The batch request is invalid (malformed)."""


class BatchExecuteError(ArangoError):
    """Failed to execute a batch request."""


####################
# Graph Exceptions #
####################


class GraphNotFoundError(NotFoundError):
    """Failed to find the graph."""


class GraphListError(ArangoError):
    """Failed to get the list of graphs."""


class GraphGetError(ArangoError):
    """Failed to get the graph."""


class GraphCreateError(ArangoError):
    """Failed to create the graph."""


class GraphDeleteError(ArangoError):
    """Failed to delete the graph."""


class GraphPropertyError(ArangoError):
    """Failed to get the graph property."""


class GraphTraversalError(ArangoError):
    """Failed to execute the graph traversal."""


################################
# Vertex Collection Exceptions #
################################


class VertexCollectionListError(ArangoError):
    """Failed to get the list of vertex collections."""


class VertexCollectionCreateError(ArangoError):
    """Failed to create the vertex collection."""


class VertexCollectionDeleteError(ArangoError):
    """Failed to delete the vertex collection."""


#########################################
# Edge Collection/Definition Exceptions #
#########################################


class EdgeDefinitionListError(ArangoError):
    """Failed to get the list of edge definitions."""


class EdgeDefinitionCreateError(ArangoError):
    """Failed to create the edge definition."""


class EdgeDefinitionReplaceError(ArangoError):
    """Failed to replace the edge definition."""


class EdgeDefinitionDeleteError(ArangoError):
    """Failed to delete the edge definition."""


##########################################
# Administration & Monitoring Exceptions #
##########################################


class LogGetError(ArangoError):
    """Failed to get the global log."""


class RountingInfoReloadError(ArangoError):
    """Failed to reload the routing information."""


class StatisticsGetError(ArangoError):
    """Failed to get the server statistics."""


class StatisticsDescriptionGetError(ArangoError):
    """Failed to get the statistics description."""


class ServerRoleGetError(ArangoError):
    """Failed to get the role of the server in a cluster."""


