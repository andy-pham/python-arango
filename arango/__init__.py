"""ArangoDB's Top-Level API."""

from datetime import datetime

from arango.method import Method
from arango.request import Request
from arango.database import Database
from arango.connection import Connection
from arango.exceptions import *
from arango.constants import HTTP_OK, LOG_LEVELS, DEFAULT_DATABASE
from arango.clients import DefaultHTTPClient
from arango.utils import uncamelify


class Arango(object):
    """Wrapper for ArangoDB's top-level APIs:

    1. Database Management
    2. User Management
    3. Administration & Monitoring
    4. Miscellaneous Functions
    """

    def __init__(self, protocol="http", host="localhost", port=8529,
                 username="root", password="", http_client=None):
        """Initialize the wrapper object.

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
        :param http_client: HTTP client for this wrapper to use
        :type http_client: arango.clients.base.BaseHTTPClient or None
        :raises: ArangoConnectionError
        """
        self._protocol = protocol
        self._host = host
        self._port = port
        # Initialize the ArangoDB HTTP Client if not given
        self._client = http_client if http_client else DefaultHTTPClient()
        # Initialize the ArangoDB Connection object
        self._conn = Connection(
            protocol=self._protocol,
            host=self._host,
            port=self._port,
            username=username,
            password=password,
            http_client=self._client,
        )
        # Check the connection by requesting a header
        res = self._conn.head("/_api/version")
        if res.status_code not in HTTP_OK:
            raise ArangoConnectionError(res)
        # Default ArangoDB database wrapper object
        self._default_db = Database(DEFAULT_DATABASE, self._conn)

    def __repr__(self):
        """Return a descriptive string of this instance."""
        return "<ArangoDB at '{}'>".format(self._host)

    def __getattr__(self, attr):
        """Call __getattr__ of the default database."""
        return getattr(self._default_db, attr)

    def __getitem__(self, item):
        """Call __getitem__ of the default database."""
        return self._default_db.collection(item)

    ###########################
    # Miscellaneous Functions #
    ###########################

    def version(self):
        """Return the version of the ArangoDB server.

        :returns: the version number
        :rtype: str
        :raises: VersionGetError
        """
        res = self._conn.get(
            endpoint='/_api/version',
            params={'details': True}
        )
        if res.status_code not in HTTP_OK:
            raise VersionGetError(res)
        return res.body["details"]

    def required_db(self):
        """Return the required database version.

        :returns: the required database version
        :rtype: str
        :raises: RequiredDatabaseVersionGetError
        """
        res = self._conn.get("/_admin/database/target-version")
        if res.status_code not in HTTP_OK:
            raise RequiredDatabaseVersionGetError(res)
        return res.body["version"]

    def server_time(self):
        """Return the system time of the ArangoDB server.

        :returns: the system time
        :rtype: datetime.datetime
        :raises: ServerTimeGetError
        """
        res = self._conn.get("/_admin/time")
        if res.status_code not in HTTP_OK:
            raise ServerTimeGetError(res)
        return datetime.fromtimestamp(res.body["time"])

    def get_wal(self):
        """Return the configuration of the write-ahead log.

        :returns: the configuration of the write-ahead log
        :rtype: dict
        :raises: WriteAheadLogGetError
        """
        res = self._conn.get("/_admin/wal/properties")
        if res.status_code not in HTTP_OK:
            raise WriteAheadLogGetError(res)
        return {
            "allow_oversize": res.body.get("allowOversizeEntries"),
            "historic_logs": res.body.get("historicLogfiles"),
            "log_size": res.body.get("logfileSize"),
            "reserve_logs": res.body.get("reserveLogfiles"),
            "sync_interval": res.body.get("syncInterval"),
            "throttle_wait": res.body.get("throttleWait"),
            "throttle_when_pending": res.body.get("throttleWhenPending")
        }

    def flush_wal(self, wait_for_sync=True, wait_for_gc=True):
        """Flush the write-ahead log to collection journals and data files.

        :param wait_for_sync: block until data is synced to disk
        :type wait_for_sync: bool
        :param wait_for_gc: block until flushed data is garbage collected
        :type wait_for_gc: bool
        :raises: WriteAheadLogFlushError
        """
        res = self._conn.put(
            "/_admin/wal/flush",
            data={
                "waitForSync": wait_for_sync,
                "waitForCollector": wait_for_gc
            }
        )
        if res.status_code not in HTTP_OK:
            raise WriteAheadLogFlushError(res)

    def configure_wal(self, allow_oversize=None, log_size=None,
                      historic_logs=None, reserve_logs=None,
                      throttle_wait=None, throttle_when_pending=None):
        """Configure the behaviour of the write-ahead log.

        When ``throttle_when_pending`` is set to 0, write-throttling will not
        be triggered at all.

        :param allow_oversize: allow operations bigger than a single log file
        :type allow_oversize: bool or None
        :param log_size: the size of each write-ahead log file
        :type log_size: int or None
        :param historic_logs: the number of historic log files to keep
        :type historic_logs: int or None
        :param reserve_logs: the number of reserve log files to allocate
        :type reserve_logs: int or None
        :param throttle_wait: wait time before aborting when throttled (in ms)
        :type throttle_wait: int or None
        :param throttle_when_pending: number of due gc ops before throttling
        :type throttle_when_pending: int or None
        :returns: the new configuration of the write-ahead log
        :rtype: dict
        :raises: Write
        """
        data = dict()
        if allow_oversize is not None:
            data["allowOversizeEntries"] = allow_oversize
        if log_size is not None:
            data["logfileSize"] = log_size
        if historic_logs is not None:
            data["historicLogfiles"] = historic_logs
        if reserve_logs is not None:
            data["reserveLogfiles"] = reserve_logs
        if throttle_wait is not None:
            data["throttleWait"] = throttle_wait
        if throttle_when_pending is not None:
            data["throttleWhenPending"] = throttle_when_pending
        res = self._conn.put("/_admin/wal/properties", data=data)
        if res.status_code not in HTTP_OK:
            raise WriteAheadLogGetError(res)
        return {
            "allow_oversize": res.body.get("allowOversizeEntries"),
            "historic_logs": res.body.get("historicLogfiles"),
            "log_size": res.body.get("logfileSize"),
            "reserve_logs": res.body.get("reserveLogfiles"),
            "sync_interval": res.body.get("syncInterval"),
            "throttle_wait": res.body.get("throttleWait"),
            "throttle_when_pending": res.body.get("throttleWhenPending")
        }

    def echo(self, short=True):
        """Return the information on the last request (headers, payload etc.)

        :param short: echo or long echo
        :type short: bool
        :returns: the information on the last request
        :rtype: dict
        :raises: EchoError
        """
        path = "/_admin/{}".format("echo" if short else "long_echo")
        res = self._conn.get(path)
        if res.status_code not in HTTP_OK:
            raise EchoError(res)
        return res.body

    def shutdown(self):
        """Initiate the server shutdown sequence.

        :raises: ShutdownError
        """
        res = self._conn.get("/_admin/shutdown")
        if res.status_code not in HTTP_OK:
            raise ShutdownError(res)

    def run_tests(self, tests):
        """Run the available unittests on the server.

        :param tests: list of files containing the test suites
        :type tests: list
        :returns: the passed result
        :rtype: dict
        :raises: TestsRunError
        """
        res = self._conn.post("/_admin/test", data={"tests": tests})
        if res.status_code not in HTTP_OK:
            raise TestsRunError(res)
        return res.body.get("passed", False)

    def execute_program(self, program):
        """Execute a javascript program on the server.

        :param program: the body of the program to execute.
        :type program: str
        :returns: the result of the execution
        :rtype: str
        :raises: ProgramExecuteError
        """
        res = self._conn.post("/_admin/execute", data=program)
        if res.status_code not in HTTP_OK:
            raise ProgramExecuteError(res)
        return res.body

    #######################
    # Database Management #
    #######################

    @property
    def databases(self):
        """"Return the database names.

        :returns: the database names
        :rtype: dict
        :raises: DatabaseListError
        """
        # Get the current user's databases
        res = self._conn.get("/_api/database/user")
        if res.status_code not in HTTP_OK:
            raise DatabaseListError(res)
        user_databases = res.body["result"]

        # Get all databases
        res = self._conn.get("/_api/database")
        if res.status_code not in HTTP_OK:
            raise DatabaseListError(res)
        all_databases = res.body["result"]

        return {"all": all_databases, "user": user_databases}

    def db(self, name):
        """Alias for self.database."""
        return self.database(name)

    def database(self, name):
        """Return the ``Database`` object of the specified name.

        :returns: the database object
        :rtype: arango.database.Database
        :raises: DatabaseNotFoundError
        """
        if name in self._database_cache:
            return self._database_cache[name]
        else:
            self._refresh_database_cache()
            if name not in self._database_cache:
                raise DatabaseNotFoundError(name)
            return self._database_cache[name]

    def create_database(self, name, users=None):
        """Create a new database.

        :param name: the name of the new database
        :type name: str
        :param users: the users configurations
        :type users: dict
        :returns: the Database object
        :rtype: arango.database.Database
        :raises: DatabaseCreateError
        """
        data = {"name": name, "users": users} if users else {"name": name}
        res = self._conn.post("/_api/database", data=data)
        if res.status_code not in HTTP_OK:
            raise DatabaseCreateError(res)
        self._refresh_database_cache()
        return self.db(name)

    def drop_database(self, name, safe_delete=False):
        """Remove the database of the specified name.

        :param name: the name of the database to delete
        :type name: str
        :param safe_delete: whether to execute a safe delete (ignore 404)
        :type safe_delete: bool
        :raises: DatabaseDeleteError
        """
        res = self._conn.delete("/_api/database/{}".format(name))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and safe_delete):
                raise DatabaseDeleteError(res)
        self._refresh_database_cache()

    ###################
    # User Management #
    ###################

    @property
    def users(self):
        """Return details on all users.

        :returns: a dictionary mapping user names to their information
        :rtype: dict
        :raises: UserListError
        """
        res = self._conn.get("/_api/user")
        if res.status_code not in HTTP_OK:
            raise UserListError(res)
        result = {}
        for record in res.body["result"]:
            result[record["user"]] = {
                "change_password": record.get("changePassword"),
                "active": record.get("active"),
                "extra": record.get("extra"),
            }
        return result

    def user(self, username):
        """Return the details on a single user.

        :returns: user information
        :rtype: dict or None
        :raises: UserNotFoundError
        """
        res = self._conn.get("/_api/user")
        if res.status_code not in HTTP_OK:
            raise UserNotFoundError(username)
        for record in res.body["result"]:
            if record["user"] == username:
                return {
                    "change_password": record.get("changePassword"),
                    "active": record.get("active"),
                    "extra": record.get("extra"),
                }
        raise UserNotFoundError(username)

    def create_user(self, username, password, active=None, extra=None,
                    change_password=None):
        """Create a new user.

        if ``change_password`` is set to true, the only operation allowed by
        the user will be ``self.replace_user`` or ``self.update_user``. All
        other operations executed by the user will result in an HTTP 403.

        :param username: the name of the user
        :type username: str
        :param password: the user password
        :type password: str
        :param active: whether the user is active
        :type active: bool or None
        :param extra: any extra data about the user
        :type extra: dict or None
        :param change_password: whether the user must change the password
        :type change_password: bool or None
        :returns: the information about the new user
        :rtype: dict
        :raises: UserCreateError
        """
        data = {"user": username, "passwd": password}
        if active is not None:
            data["active"] = active
        if extra is not None:
            data["extra"] = extra
        if change_password is not None:
            data["changePassword"] = change_password

        res = self._conn.post("/_api/user", data=data)
        if res.status_code not in HTTP_OK:
            raise UserCreateError(res)
        return {
            "active": res.body.get("active"),
            "change_password": res.body.get("changePassword"),
            "extra": res.body.get("extra"),
        }

    def update_user(self, username, password=None, active=None, extra=None,
                    change_password=None):
        """Update an existing user.

        if ``change_password`` is set to true, the only operation allowed by
        the user will be ``self.replace_user`` or ``self.update_user``. All
        other operations executed by the user will result in an HTTP 403.

        :param username: the name of the existing user
        :type username: str
        :param password: the user password
        :type password: str
        :param active: whether the user is active
        :type active: bool or None
        :param extra: any extra data about the user
        :type extra: dict or None
        :param change_password: whether the user must change the password
        :type change_password: bool or None
        :returns: the information about the updated user
        :rtype: dict
        :raises: UserUpdateError
        """
        data = dict()
        if password is not None:
            data["password"] = password
        if active is not None:
            data["active"] = active
        if extra is not None:
            data["extra"] = extra
        if change_password is not None:
            data["changePassword"] = change_password

        res = self._conn.patch(
            "/_api/user/{user}".format(user=username), data=data
        )
        if res.status_code not in HTTP_OK:
            raise UserUpdateError(res)
        return {
            "active": res.body.get("active"),
            "change_password": res.body.get("changePassword"),
            "extra": res.body.get("extra"),
        }

    def replace_user(self, username, password, active=None, extra=None,
                     change_password=None):
        """Replace an existing user.

        if ``change_password`` is set to true, the only operation allowed by
        the user will be ``self.replace_user`` or ``self.update_user``. All
        other operations executed by the user will result in an HTTP 403.

        :param username: the name of the existing user
        :type username: str
        :param password: the user password
        :type password: str
        :param active: whether the user is active
        :type active: bool or None
        :param extra: any extra data about the user
        :type extra: dict or None
        :param change_password: whether the user must change the password
        :type change_password: bool or None
        :returns: the information about the replaced user
        :rtype: dict
        :raises: UserReplaceError
        """
        data = {"user": username, "password": password}
        if active is not None:
            data["active"] = active
        if extra is not None:
            data["extra"] = extra
        if change_password is not None:
            data["changePassword"] = change_password

        res = self._conn.put(
            "/_api/user/{user}".format(user=username), data=data
        )
        if res.status_code not in HTTP_OK:
            raise UserReplaceError(res)
        return {
            "active": res.body.get("active"),
            "change_password": res.body.get("changePassword"),
            "extra": res.body.get("extra"),
        }

    def delete_user(self, username, safe_delete=False):
        """Delete an existing user.

        :param username: the name of the user
        :type username: str
        :param safe_delete: ignores HTTP 404 if set to True
        :type safe_delete: bool
        :raises: UserDeleteError
        """
        res = self._conn.delete("/_api/user/{user}".format(user=username))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and safe_delete):
                raise UserDeleteError(res)

    ###############################
    # Administration & Monitoring #
    ###############################

    def get_log(self, upto=None, level=None, start=None, size=None,
                offset=None, search=None, sort=None):
        """Read the global log from the server

        The parameters ``upto`` and ``level`` are mutually exclusive.
        The values for ``upto`` and ``level`` must be one of:

        'fatal' or 0
        'error' or 1
        'warning' or 2
        'info' or 3 (default)
        'debug' or 4

        The parameters ``offset`` and ``size`` can be used for pagination.
        The values for ``sort`` must be one of 'asc' or 'desc'.

        :param upto: return entries up to this level
        :type upto: str or int or None
        :param level: return entries of this level only
        :type level: str or int or None
        :param start: return entries whose id >= to the given value
        :type start: int or None
        :param size: restrict the result to the given value
        :type size: int or None
        :param offset: return entries skipping the given number
        :type offset: int or None
        :param search: return only the entires containing the given text
        :type search: str or None
        :param sort: sort the entries according to their lid values
        :type sort: str or None
        :returns: the server log
        :rtype: dict
        :raises: LogGetError
        """
        params = dict()
        if upto is not None:
            params["upto"] = upto
        if level is not None:
            params["level"] = level
        if start is not None:
            params["start"] = start
        if size is not None:
            params["size"] = size
        if offset is not None:
            params["offset"] = offset
        if search is not None:
            params["search"] = search
        if sort is not None:
            params["sort"] = sort
        res = self._conn.get("/_admin/log")
        if res.status_code not in HTTP_OK:
            LogGetError(res)
        return res.body

    def reload_routing(self):
        """Reload the routing information from the collection ``routing``.

        :raises: RoutingInfoReloadError
        """
        res = self._conn.post("/_admin/routing/reload")
        if res.status_code not in HTTP_OK:
            raise RoutingInfoReloadError(res)

    @property
    def statistics(self):
        """Return the server statistics.

        :returns: the statistics information
        :rtype: dict
        :raises: StatisticsGetError
        """
        res = self._conn.get("/_admin/statistics")
        if res.status_code not in HTTP_OK:
            raise StatisticsGetError(res)
        del res.body["code"]
        del res.body["error"]
        return res.body

    @property
    def statistics_description(self):
        """Return the description of the statistics from self.statistics.

        :returns: the statistics description
        :rtype: dict
        :raises: StatisticsDescriptionError
        """
        res = self._conn.get("/_admin/statistics-description")
        if res.status_code not in HTTP_OK:
            raise StatisticsDescriptionGetError(res)
        del res.body["code"]
        del res.body["error"]
        return res.body

    @property
    def server_role(self):
        """Return the role of the server in the cluster (if applicable)

        Possible return values are:

        COORDINATOR: the server is a coordinator in a cluster
        PRIMARY:     the server is a primary database server in a cluster
        SECONDARY:   the server is a secondary database server in a cluster
        UNDEFINED:   in a cluster, UNDEFINED is returned if the server role
                     cannot be determined. On a single server, UNDEFINED is
                     the only possible return value.

        :returns: the server role
        :rtype: str
        :raises: ServerRoleGetError
        """
        res = self._conn.get("/_admin/server/role")
        if res.status_code not in HTTP_OK:
            raise ServerRoleGetError(res)
        return res.body["role"]

if __name__ == "__main__":
    a = Arango()
    print(a.version)
