"""ArangoDB's Top-Level API."""

from __future__ import absolute_import, unicode_literals

from datetime import datetime

from requests import ConnectionError

from arango.clients import DefaultHTTPClient
from arango.connection import Connection
from arango.constants import HTTP_OK, LOG_LEVELS, DEFAULT_DB
from arango.database import Database
from arango.exceptions import *
from arango.utils import uncamelify
from arango.version import VERSION


class Arango(object):
    """Wrapper for ArangoDB's top level API endpoints which cover:

    1. Database Management
    2. User Management
    3. Write-Ahead Log Management
    4. Administration & Monitoring
    5. Miscellaneous Functions
    6. Task Management
    """

    def __init__(self, protocol='http', host='localhost', port=8529,
                 username='root', password='', client=None, verify=False):
        """Initialize the API driver.

        :param protocol: the internet transfer protocol (default: 'http')
        :type protocol: str
        :param host: ArangoDB host (default: 'localhost')
        :type host: str
        :param port: ArangoDB port (default: 8529)
        :type port: int
        :param username: ArangoDB username (default: 'root')
        :type username: str
        :param password: ArangoDB password (default: '')
        :type password: str
        :param client: the HTTP client
        :type client: arango.clients.base.BaseHTTPClient or None
        :param verify: check the connection during initialization
        :type verify: bool
        :raises: ServerConnectionError
        """
        self._protocol = protocol
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._client = client if client else DefaultHTTPClient()

        # Initialize the ArangoDB API connection object
        self._conn = Connection(
            protocol=self._protocol,
            host=self._host,
            port=self._port,
            username=self._username,
            password=self._password,
            client=self._client,
        )
        # Verify the connection by requesting a header
        if verify:
            res = self._conn.head('/_api/version')
            if res.status_code not in HTTP_OK:
                raise ServerConnectionError(res)

        # Initialize the default database
        self._default_db = Database(self._conn)

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
    def version(self):
        """Return the version of the driver.

        :returns: the version of the driver
        :rtype: str
        """
        return VERSION

    def __repr__(self):
        """Return a descriptive string of this instance."""
        return "<ArangoDB API driver pointing to '{}'>".format(self._host)

    def __getattr__(self, attr):
        """Call __getattr__ of the default database."""
        return getattr(self._default_db, attr)

    def __getitem__(self, item):
        """Call __getitem__ of the default database."""
        return self._default_db[item]

    ###########################
    # Miscellaneous Functions #
    ###########################

    def get_version(self, details=False):
        """Return the version of the ArangoDB server.

        :param details: whether to include the component information
        :type details: bool
        :returns: the server version
        :rtype: str
        :raises: VersionGetError
        """
        res = self._conn.get(
            endpoint='/_api/version',
            params={'details': details}
        )
        if res.status_code not in HTTP_OK:
            raise VersionGetError(res)
        return res.body['details'] if details else res.body['version']

    def get_target_version(self):
        """Return the required version of the target database.

        :returns: the required version of the target database
        :rtype: str
        :raises: TargetVersionGetError
        """
        res = self._conn.get('/_admin/database/target-version')
        if res.status_code not in HTTP_OK:
            raise TargetVersionGetError(res)
        return res.body['version']

    def get_system_time(self):
        """Return the current system time on the server side.

        :returns: the system time
        :rtype: datetime.datetime
        :raises: SystemTimeGetError
        """
        res = self._conn.get('/_admin/time')
        if res.status_code not in HTTP_OK:
            raise SystemTimeGetError(res)
        return datetime.fromtimestamp(res.body['time'])

    def echo(self):
        """Return information on the last request (headers, payload etc.)

        :returns: the information on the last request
        :rtype: dict
        :raises: EchoError
        """
        res = self._conn.get('/_admin/echo')
        if res.status_code not in HTTP_OK:
            raise EchoError(res)
        return res.body

    def sleep(self, seconds):
        """Suspend the execution for a specified duration before returning.

        :param seconds: the amount of seconds to suspend
        :type seconds: int
        :returns: the number of seconds suspended
        :rtype: int
        :raises: SleepError
        """
        res = self._conn.get(
            '/_admin/sleep',
            params={'duration': seconds}
        )
        if res.status_code not in HTTP_OK:
            raise SleepError(res)
        return res.body['duration']

    def shutdown(self):
        """Initiate the server shutdown sequence.

        :returns: whether the server was shutdown successfully
        :rtype: bool
        :raises: ShutdownError
        """
        try:
            res = self._conn.get('/_admin/shutdown')
        except ConnectionError:
            return False
        else:
            if res.status_code not in HTTP_OK:
                raise ShutdownError(res)
            return True

    def execute_tests(self, tests):
        """Run the available unittests on the server.

        :param tests: list of files containing the test suites
        :type tests: list
        :returns: the test result
        :rtype: dict
        :raises: TestsRunError
        """
        res = self._conn.post('/_admin/test', data={'tests': tests})
        if res.status_code not in HTTP_OK:
            raise TestsExecuteError(res)
        return res.body

    def execute_javascript(self, javascript):
        """Execute a javascript program on the server.

        :param javascript: the body of the javascript program to execute.
        :type javascript: str
        :returns: the result of the execution
        :rtype: str
        :raises: ProgramExecuteError
        """
        res = self._conn.post(
            '/_admin/execute',
            data=javascript
        )
        if res.status_code not in HTTP_OK:
            raise ProgramExecuteError(res)
        return res.body

    ##############################
    # Write-Ahead Log Management #
    ##############################

    def get_wal_config(self):
        """Return the configuration of the write-ahead log.

        :returns: the configuration of the write-ahead log
        :rtype: dict
        :raises: WriteAheadLogGetError
        """
        res = self._conn.get('/_admin/wal/properties')
        if res.status_code not in HTTP_OK:
            raise WriteAheadLogGetError(res)
        return {
            'oversized_ops': res.body.get('allowOversizeEntries'),
            'log_size': res.body.get('logfileSize'),
            'historic_logs': res.body.get('historicLogfiles'),
            'reserve_logs': res.body.get('reserveLogfiles'),
            'sync_interval': res.body.get('syncInterval'),
            'throttle_wait': res.body.get('throttleWait'),
            'throttle_limit': res.body.get('throttleWhenPending')
        }

    def set_wal_config(self, oversized_ops=None, log_size=None,
                       historic_logs=None, reserve_logs=None,
                       throttle_wait=None, throttle_limit=None):
        """Configure the parameters of the write-ahead log.

        Setting ``throttle_when_pending`` to 0 disables the throttling.

        :param oversized_ops: execute and store ops bigger than a log file
        :type oversized_ops: bool or None
        :param log_size: the size of each write-ahead log file
        :type log_size: int or None
        :param historic_logs: the number of historic log files to keep
        :type historic_logs: int or None
        :param reserve_logs: the number of reserve log files to allocate
        :type reserve_logs: int or None
        :param throttle_wait: wait time before aborting when throttled (in ms)
        :type throttle_wait: int or None
        :param throttle_limit: number of pending gc ops before write-throttling
        :type throttle_limit: int or None
        :returns: the new configuration of the write-ahead log
        :rtype: dict
        :raises: WriteAheadLogGetError
        """
        data = {}
        if oversized_ops is not None:
            data['allowOversizeEntries'] = oversized_ops
        if log_size is not None:
            data['logfileSize'] = log_size
        if historic_logs is not None:
            data['historicLogfiles'] = historic_logs
        if reserve_logs is not None:
            data['reserveLogfiles'] = reserve_logs
        if throttle_wait is not None:
            data['throttleWait'] = throttle_wait
        if throttle_limit is not None:
            data['throttleWhenPending'] = throttle_limit
        res = self._conn.put('/_admin/wal/properties', data=data)
        if res.status_code not in HTTP_OK:
            raise WriteAheadLogGetError(res)
        return {
            'oversized_ops': res.body.get('allowOversizeEntries'),
            'log_size': res.body.get('logfileSize'),
            'historic_logs': res.body.get('historicLogfiles'),
            'reserve_logs': res.body.get('reserveLogfiles'),
            'sync_interval': res.body.get('syncInterval'),
            'throttle_wait': res.body.get('throttleWait'),
            'throttle_limit': res.body.get('throttleWhenPending')
        }

    def flush_wal(self, wait_for_sync=True, wait_for_gc=True):
        """Flush the write-ahead log to collection journals and data files.

        :param wait_for_sync: block until data is synced to disk
        :type wait_for_sync: bool
        :param wait_for_gc: block until flushed data is garbage collected
        :type wait_for_gc: bool
        :returns: whether the write-ahead log was flushed successfully
        :rtype: bool
        :raises: WriteAheadLogFlushError
        """
        res = self._conn.put(
            '/_admin/wal/flush',
            data={
                'waitForSync': wait_for_sync,
                'waitForCollector': wait_for_gc
            }
        )
        if res.status_code not in HTTP_OK:
            raise WriteAheadLogFlushError(res)
        return not res.body['error']

    def get_wal_transactions(self):
        """Return the information about the currently running transactions.

        Fields in the returned dictionary:

        ``last_collected``:
            min ID of the last collected log file (at the start of
            running transaction). None if no transaction is running.

        ``last_sealed``:
            min ID of the last sealed log file (at the start of each
            running transaction). None if no transaction is running.

        ``count``:
            the number of current running transactions

        :returns: the information about the currently running transactions
        :rtype: dict
        :raises: TransactionsGetError
        """
        res = self._conn.get('/_admin/wal/transactions')
        if res.status_code not in HTTP_OK:
            raise TransactionGetError(res)
        return {
            'last_collected': res.body['minLastCollected'],
            'last_sealed': res.body['minLastSealed'],
            'count': res.body['runningTransactions']
        }

    #######################
    # Database Management #
    #######################

    def list_databases(self, user_only=False):
        """"Return the database names.

        :param user_only: return only the user database names
        :type user_only: bool
        :returns: the database names
        :rtype: list
        :raises: DatabaseListError
        """
        # Get the current user's databases
        res = self._conn.get(
            '/_api/database/user'
            if user_only
            else '/_api/database'
        )
        if res.status_code not in HTTP_OK:
            raise DatabaseListError(res)
        return res.body['result']

    def db(self, name=DEFAULT_DB):
        """Return the database object of the specified name.

        :param name: the name of the database
        :type name: str
        :returns: the database object
        :rtype: arango.database.Database
        """
        return Database(Connection(
            protocol=self._protocol,
            host=self._host,
            port=self._port,
            username=self._username,
            password=self._password,
            database=name,
            client=self._client
        ))

    def create_database(self, name, users=None):
        """Create a new database.

        :param name: the name of the new database
        :type name: str
        :param users: the users configuration
        :type users: dict
        :returns: the database object
        :rtype: arango.database.Database
        :raises: DatabaseCreateError
        """
        res = self._conn.post(
            '/_api/database',
            data={'name': name, 'users': users}
            if users else {'name': name}
        )
        if res.status_code not in HTTP_OK:
            raise DatabaseCreateError(res)
        return self.db(name)

    def drop_database(self, name, ignore_missing=False):
        """Drop the database of the specified name.

        :param name: the name of the database to delete
        :type name: str
        :param ignore_missing: ignore HTTP 404
        :type ignore_missing: bool
        :returns: whether the database was dropped successfully
        :rtype: bool
        :raises: DatabaseDeleteError
        """
        res = self._conn.delete('/_api/database/{}'.format(name))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and ignore_missing):
                raise DatabaseDeleteError(res)
        return not res.body['error']

    ###################
    # User Management #
    ###################

    def list_users(self):
        """Return details on all users.

        :returns: the mapping of usernames to user information
        :rtype: dict
        :raises: UserListError
        """
        res = self._conn.get('/_api/user')
        if res.status_code not in HTTP_OK:
            raise UserListError(res)
        return {
            record['user']: {
                'user': record['user'],
                'active': record['active'],
                'extra': record['extra'],
                'change_password': record['changePassword']
            } for record in res.body['result']
        }

    def get_user(self, username):
        """Return the details on a single user.

        :param username: the username
        :type username: str
        :returns: user information
        :rtype: dict or None
        :raises: UserNotFoundError
        """
        res = self._conn.get('/_api/user')
        if res.status_code not in HTTP_OK:
            raise UserNotFoundError(username)
        for record in res.body['result']:
            if record['user'] == username:
                return {
                    'user': record['user'],
                    'active': record['active'],
                    'extra': record['extra'],
                    'change_password': record['changePassword']
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
        data = {'user': username, 'passwd': password}
        if active is not None:
            data['active'] = active
        if extra is not None:
            data['extra'] = extra
        if change_password is not None:
            data['changePassword'] = change_password

        res = self._conn.post('/_api/user', data=data)
        if res.status_code not in HTTP_OK:
            raise UserCreateError(res)
        return {
            'user': res.body['user'],
            'active': res.body['active'],
            'extra': res.body['extra'],
            'change_password': res.body['changePassword'],
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
        data = {}
        if password is not None:
            data['password'] = password
        if active is not None:
            data['active'] = active
        if extra is not None:
            data['extra'] = extra
        if change_password is not None:
            data['changePassword'] = change_password

        res = self._conn.patch(
            '/_api/user/{user}'.format(user=username), data=data
        )
        if res.status_code not in HTTP_OK:
            raise UserUpdateError(res)
        return {
            'user': res.body['user'],
            'active': res.body['active'],
            'extra': res.body['extra'],
            'change_password': res.body['changePassword'],
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
        data = {'user': username, 'password': password}
        if active is not None:
            data['active'] = active
        if extra is not None:
            data['extra'] = extra
        if change_password is not None:
            data['changePassword'] = change_password

        res = self._conn.put(
            '/_api/user/{user}'.format(user=username), data=data
        )
        if res.status_code not in HTTP_OK:
            raise UserReplaceError(res)
        return {
            'user': res.body['user'],
            'active': res.body['active'],
            'extra': res.body['extra'],
            'change_password': res.body['changePassword'],
        }

    def delete_user(self, username, ignore_missing=False):
        """Delete an existing user.

        :param username: the name of the user
        :type username: str
        :param ignore_missing: ignore HTTP 404
        :type ignore_missing: bool
        :raises: UserDeleteError
        """
        res = self._conn.delete('/_api/user/{user}'.format(user=username))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and ignore_missing):
                raise UserDeleteError(res)
        return not res.body['error']

    ###############################
    # Administration & Monitoring #
    ###############################

    def get_log(self, upto=None, level=None, start=None, size=None,
                offset=None, search=None, sort=None):
        """Read the global log from the server.

        The parameters ``upto`` and ``level`` are mutually exclusive.
        The values for ``upto`` or ``level`` must be one of:

            ``fatal`` or 0
            ``error`` or 1
            ``warning`` or 2
            ``info`` or 3 (default)
            ``debug`` or 4
        The parameters ``offset`` and ``size`` can be used for pagination.
        The values for ``sort`` are 'asc' or 'desc'.

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
            params['upto'] = upto
        if level is not None:
            params['level'] = level
        if start is not None:
            params['start'] = start
        if size is not None:
            params['size'] = size
        if offset is not None:
            params['offset'] = offset
        if search is not None:
            params['search'] = search
        if sort is not None:
            params['sort'] = sort
        res = self._conn.get('/_admin/log')
        if res.status_code not in HTTP_OK:
            LogGetError(res)
        if 'totalAmount' in res.body:
            res.body['total_amount'] = res.body.pop('totalAmount')
        return res.body

    def reload_routing(self):
        """Reload the routing information from the collection ``routing``.

        :returns: whether routing was reloaded successfully
        :rtype: bool
        :raises: RoutingInfoReloadError
        """
        res = self._conn.post('/_admin/routing/reload')
        if res.status_code not in HTTP_OK:
            raise RountingInfoReloadError(res)
        return not res.body['error']

    def get_statistics(self, description=False):
        """Return the server statistics.

        If ``description`` is set to True, the description of each key in the
        statistics dictionary is returned instead.

        :param description: return the description instead
        :type description: bool
        :returns: the statistics information
        :rtype: dict
        :raises: StatisticsGetError
        """
        if description:
            endpoint = '/_admin/statistics-description'
        else:
            endpoint = '/_admin/statistics'
        res = self._conn.get(endpoint)
        if res.status_code not in HTTP_OK:
            raise StatisticsGetError(res)
        res.body.pop('code', None)
        res.body.pop('error', None)
        return res.body

    def get_role(self):
        """Return the role of the server in the cluster if applicable

        Possible return values are:

        SINGLE:      the server is not in a cluster
        COORDINATOR: the server is a coordinator in the cluster
        PRIMARY:     the server is a primary database in the cluster
        SECONDARY:   the server is a secondary database in the cluster
        UNDEFINED:   in a cluster, UNDEFINED is returned if the server role
                     cannot be determined. On a single server, UNDEFINED is
                     the only possible return value.

        :returns: the server role
        :rtype: str
        :raises: ServerRoleGetError
        """
        res = self._conn.get('/_admin/server/role')
        if res.status_code not in HTTP_OK:
            raise ServerRoleGetError(res)
        return res.body.get('role')

    def get_endpoints(self):
        """Return the list of the endpoints the server is listening on.

        Each endpoint is mapped to a list of databases. If the list is empty,
        it means all databases can be accessed via the endpoint. If the list
        contains more than one database, the first database receives all the
        requests by default, unless the name is explicitly specified.

        :returns: the list of endpoints
        :rtype: list
        :raises EndpointsGetError
        """
        res = self._conn.get('/_api/endpoint')
        if res.status_code not in HTTP_OK:
            raise EndpointsGetError(res)
        return res.body

    ###################
    # Task Management #
    ###################

    def list_tasks(self):
        """Return all server tasks that are currently active.

        :returns: server tasks that are currently active
        :rtype: dict
        :raises: TaskGetError
        """
        res = self._conn.get('/_api/tasks')
        if res.status_code not in HTTP_OK:
            raise TaskGetError(res)
        return {record['id']: record for record in res.body}

    def get_task(self, task_id):
        """Return the active server task with the given id.

        :param task_id: the id of the server task
        :type task_id: str
        :returns: the details on the active task
        :rtype: dict
        :raises: TaskGetError
        """
        res = self._conn.get('/_api/tasks/{}'.format(task_id))
        if res.status_code not in HTTP_OK:
            raise TaskGetError(res)
        res.body.pop('code', None)
        res.body.pop('error', None)
        return res.body

    # TODO verify which arguments are optional
    def create_task(self, name, command, params=None, period=None,
                    offset=None, task_id=None):
        """Create a new task with.

        A task can be created with a pre-defined ID which can be specified
        through the ``id`` parameter.

        :param name: the name of the task
        :type name: str
        :param command: the Javascript code to execute
        :type command: str
        :param params: the parameters passed into the command
        :type params: dict
        :param period: the number of seconds between the executions
        :type period: int
        :param offset: the initial delay in seconds
        :type offset: int
        :param task_id: pre-defined ID for the new task
        :type task_id: str
        :return: the details on the new task
        :rtype: dict
        :raises: TaskCreateError
        """
        data = {
            'name': name,
            'command': command,
            'params': params if params else {},
        }
        if task_id is not None:
            data['id'] = task_id
        if period is not None:
            data['period'] = period
        if offset is not None:
            data['offset'] = offset
        res = self._conn.post(
            '/_api/tasks/{}'.format(task_id if task_id else ''),
            data=data
        )
        if res.status_code not in HTTP_OK:
            raise TaskCreateError(res)
        return res.body

    def delete_task(self, task_id, ignore_missing=False):
        """Delete the server task specified by ID.

        :param task_id: the ID of the server task
        :type task_id: str
        :param ignore_missing: ignore HTTP 404
        :type ignore_missing: bool
        :returns: the result of the deletion
        :rtype: bool
        :raises: TaskDeleteError
        """
        res = self._conn.delete('/_api/tasks/{}'.format(task_id))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and ignore_missing):
                raise TaskDeleteError(res)
        return not res.body['error']
