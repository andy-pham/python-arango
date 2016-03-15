"""ArangoDB's Top-Level API."""

from __future__ import absolute_import, unicode_literals

from datetime import datetime


from arango.database import Database
from arango.connection import Connection
from arango.exceptions import *
from arango.constants import HTTP_OK, LOG_LEVELS, DEFAULT_DB
from arango.clients import DefaultHTTPClient
from arango.utils import uncamelify, sanitize_str


class Arango(object):
    """Driver for ArangoDB's top-level APIs:

    1. Database Management
    2. User Management
    3. Administration & Monitoring
    4. Miscellaneous Functions
    """

    def __init__(self, protocol='http', host='localhost', port=8529,
                 username='root', password='', http_client=None,
                 verify_connection=False):
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
        :param http_client: the HTTP client to use under the hood
        :type http_client: arango.clients.base.BaseHTTPClient or None
        :param verify_connection: check the connection during instantiation
        :type verify_connection: bool
        :raises: arango.exceptions.ConnectionError
        """
        self._protocol = protocol
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._client = http_client if http_client else DefaultHTTPClient()

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
        if verify_connection:
            res = self._conn.head('/_api/version')
            if res.status_code not in HTTP_OK:
                raise ServerConnectionError(res)
        # Initialize the wrapper for the default database
        self._default_database = Database(self._conn)

    @property
    def protocol(self):
        return self._protocol

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    def __repr__(self):
        """Return a descriptive string of this instance."""
        return "<ArangoDB API driver pointing to '{}'>".format(self._host)

    def __getattr__(self, attr):
        """Call __getattr__ of the default database."""
        return getattr(self._default_database, attr)

    def __getitem__(self, item):
        """Call __getitem__ of the default database."""
        return self._default_database.collection(item)

    ###########################
    # Miscellaneous Functions #
    ###########################

    def get_version(self, details=False):
        """Return the version of the ArangoDB server.

        :param details: whether to include component information
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
        :raises: RequiredDatabaseVersionGetError
        """
        res = self._conn.get('/_admin/database/target-version')
        if res.status_code not in HTTP_OK:
            raise RequiredDatabaseVersionGetError(res)
        return str(res.body['version'])

    def get_system_time(self):
        """Return the current system time.

        :returns: the system time
        :rtype: datetime.datetime
        :raises: TimeGetError
        """
        res = self._conn.get('/_admin/time')
        if res.status_code not in HTTP_OK:
            raise TimeGetError(res)
        return datetime.fromtimestamp(res.body['time'])

    def get_wal(self):
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

    def get_transactions(self):
        """Return information about the currently running transactions.

        Fields in the returned dictionary:
            count: the number of current running transactions
            last_collected: min ID of the last collected logfile (at the start
                of running transaction). None if no transaction is running.
            last_sealed: min ID of the last sealed logfile (at the start of
                each running transaction). None if no transaction is running.

        :returns: the information about the currently running transactions
        :rtype: dict
        :raises: TransactionsGetError
        """
        res = self._conn.get('/_admin/wal/transactions')
        if res.status_code not in HTTP_OK:
            raise TransactionGetError(res)
        return {
            'count': res.body.get('runningTransactions'),
            'last_collected': res.body.get('minLastCollected'),
            'last_sealed': res.body.get('minLastSealed'),
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
            '/_admin/wal/flush',
            data={
                'waitForSync': wait_for_sync,
                'waitForCollector': wait_for_gc
            }
        )
        if res.status_code not in HTTP_OK:
            raise WriteAheadLogFlushError(res)
        return res.body

    def configure_wal(self, oversized_ops=None, log_size=None,
                      historic_logs=None, reserve_logs=None,
                      throttle_wait=None, throttle_limit=None):
        """Configure the parameters of the write-ahead log.

        When ``throttle_when_pending`` is set to 0, write-throttling will not
        be triggered at all.

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
        :raises: Write
        """
        data = dict()
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

    def echo(self):
        """Return information on the last request (headers, payload etc.)

        :returns: the information on the last request
        :rtype: dict
        :raises: EchoError
        """
        res = self._conn.get('/_admin/echo')
        if res.status_code not in HTTP_OK:
            raise EchoError(res)
        return {
            'headers': sanitize_str(res.body['headers']),
            'request_type': str(res.body['requestType']),
            'parameters': sanitize_str(res.body['parameters'])
        }

    def sleep(self, seconds):
        """Suspend the execution for a specified duration before returning.

        :param seconds: the amount of seconds to wait until the reply is sent
        :raises: SleepError
        """
        res = self._conn.get(
            '/_admin/sleep',
            params={'duration': seconds}
        )
        if res.status_code not in HTTP_OK:
            raise SleepError(res)
        return res.body.get('duration')

    def shutdown(self):
        """Initiate the server shutdown sequence.

        :raises: ShutdownError
        """
        res = self._conn.get('/_admin/shutdown')
        if res.status_code not in HTTP_OK:
            raise ShutdownError(res)
        return res.body

    def test(self, tests):
        """Run the available unittests on the server.

        :param tests: list of files containing the test suites
        :type tests: list
        :returns: the test result
        :rtype: dict
        :raises: TestsRunError
        """
        res = self._conn.post('/_admin/test', data={'tests': tests})
        if res.status_code not in HTTP_OK:
            raise TestsRunError(res)
        return res.body.get('passed')

    def execute(self, javascript):
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

    #######################
    # Database Management #
    #######################

    def list_databases(self, user_only=False):
        """"Return the database names.

        :param user_only: return only the user database names
        :type user_only: bool
        :returns: the database names
        :rtype: dict
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

    def db(self, name):
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
        :returns: the Database object
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

    def drop_database(self, name, ignore_error=False):
        """Drop the database of the specified name.

        :param name: the name of the database to delete
        :type name: str
        :param ignore_error: whether to ignore 404
        :type ignore_error: bool
        :raises: DatabaseDeleteError
        """
        res = self._conn.delete('/_api/database/{}'.format(name))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and ignore_error):
                raise DatabaseDeleteError(res)
        return res.body

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
        return [{
            'user': record['user'],
            'active': record['active'],
            'extra': record['extra'],
            'change_password': record['changePassword']
        } for record in res.body['result']]

    def user(self, username):
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
            'active': res.body.get('active'),
            'change_password': res.body.get('changePassword'),
            'extra': res.body.get('extra'),
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
            'active': res.body.get('active'),
            'extra': res.body.get('extra'),
            'change_password': res.body.get('changePassword'),
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
            'active': res.body.get('active'),
            'extra': res.body.get('extra'),
            'change_password': res.body.get('changePassword'),
        }

    def delete_user(self, username, safe_delete=False):
        """Delete an existing user.

        :param username: the name of the user
        :type username: str
        :param safe_delete: ignores HTTP 404 if set to True
        :type safe_delete: bool
        :raises: UserDeleteError
        """
        res = self._conn.delete('/_api/user/{user}'.format(user=username))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and safe_delete):
                raise UserDeleteError(res)
        return res.body

    ###############################
    # Administration & Monitoring #
    ###############################

    def read_log(self, upto=None, level=None, start=None, size=None,
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
        return res.body

    def reload_routing(self):
        """Reload the routing information from the collection ``routing``.

        :raises: RoutingInfoReloadError
        """
        res = self._conn.post('/_admin/routing/reload')
        if res.status_code not in HTTP_OK:
            raise RountingInfoReloadError(res)
        return res.body

    def statistics(self, description=False):
        """Return the server statistics.

        :param description: return the statistics description instead
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

    def role(self):
        """Return the role of the server in the cluster if applicable

        Possible return values are:

        SINGLE:
        COORDINATOR: the server is a coordinator in the cluster
        PRIMARY:     the server is a primary database in the cluster
        SECONDARY:   the server is a secondary database in the cluster
        UNDEFINED:   in a cluster, UNDEFINED is returned if the server role
                     cannot be determined. On a single server, UNDEFINED is
                     the only possible return value.

        :returns: the server role
        :rtype: unicode
        :raises: ServerRoleGetError
        """
        res = self._conn.get('/_admin/server/role')
        if res.status_code not in HTTP_OK:
            raise ServerRoleGetError(res)
        return res.body.get('role')

    def endpoints(self):
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

    #########
    # Tasks #
    #########

    def tasks(self):
        """Return all server tasks that are currently active.

        :returns: list of server tasks that are currently active
        :rtype: list
        :raises: TaskGetError
        """
        res = self._conn.get('/_api/tasks')
        if res.status_code not in HTTP_OK:
            raise TaskGetError(res)
        return res.body

    def task(self, id):
        """Return the active server task with the given id.

        :param id: the id of the server task
        :type id: str
        :returns: the details on the active task
        :rtype: dict
        :raises: TaskGetError
        """
        res = self._conn.get('/_api/tasks/{}'.format(id))
        if res.status_code not in HTTP_OK:
            raise TaskGetError(res)
        return res.body

    # TODO verify which arguments are optional
    def create_task(self, name, command, period=None, offset=None, params=None,
                    id=None):
        """Create a new task with a generated ID.

        :param name: the name of the task
        :type name: str
        :param command: the Javascript code to execute
        :type command: str
        :param period: the number of seconds between the executions
        :type period: int
        :param params: the parameters passed into the command
        :type params: dict

        :param offset: the initial delay in seconds
        :type offset: int
        :return: the details on the new task created
        :raises: TaskCreateError
        """
        data = {
            'name': name,
            'command': command,
            'params': params if params else {},
        }
        if id is not None:
            data['id'] = id
        if period is not None:
            data['period'] = period
        if offset is not None:
            data['offset'] = offset
        res = self._conn.put(
            '/_api/tasks/{}'.format(id if id else ''),
            data=data
        )
        if res.status_code not in HTTP_OK:
            raise TaskCreateError(res)
        return res.body

    def delete_task(self, id):
        """Delete the server task specified by ID.

        :param id: the ID of the server task
        :type id: str
        :returns: the result of the deletion
        :rtype: dict
        :raises: TaskDeleteError
        """
        res = self._conn.delete('/_api/tasks/{}'.format(id))
        if res.status_code not in HTTP_OK:
            raise TaskDeleteError(res)
        return res.body
