from __future__ import absolute_import, unicode_literals

import random
from datetime import datetime

import pytest
from six import string_types

from arango import Arango
from arango.constants import DEFAULT_DB
from arango.database import Database
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_user_name,
    generate_task_name
)
from arango.version import VERSION


def setup_module(*_):
    global driver, db_name, username, task_name, task_id

    driver = Arango()
    db_name = generate_db_name(driver)
    username = generate_user_name(driver)
    task_name = generate_task_name(driver)
    task_id = ''


def teardown_module(*_):
    driver.drop_database(db_name, ignore_missing=True)
    driver.delete_user(username, ignore_missing=True)
    driver.delete_task(task_id, ignore_missing=True)


def test_properties():
    assert driver.protocol == 'http'
    assert driver.host == 'localhost'
    assert driver.port == 8529
    assert driver.version == VERSION
    assert 'ArangoDB API driver' in repr(driver)


def test_server_version():
    version = driver.server_version()
    assert isinstance(version, string_types)


def test_server_details():
    details = driver.server_details()
    assert 'architecture' in details
    assert 'server-version' in details


def test_target_version():
    version = driver.target_version()
    assert isinstance(version, string_types)


def test_server_statistics():
    statistics = driver.server_statistics(description=False)
    assert isinstance(statistics, dict)
    assert 'time' in statistics
    assert 'system' in statistics
    assert 'server' in statistics

    description = driver.server_statistics(description=True)
    assert isinstance(description, dict)
    assert 'figures' in description
    assert 'groups' in description


def test_server_role():
    assert driver.server_role() in {
        'SINGLE',
        'COORDINATOR',
        'PRIMARY',
        'SECONDARY',
        'UNDEFINED'
    }


def test_server_time():
    system_time = driver.server_time()
    assert isinstance(system_time, datetime)


def test_echo():
    last_request = driver.echo()
    assert 'protocol' in last_request
    assert 'user' in last_request
    assert 'requestType' in last_request
    assert 'rawRequestBody' in last_request


def test_sleep():
    assert driver.sleep(1) == 1
    assert driver.sleep(2) == 2


# def test_shutdown():
#     assert isinstance(driver.shutdown(), bool)


# def test_run_tests():
#     assert isinstance(driver.run_tests, dict)


def test_run_program():
    assert driver.run_program('return 1') == '1'
    assert driver.run_program('return "test"') == '"test"'
    with pytest.raises(ProgramExecuteError) as err:
        driver.run_program('return invalid')
    assert err.value.message == 'Internal Server Error'


# TODO test parameters
def test_read_log():
    log = driver.read_log()
    assert 'lid' in log
    assert 'level' in log
    assert 'text' in log
    assert 'total_amount' in log


def test_reload_routing():
    result = driver.reload_routing()
    assert isinstance(result, bool)


def test_endpoints():
    endpoints = driver.endpoints()
    assert isinstance(endpoints, list)
    for endpoint in endpoints:
        assert 'databases' in endpoint
        assert 'endpoint' in endpoint


# TODO something wrong here
def test_list_databases():
    result = driver.list_databases()
    assert DEFAULT_DB in result

    result = driver.list_databases(user_only=True)
    assert DEFAULT_DB in result


def test_get_database():
    db = driver.db(DEFAULT_DB)
    assert isinstance(db, Database)
    assert db.name == DEFAULT_DB


def test_database_mgnt():
    assert db_name not in driver.list_databases()

    # Test create database
    db = driver.create_database(db_name)
    assert db_name in driver.list_databases()
    assert isinstance(db, Database)
    assert db.name == db_name

    # Test get after create database
    db = driver.db(db_name)
    assert isinstance(db, Database)
    assert db.name == db_name

    # Test create duplicate database
    with pytest.raises(DatabaseCreateError):
        driver.create_database(db_name)

    # Test list after create database
    assert db_name in driver.list_databases()

    # Test drop database
    result = driver.drop_database(db_name)
    assert result is True
    assert db_name not in driver.list_databases()

    # Test drop missing database
    with pytest.raises(DatabaseDeleteError):
        driver.drop_database(db_name)

    # Test drop missing database (ignore missing)
    result = driver.drop_database(db_name, ignore_missing=True)
    assert result is False


def test_wal_options():
    options = driver.wal_options()
    assert 'oversized_ops' in options
    assert 'log_size' in options
    assert 'historic_logs' in options
    assert 'reserve_logs' in options


def test_set_wal_options():
    driver.set_wal_options(
        historic_logs=15,
        oversized_ops=False,
        log_size=30000000,
        reserve_logs=5,
        throttle_limit=1000,
        throttle_wait=16000
    )
    options = driver.wal_options()
    assert options['historic_logs'] == 15
    assert options['oversized_ops'] is False
    assert options['log_size'] == 30000000
    assert options['reserve_logs'] == 5
    assert options['throttle_limit'] == 1000
    assert options['throttle_wait'] == 16000


def test_flush_wal():
    assert isinstance(driver.flush_wal(), bool)


def test_wal_transactions():
    result = driver.wal_transactions()
    assert 'count' in result
    assert 'last_sealed' in result
    assert 'last_collected' in result


def test_user_management():
    # Test get users
    users = driver.list_users()
    assert isinstance(users, dict)
    assert 'root' in users

    root_user = users['root']
    assert 'active' in root_user
    assert 'extra'in root_user
    assert 'change_password' in root_user

    assert username not in driver.list_users()

    # Test create user
    user = driver.create_user(
        username,
        'password',
        active=True,
        extra={'hello': 'world'},
        change_password=False,
    )
    assert user['active'] is True
    assert user['extra'] == {'hello': 'world'}
    assert user['change_password'] is False
    assert username in driver.list_users()

    # Test create duplicate user
    with pytest.raises(UserCreateError):
        driver.create_user(username, 'password')

    missing = generate_user_name(driver)

    # Test update user
    user = driver.update_user(
        username,
        password='test',
        active=False,
        extra={'foo': 'bar'},
        change_password=True
    )
    assert user['active'] is False
    assert user['extra'] == {
        'hello': 'world',
        'foo': 'bar'
    }
    assert user['change_password'] is True

    # Test update missing user
    with pytest.raises(UserUpdateError):
        driver.update_user(
            missing,
            password='test',
            active=False,
            extra={'foo': 'bar'},
            change_password=True
        )

    # Test replace user
    user = driver.replace_user(
        username,
        password='test',
        active=True,
        extra={'foo': 'baz'},
        change_password=False
    )
    assert user['active'] is True
    assert user['extra'] == {'foo': 'baz'}
    assert user['change_password'] is False

    # Test replace missing user
    with pytest.raises(UserReplaceError):
        driver.replace_user(
            missing,
            password='test',
            active=True,
            extra={'foo': 'baz'},
            change_password=False
        )

    # Test delete user
    result = driver.delete_user(username)
    assert result is True
    assert username not in driver.list_users()

    # Test delete missing user
    with pytest.raises(UserDeleteError):
        driver.delete_user(username)

    # Test delete missing user (ignore missing)
    result = driver.delete_user(username, ignore_missing=True)
    assert result is False


def test_task_management():
    global task_id

    # Test get tasks
    tasks = driver.get_tasks()
    assert isinstance(tasks, dict)
    for task in tasks.values():
        assert 'command' in task
        assert 'created' in task
        assert 'database' in task
        assert 'id' in task
        assert 'name' in task

    # Test get task
    tasks = driver.get_tasks()
    if tasks:
        chosen_task_id = random.choice(tasks.keys())
        retrieved_task = driver.get_task(chosen_task_id)
        assert tasks[chosen_task_id] == retrieved_task

    cmd = "(function(params) { require('internal').print(params); })(params)"

    # Test create task
    assert task_name not in driver.get_tasks()
    task = driver.create_task(
        name=task_name,
        command=cmd,
        params={'foo': 'bar', 'bar': 'foo'},
        period=2,
        offset=3,
    )
    task_id = task['id']
    assert task_id in driver.get_tasks()
    assert task_name == driver.get_tasks()[task_id]['name']

    # Test get after create task
    task = driver.get_task(task_id)
    assert task['command'] == cmd
    assert task['name'] == task_name
    assert task['period'] == 2

    # Test create duplicate task (with ID)
    with pytest.raises(TaskCreateError):
        task = driver.create_task(
            task_id=task_id,
            name=task_name,
            command=cmd,
            params={'foo': 'bar', 'bar': 'foo'},
            period=3,
            offset=4,
        )

    # Test delete task
    result = driver.delete_task(task['id'])
    assert result is True
    assert task_id not in driver.get_tasks()

    # Test delete missing task
    with pytest.raises(TaskDeleteError):
        driver.delete_task(task['id'])

    # Test delete missing task (ignore missing)
    result = driver.delete_task(task['id'], ignore_missing=True)
    assert result is False

    # Test create task with ID
    task = driver.create_task(
        task_id=task_id,
        name=task_name,
        command=cmd,
        params={'foo': 'bar', 'bar': 'foo'},
        period=3,
        offset=4,
    )
    assert task['id'] == task_id
    assert task['command'] == cmd
    assert task['name'] == task_name
    assert task['period'] == 3

    # Test get after create task with ID
    task = driver.get_task(task_id)
    assert task['id'] == task_id
    assert task['command'] == cmd
    assert task['name'] == task_name
    assert task['period'] == 3


# def test_execute_transaction():
#     # Test execute transaction with no params
#     action = """
#         function () {{
#             var db = require('internal').db;
#             db.{col}.save({{ _key: 'doc1'}});
#             db.{col}.save({{ _key: 'doc2'}});
#             return 'success!';
#         }}
#     """.format(col=col_name)
#
#     result = db.execute_transaction(
#         action=action,
#         read_collections=[col_name],
#         write_collections=[col_name],
#         sync=True,
#         lock_timeout=10000
#     )
#     assert result == 'success!'
#     assert 'doc1' in collection
#     assert 'doc2' in collection
#
#     # Test execute transaction with params
#     action = """
#         function (params) {{
#             var db = require('internal').db;
#             db.{col}.save({{ _key: 'doc3', val: params.val1 }});
#             db.{col}.save({{ _key: 'doc4', val: params.val2 }});
#             return 'success!';
#         }}
#     """.format(col=col_name)
#
#     result = db.execute_transaction(
#         action=action,
#         read_collections=[col_name],
#         write_collections=[col_name],
#         params={"val1": 1, "val2": 2},
#         sync=True,
#         lock_timeout=10000
#     )
#     assert result == 'success!'
#     assert 'doc3' in collection
#     assert 'doc4' in collection
#     assert collection["doc3"]["val"] == 1
#     assert collection["doc4"]["val"] == 2
