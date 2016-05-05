from __future__ import absolute_import, unicode_literals

from collections import Mapping, Iterable

def generate_db_name(driver):
    """Generate and return the next available database name.

    :param driver: ArangoDB driver object
    :type driver: arango.Arango
    :returns: the next available database name
    :rtype: str
    """
    num = 0
    existing = set(driver.list_databases())
    while "test_database_{num:03d}".format(num=num) in existing:
        num += 1
    return "test_database_{num:03d}".format(num=num)


def generate_col_name(database):
    """Generate and return the next available collection name.

    :param database: ArangoDB database
    :type database: arango.Arango or arango.database.ArangoDatabase
    :returns: the next available collection name
    :rtype: str
    """
    num = 0
    existing = set(database.list_collections())
    while "test_collection_{num:03d}".format(num=num) in existing:
        num += 1
    return "test_collection_{num:03d}".format(num=num)


def generate_graph_name(database):
    """Generate and return the next available collection name.

    :param database: ArangoDB database
    :type database: arango.Arango or arango.database.ArangoDatabase
    :returns: the next available graph name
    :rtype: str
    """
    num = 0
    existing = set(database.list_graphs())
    while "test_graph_{num:03d}".format(num=num) in existing:
        num += 1
    return "test_graph_{num:03d}".format(num=num)


def generate_task_name(driver):
    """Generate and return the next available task name.

    :param driver: ArangoDB driver
    :type driver: arango.Arango or arango.database.ArangoDatabase
    :returns: the next available user name
    :rtype: str
    """
    num = 0
    existing = set(task['name'] for task in driver.get_tasks().values())
    while "test_task_{num:03d}".format(num=num) in existing:
        num += 1
    return "test_task_{num:03d}".format(num=num)


def generate_user_name(driver):
    """Generate and return the next available user name.

    :param driver: ArangoDB driver
    :type driver: arango.Arango or arango.database.ArangoDatabase
    :returns: the next available user name
    :rtype: str
    """
    num = 0
    existing = set(driver.list_users())
    while "test_user_{num:03d}".format(num=num) in existing:
        num += 1
    return "test_user_{num:03d}".format(num=num)


def clean_keys(obj):
    """Return the document(s) with all the system keys stripped.

    :param obj: document(s)
    :type obj: list or dict
    :returns: the document(s) with the system keys stripped
    :rtype: list or dict
    """
    if isinstance(obj, Mapping):
        return {
            k: v for k, v in obj.items()
            if not (k != '_key' and k.startswith("_"))
        }
    elif isinstance(obj, Iterable):
        return [
            {
                k: v for k, v in document.items()
                if not (k != '_key' and k.startswith("_"))
            }
            for document in obj
        ]