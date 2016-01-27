"""Utility Functions."""

import importlib
from re import sub
from collections import Mapping, Iterable
try:
    builtins = importlib.import_module('__builtin__')
except ImportError:
    builtins = importlib.import_module('builtins')


def is_str(obj):
    """Return True iff ``obj`` is an instance of str or unicode.

    :param obj: the object to check
    :type obj: object
    :returns: True iff ``obj`` is an instance of str/unicode
    :rtype: bool
    """
    base_str = getattr(builtins, 'basestring', None)
    return isinstance(obj, base_str) if base_str else isinstance(obj, str)


def unicode_to_str(obj):
    """Convert any unicode in ``obj`` to str and return it.

    :param obj: the object to sanitize
    :type obj: object
    :returns: the sanitized object
    :rtype: object
    """
    if is_str(obj):
        return str(obj)
    elif isinstance(obj, Mapping):
        return dict(map(unicode_to_str, obj.items()))
    elif isinstance(obj, Iterable):
        return type(obj)(map(unicode_to_str, obj))
    else:
        return obj


def camelify(obj):
    """Convert any string in ``obj`` from snake case to camel case.

    All strings are assumed to be in snake case in the beginning.

    :param obj: the object to camelify
    :type obj: object or str or Mapping or Iterable
    :returns: the camelified object
    :rtype: object
    """
    if is_str(obj):
        words = obj.split("_")
        return words[0] + "".join(word.title() for word in words[1:])
    elif isinstance(obj, Mapping):
        return dict(map(camelify, obj.items()))
    elif isinstance(obj, Iterable):
        return type(obj)(map(camelify, obj))
    else:
        return obj


def uncamelify(obj):
    """Convert any strings in ``obj`` from camel case to snake case.

    All strings are assumed to be in camel case in the beginning.

    :param obj: the object to uncamelify
    :type obj: object or str or Mapping or Iterable
    :returns: the uncamelified object
    :rtype: object
    """
    if is_str(obj):
        return sub('(?!^)([A-Z]+)', r'_\1', obj).lower()
    elif isinstance(obj, Mapping):
        return dict(map(uncamelify, obj.items()))
    elif isinstance(obj, Iterable):
        return type(obj)(map(uncamelify, obj))
    else:
        return obj


def filter_keys(dictionary, filtered):
    """Return a new dictionary with the specified keys filtered out.

    :param dictionary: the dictionary object
    :type dictionary: dict
    :param filtered: the list of keys to filter
    :type filtered: list
    :returns: the dictionary with the specified keys filtered
    :rtype: dict
    """
    return {k: v for k, v in dictionary.items() if k not in filtered}
