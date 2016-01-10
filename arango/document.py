"""ArangoDB Collection."""

from collections import MutableMapping
import json

from arango.utils import camelify, uncamelify
from arango.exceptions import *
from arango.cursor import cursor
from arango.constants import COLLECTION_STATUSES, HTTP_OK


class Document(dict):
    """Wrapper for ArangoDB documents."""

    # Start by filling-out the abstract methods
    def __init__(self, data, api):
        self.update(data)

