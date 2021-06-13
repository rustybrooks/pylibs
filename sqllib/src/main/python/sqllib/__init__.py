import sys

STRING_TYPE = (str, unicode) if (sys.version_info < (3, 0)) else str
DICT_TYPE = dict
LIST_TYPE = (list, tuple)

from .sql import SQLBase, Migration, MigrationStatement, chunked, sql_factory
from .structures import dictobj
