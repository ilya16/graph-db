"""
Python types on which database engine operates using public interfaces.
"""

__all__ = [
    'BYTEORDER',
    'MEMORY',
    'NODE_STORAGE',
    'RELATIONSHIP_STORAGE',
    'NODE_RECORD_SIZE',
    'RELATIONSHIP_RECORD_SIZE'
]

BYTEORDER = 'big'

MEMORY = 'memory:'
DB_PATH = 'graph_db/'

NODE_STORAGE = 'node_storage.db'
RELATIONSHIP_STORAGE = 'relationship_storage.db'

NODE_RECORD_SIZE = 13
RELATIONSHIP_RECORD_SIZE = 34
PROPERTY_RECORD_SIZE = 20
LABEL_RECORD_SIZE = 5
DYNAMIC_RECORD_SIZE = 36
