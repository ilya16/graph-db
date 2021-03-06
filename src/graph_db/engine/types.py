"""
Python types on which database engine operates using public interfaces.
"""
from typing import Union

__all__ = [
    'DB_TYPE',
    'BYTEORDER',
    'SIGNED',
    'ENCODING',
    'INVALID_ID',
    'DFS_CONFIG_PATH',
    'WORKER_PATH',
    'REPLICA_PATH',
    'NODE_STORAGE',
    'RELATIONSHIP_STORAGE',
    'PROPERTY_STORAGE',
    'LABEL_STORAGE',
    'DYNAMIC_STORAGE',
    'NODE_RECORD_SIZE',
    'RELATIONSHIP_RECORD_SIZE',
    'PROPERTY_RECORD_SIZE',
    'LABEL_RECORD_SIZE',
    'DYNAMIC_RECORD_SIZE',
    'DYNAMIC_RECORD_PAYLOAD_SIZE'
]

DB_TYPE = Union[str, int, float, bool]

BYTEORDER = 'big'
SIGNED = True
ENCODING = 'utf-8'
INVALID_ID = -1

DFS_CONFIG_PATH = 'configs/config.json'
WORKER_PATH = 'worker_instance_'
REPLICA_PATH = 'replica_'

NODE_STORAGE = 'node_storage.db'
RELATIONSHIP_STORAGE = 'relationship_storage.db'
PROPERTY_STORAGE = 'property_storage.db'
LABEL_STORAGE = 'label_storage.db'
DYNAMIC_STORAGE = 'dynamic_storage.db'

NODE_RECORD_SIZE = 13
RELATIONSHIP_RECORD_SIZE = 33
PROPERTY_RECORD_SIZE = 13
LABEL_RECORD_SIZE = 5
DYNAMIC_RECORD_SIZE = 32
DYNAMIC_RECORD_PAYLOAD_SIZE = 27


