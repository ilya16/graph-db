import os

# Manager config

# Default ports
DEFAULT_WORKER_PORTS = [8888, 8889]
DEFAULT_WORKER_REPLICA_PORT = (8888)
DEFAULT_MANAGER_PORTS = (2131, 2132)
DEFAULT_MANAGER_HOSTNAME = 'localhost'
DEFAULT_WORKER_HOSTNAMES = 'localhost'


base_config = {
    'NodeStorage': True,
    'RelationshipStorage': True,
    'LabelStorage': True,
    'PropertyStorage': True,
    'DynamicStorage': True
}

base_path = 'temp_db/'
worker_path = 'worker_instance_'
replica_path = 'replica_'

dfs_mode = {
    'Replicate' : True,
    'Distribute' : False
}

REPLICATE_FACTOR = 3

