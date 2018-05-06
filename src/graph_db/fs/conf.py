import os

# Manager config

# Default ports
DEFAULT_WORKER_PORTS = (8888, 8889, 8890, 8891)
DEFAULT_MANAGER_PORTS = (2131, 2132)

base_config = {
    'NodeStorage': True,
    'RelationshipStorage': True,
    'LabelStorage': True,
    'PropertyStorage': True,
    'DynamicStorage': True
}

base_path = 'temp_db/'

worker_path = 'worker_instance_'

dfs_mode = {
    'Replicate' : True,
    'Distribute' : False
}

REPLICATE_FACTOR = 3

