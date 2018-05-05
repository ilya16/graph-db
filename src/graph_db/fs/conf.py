import os

# Manager config

# Default ports
DEFAULT_WORKER_PORTS = (8888, 8889, 8890, 8891)
DEFAULT_MANAGER_PORTS = (2131, 2132)

LOG_DIR = '/tmp/worker/log'
if not os.path.isdir(LOG_DIR):
    os.makedirs(LOG_DIR)

DATA_DIR = '/tmp/worker/data'
if not os.path.isdir(DATA_DIR):
    os.makedirs(DATA_DIR)

def clean():
    for f in os.listdir(DATA_DIR):
        os.remove(DATA_DIR+f)

base_config = {
    'NodeStorage': True,
    'RelationshipStorage': True,
    'LabelStorage': True,
    'PropertyStorage': True,
    'DynamicStorage': True
}

base_path = '/tmp/worker/db'