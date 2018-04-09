from graph_db.engine.label import Label
from graph_db.engine.node import Node
from graph_db.fs.fs_manager import DBFSManager
from graph_db.fs.worker import WorkerFSManager, WorkerConfig

if __name__ == '__main__':
    dbfs_manager = DBFSManager()
    local_storage = WorkerFSManager(base_path='local-db/', config=WorkerConfig())
    dbfs_manager.add_worker(local_storage)
    print(dbfs_manager.get_stats())

    n = Node(label=Label('TEST', id=35))
    print(n)
    # dbfs_manager.insert_node(n)

    print(dbfs_manager.select_node(0))