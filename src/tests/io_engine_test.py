from graph_db.engine.label import Label
from graph_db.engine.relationship import Relationship
from graph_db.engine.node import Node
from graph_db.fs.io_engine import IOEngine
from graph_db.fs.worker import Worker, WorkerConfig

if __name__ == '__main__':
    io_engine = IOEngine()

    local_storage = Worker(base_path='local-db/', config=WorkerConfig())

    io_engine.add_worker(local_storage)
    print(io_engine.get_stats())

    n = Node(label=Label('TEST', id=1))
    print(n)
    io_engine.insert_node(n)

    print(io_engine.select_node(0))