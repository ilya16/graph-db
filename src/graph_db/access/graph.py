from graph_db.engine.label import Label
from graph_db.engine.relationship import Relationship
from graph_db.engine.node import Node
from graph_db.fs.io_engine import IOEngine
from graph_db.fs.worker import Worker, WorkerConfig


class Graph:

    def __init__(self, name):
        self.name = name
        self.io_engine = IOEngine()
        self.local_storage = Worker(base_path='tests/local-db/', config=WorkerConfig())
        self.io_engine.add_worker(self.local_storage)

    def create_node(self, label):
        n = Node(label=Label(label, id=1))
        self.io_engine.insert_node(n)

    def show_node(self):
        print(self.io_engine.select_node(0))