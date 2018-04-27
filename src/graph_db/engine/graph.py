from graph_db.engine.label import Label
from graph_db.engine.relationship import Relationship
from graph_db.engine.node import Node
from graph_db.fs.io_engine import IOEngine
from graph_db.fs.worker import Worker


class Graph:

    def __init__(self, name, temp_dir):
        self.name = name
        self.io_engine = IOEngine()
        temp_storage = Worker(base_path=temp_dir)
        self.io_engine.add_worker(temp_storage)
        self.number_of_nodes = 0
        self.number_of_edges = 0
        self.number_of_labels = 0
        self.ids = {}

    def create_node(self, label):
        node = Node(label=Label(label))
        self.io_engine.insert_node(node)
        self.ids[label] = node
        self.number_of_nodes += 1
        self.number_of_labels += 1

    def select_nth_node(self, n):
        return self.io_engine.select_node(n)

    def create_edge(self, label, start_node_label, end_node_label):
        edge = Relationship(label=Label(label),
                            start_node=self.ids[start_node_label],
                            end_node=self.ids[end_node_label])
        self.number_of_edges += 1
        self.number_of_labels += 1
        self.io_engine.insert_relationship(edge)

    def select_nth_edge(self, n):
        return self.io_engine.select_relationship(n)

    def close_engine(self):
        self.io_engine.close()

    def get_stats(self):
        return self.io_engine.get_stats()