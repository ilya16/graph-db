from graph_db.engine.label import Label
from graph_db.engine.relationship import Relationship
from graph_db.engine.node import Node
from graph_db.engine.property import Property
from graph_db.fs.io_engine import IOEngine
from graph_db.fs.worker import Worker


class Graph:

    def __init__(self, name, temp_dir):
        temp_storage = Worker(base_path=temp_dir)
        self.name = name
        self.io_engine = IOEngine()
        self.io_engine.add_worker(temp_storage)
        self.number_of_nodes = 0
        self.number_of_edges = 0
        self.number_of_labels = 0
        self.ids_nodes = dict()
        self.ids_edges = dict()

    def create_node(self, label, key, value):
        node = Node(label=Label(label))
        if key is not None and value is not None:
            prop = Property(key, value)
            node.add_property(prop)
        self.io_engine.insert_node(node)
        if label in self.ids_nodes:
            self.ids_nodes[label].append(node)
        else:
            self.ids_nodes[label] = [node]
        self.number_of_nodes += 1
        self.number_of_labels += 1

    def select_nth_node(self, n):
        return self.io_engine.select_node(n)

    def create_edge(self, label, start_node_label, end_node_label, key, value):
        edge = Relationship(label=Label(label),
                            start_node=self.ids_nodes[start_node_label][0],
                            end_node=self.ids_nodes[end_node_label][0])
        if key is not None and value is not None:
            prop = Property(key, value)
            edge.add_property(prop)
        self.io_engine.insert_relationship(edge)
        if label in self.ids_edges:
            self.ids_edges[label].append(edge)
        else:
            self.ids_edges[label] = [edge]
        self.number_of_edges += 1
        self.number_of_labels += 1

    def select_nth_edge(self, n):
        return self.io_engine.select_relationship(n)

    def select_edge_by_label(self, label):
        return self.ids_edges[label]

    def select_node_by_label(self, label):
        return self.ids_nodes[label]

    def close_engine(self):
        self.io_engine.close()

    def get_stats(self):
        return self.io_engine.get_stats()