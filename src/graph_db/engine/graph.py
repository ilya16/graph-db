from typing import Dict

from graph_db.engine.label import Label
from graph_db.engine.relationship import Relationship
from graph_db.engine.node import Node


class Graph:
    def __init__(self, name: str):
        self._name = name
        self._nodes = dict()
        self._relationships = dict()
        self._labels = dict()

        self._empty = True
        self._consistent = True

    def set_name(self, name: str):
        self._name = name
        
    def get_name(self) -> str:
        return self._name

    def add_node(self, node: Node):
        self._nodes[node.get_id()] = node

    def get_node(self, node_id: int) -> Node:
        return self._nodes[node_id] if node_id in self._nodes else None

    def get_nodes(self) -> Dict[int, Node]:
        return self._nodes

    def add_relationship(self, rel: Relationship):
        self._relationships[rel.get_id()] = rel

    def get_relationship(self, rel_id) -> Relationship:
        return self._relationships[rel_id] if rel_id in self._relationships else None

    def get_relationships(self) -> Dict[int, Relationship]:
        return self._relationships

    def add_label(self, label: Label):
        self._labels[label.get_id()] = label

    def get_label(self, label_id: int) -> Label:
        return self._labels[label_id] if label_id in self._labels else None

    def get_labels(self) -> Dict[int, Label]:
        return self._labels

    def set_empty(self, empty: bool):
        self._empty = empty

    def is_empty(self) -> bool:
        return self._empty

    def set_consistent(self, consistent: bool):
        self._consistent = consistent

    def is_consistent(self) -> bool:
        return self._consistent

    def clear(self):
        self._nodes = dict()
        self._relationships = dict()
        self._labels = dict()
        self._empty = True
        self._consistent = False

#     def update_node(self, node_id, prop):
#         node = self.select_node_by_id(node_id)
#         if node is not None:
#             p = {prop.get_key(): prop.get_value()}
#             key = frozenset(p.items())
#             if key in self.properties:
#                 self.properties[key].append(node)
#             else:
#                 self.properties[key] = [node]
#             node.add_property(prop)
#             return self.io_engine.update_node(node)
#         else:
#             return None

#     def update_relationship(self, rel_id, prop):
#         rel = self.select_relationship_by_id(rel_id)
#         if rel is not None:
#             p = {prop.get_key(): prop.get_value()}
#             key = frozenset(p.items())
#             if key in self.properties:
#                 self.properties[key].append(rel)
#             else:
#                 self.properties[key] = [rel]
#             rel.add_property(prop)
#             return self.io_engine.update_relationship(rel)
#         else:
#             return None

#     def delete_node(self, node_id):
#         node = self.select_node_by_id(node_id)
#         if node is not None:
#             node.set_used(False)
#             return self.io_engine.update_node(node)
#         else:
#             return None

#     def delete_relationship(self, rel_id):
#         rel = self.select_relationship_by_id(rel_id)
#         if rel is not None:
#             rel.set_used(False)
#             return self.io_engine.update_relationship(rel)
#         else:
#             return None