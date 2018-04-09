from typing import List

from .property import Property
from .node import Node


class Relationship:
    """ Define a single edge"""
    properties: List[Property] = []

    def __init__(self, type: str, start_node: Node, end_node: Node):
        self.type = type
        self.start_node = start_node
        self.end_node = end_node

    def set_start_node(self, start_node: Node):
        self.start_node = start_node

    def get_start_node(self) -> Node:
        return self.start_node

    def set_end_node(self, end_node: Node):
        self.end_node = end_node

    def get_end_node(self) -> Node:
        return self.end_node

    def set_type(self, type: str):
        self.type = type

    def get_type(self) -> str:
        return self.type

    def add_property(self, prop: Property):
        self.properties.append(prop)

    def get_edge_property_value(self, key):
        if any(key in d.key for d in self.properties):
            for prop in self.properties:
                try:
                    return prop.key
                except KeyError:
                    continue
        else:
            return None

    def get_edge_properties(self):
        return self.properties
