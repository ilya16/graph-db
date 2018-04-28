from typing import List

from graph_db.engine.label import Label
from .property import Property
from .node import Node


class Relationship:
    """ Relationship between two nodes in a Graph. """
    _properties: List[Property] = []

    def __init__(self,
                 label: Label,
                 start_node: Node,
                 end_node: Node,
                 id: int = -1,
                 start_prev_rel: 'Relationship' = None,
                 start_next_rel: 'Relationship' = None,
                 end_prev_rel: 'Relationship' = None,
                 end_next_rel: 'Relationship' = None,
                 used: bool = True):
        self._id = id
        self._label = label
        self._start_node = start_node
        self._end_node = end_node
        self._start_prev_rel = start_prev_rel
        self._start_next_rel = start_next_rel
        self._end_prev_rel = end_prev_rel
        self._end_next_rel = end_next_rel
        self._used = used

    def set_id(self, id: int):
        self._id = id

    def get_id(self) -> int:
        return self._id

    def set_start_node(self, start_node: Node):
        self._start_node = start_node

    def get_start_node(self) -> Node:
        return self._start_node

    def set_end_node(self, end_node: Node):
        self._end_node = end_node

    def get_end_node(self) -> Node:
        return self._end_node

    def get_label(self) -> Label:
        return self._label

    def set_label(self, label: Label):
        self._label = label

    def add_property(self, prop: Property):
        self._properties.append(prop)

    def get_rel_property_value(self, key):
        if any(key in p.get_key() for p in self._properties):
            for prop in self._properties:
                try:
                    return prop.get_key()
                except KeyError:
                    continue
        else:
            return None

    def get_rel_properties(self):
        return self._properties

    def get_first_property(self):
        if self._properties:
            return self._properties[0]
        else:
            return None

    def set_start_prev_rel(self, start_prev_rel):
        self._start_prev_rel = start_prev_rel

    def get_start_prev_rel(self):
        return self._start_prev_rel

    def set_start_next_rel(self, start_next_rel):
        self._start_next_rel = start_next_rel

    def get_start_next_rel(self):
        return self._start_next_rel

    def set_end_prev_rel(self, end_prev_rel):
        self._end_prev_rel = end_prev_rel

    def get_end_prev_rel(self):
        return self._end_prev_rel

    def set_end_next_rel(self, end_next_rel):
        self._end_next_rel = end_next_rel

    def get_end_next_rel(self):
        return self._end_next_rel

    def set_used(self, used: bool):
        self._used = used

    def is_used(self) -> bool:
        return self._used

    def __str__(self) -> str:
        return f'Edge #{self._id} = {{' \
               f'label: {self._label.get_name()}, ' \
               f'first_property: {self.get_first_property()}, ' \
               f'start_node: {self.get_start_node()}, ' \
               f'end_node: {self.get_end_node()}, ' \
               f'used: {self._used}' \
               f'}}'
