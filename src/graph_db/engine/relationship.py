from typing import Union, List

from graph_db.engine.label import Label
from graph_db.engine.types import INVALID_ID
from .property import Property
from .node import Node


class Relationship:
    """ Relationship between two nodes in a Graph. """

    def __init__(self,
                 label: Label,
                 start_node: Node,
                 end_node: Node,
                 id: int = INVALID_ID,
                 start_prev_rel: 'Relationship' = None,
                 start_next_rel: 'Relationship' = None,
                 end_prev_rel: 'Relationship' = None,
                 end_next_rel: 'Relationship' = None,
                 properties: List[Property] = list(),
                 used: bool = True):
        self._id = id
        self._label = label
        self._start_node = start_node
        self._end_node = end_node
        self._start_prev_rel = start_prev_rel
        self._start_next_rel = start_next_rel
        self._end_prev_rel = end_prev_rel
        self._end_next_rel = end_next_rel
        self._properties = properties
        self._used = used

        self._init_properties()
        self._init_dependencies()

    def _init_properties(self):
        for i in range(len(self._properties) - 1):
            self._properties[i].set_next_property(self._properties[i + 1])

    def _init_dependencies(self):
        start_node = self._start_node
        end_node = self._end_node

        start_prev_rel = start_node.get_last_relationship()
        if start_prev_rel:
            assert start_node == start_prev_rel.get_start_node() \
                   or start_node == start_prev_rel.get_end_node()
            if start_node == start_prev_rel.get_start_node():
                start_prev_rel.set_start_next_rel(self)
            else:
                start_prev_rel.set_end_next_rel(self)
            self.set_start_prev_rel(start_prev_rel)
        start_node.add_relationship(self)

        end_prev_rel = end_node.get_last_relationship()
        if end_prev_rel:
            assert end_node == end_prev_rel.get_start_node() \
                   or end_node == end_prev_rel.get_end_node()
            if end_node == end_prev_rel.get_start_node():
                end_prev_rel.set_start_next_rel(self)
            else:
                end_prev_rel.set_end_next_rel(self)
            self.set_end_prev_rel(end_prev_rel)
        end_node.add_relationship(self)

    def set_id(self, id: int):
        self._id = id

    def get_id(self) -> int:
        return self._id

    def get_start_node(self) -> Node:
        return self._start_node

    def get_end_node(self) -> Node:
        return self._end_node

    def get_label(self) -> Label:
        return self._label

    def set_label(self, label: Label):
        self._label = label

    def add_property(self, prop: Property):
        if self._properties:
            self.get_last_property().set_next_property(prop)
        self._properties.append(prop)

    def get_properties(self) -> List[Property]:
        return self._properties

    def get_first_property(self) -> Union[Property, None]:
        return self._properties[0] if self._properties else None

    def get_last_property(self) -> Union[Property, None]:
        return self._properties[-1] if self._properties else None

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

    def remove_dependencies(self):
        start_node = self._start_node
        end_node = self._end_node

        start_prev_rel = self.get_start_prev_rel()
        start_next_rel = self.get_start_next_rel()
        end_prev_rel = self.get_end_prev_rel()
        end_next_rel = self.get_end_next_rel()

        if start_prev_rel:
            if start_node == start_prev_rel.get_start_node():
                start_prev_rel.set_start_next_rel(start_next_rel)
            else:
                start_prev_rel.set_end_next_rel(start_next_rel)
            if start_next_rel:
                if start_node == start_next_rel.get_start_node():
                    start_next_rel.set_start_prev_rel(start_prev_rel)
                else:
                    start_next_rel.set_end_prev_rel(start_prev_rel)

        if end_prev_rel:
            if end_node == end_prev_rel.get_start_node():
                end_prev_rel.set_start_next_rel(end_next_rel)
            else:
                end_prev_rel.set_end_next_rel(end_next_rel)
            if end_next_rel:
                if end_node == end_next_rel.get_start_node():
                    end_next_rel.set_start_prev_rel(end_prev_rel)
                else:
                    end_next_rel.set_end_prev_rel(end_prev_rel)

        self.set_start_prev_rel(None)
        self.set_start_next_rel(None)
        self.set_end_prev_rel(None)
        self.set_end_next_rel(None)

        start_node.remove_relationship(self)
        end_node.remove_relationship(self)

    def __str__(self) -> str:
        properties_str = " ".join(map(str, self._properties)) if self._properties else None
        return f'Edge #{self._id} = {{' \
               f'label: {self._label}, ' \
               f'properties: {properties_str}, ' \
               f'start_node: {self.get_start_node()}, ' \
               f'end_node: {self.get_end_node()}, ' \
               f'used: {self._used}' \
               f'}}'
