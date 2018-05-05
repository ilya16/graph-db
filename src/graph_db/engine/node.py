from typing import List, Union

from .property import Property
from graph_db.engine.types import INVALID_ID
from .label import Label


class Node:
    """ Node in a Graph. """

    def __init__(self,
                 label: Label,
                 id: int = INVALID_ID,
                 properties: List[Property] = list(),
                 used: bool = True):
        self._id = id
        self._label = label
        self._used = used
        self._properties = properties
        self._relationships = []

        self._init_properties()

    def _init_properties(self):
        for i in range(len(self._properties) - 1):
            self._properties[i].set_next_property(self._properties[i + 1])

    def set_id(self, id: int):
        self._id = id

    def get_id(self) -> int:
        return self._id

    def set_label(self, label: Label):
        self._label = label

    def get_label(self) -> Label:
        return self._label

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

    def add_relationship(self, rel):
        assert self == rel.get_start_node() or self == rel.get_end_node()
        self._relationships.append(rel)

    def remove_relationship(self, rel):
        assert rel in self._relationships
        self._relationships.remove(rel)

    def get_relationships(self):
        return self._relationships

    def get_first_relationship(self):
        return self._relationships[0] if self._relationships else None

    def get_last_relationship(self):
        return self._relationships[-1] if self._relationships else None

    def set_used(self, used: bool):
        self._used = used

    def is_used(self) -> bool:
        return self._used

    def remove_dependencies(self):
        for rel in self._relationships:
            rel

        start_node.remove_relationship(self)
        end_node.remove_relationship(self)

    def __str__(self) -> str:
        properties_str = " ".join(map(str, self._properties)) if self._properties else None
        return f'Node #{self._id} = {{' \
               f'label: {self._label}, ' \
               f'properties: {properties_str}, ' \
               f'used: {self._used}' \
               f'}}'
