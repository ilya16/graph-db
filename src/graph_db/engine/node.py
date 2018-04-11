from typing import List

from graph_db.engine.property import Property
from graph_db.engine.types import DB_TYPE
from .label import Label


class Node:
    """ Node in a Graph. """
    _properties = []
    _relationships = []

    def __init__(self, label: Label, id: int = -1, used: bool = True):
        self._id = id
        self._label = label
        self._used = used

    def set_id(self, id: int):
        self._id = id

    def get_id(self) -> int:
        return self._id

    def get_label(self) -> Label:
        return self._label

    def set_label(self, label: Label):
        self._label = label

    def add_property(self, prop: Property):
        self._properties.append(prop)

    def get_property_value(self, key: DB_TYPE) -> DB_TYPE:
        if any(key in d.key for d in self._properties):
            for prop in self._properties:
                try:
                    return prop.key
                except KeyError:
                    continue
        else:
            return None

    def get_properties(self) -> List[Property]:
        return self._properties

    def get_first_property(self) -> Property:
        if self._properties:
            return self._properties[0]
        else:
            return None

    def get_first_relationship(self):
        if self._relationships:
            return self._relationships[0]
        else:
            return None

    def set_used(self, used: bool):
        self._used = used

    def is_used(self) -> bool:
        return self._used

    def __str__(self) -> str:
        return f'Node #{self._id} = {{' \
               f'label: {self._label}, ' \
               f'first_property: {self.get_first_property()}, ' \
               f'first_relationship: {self.get_first_relationship()}, ' \
               f'used: {self._used}' \
               f'}}'
