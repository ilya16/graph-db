from typing import List, Union

from .property import Property
from graph_db.engine.types import DB_TYPE, INVALID_ID
from .label import Label


class Node:
    """ Node in a Graph. """

    def __init__(self,
                 label: Label,
                 id: int = INVALID_ID,
                 used: bool = True):
        self._properties = []
        self._relationships = []
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

    def get_first_property(self) -> Union[Property, None]:
        if self._properties:
            return self._properties[0]
        else:
            return None

    def get_first_relationship(self):
        if self._relationships:
            return self._relationships[0]
        else:
            return None

    def add_relationship(self, rel):
        return self._relationships.append(rel)

    def set_used(self, used: bool):
        self._used = used

    def is_used(self) -> bool:
        return self._used

    def __str__(self) -> str:
        return f'Node #{self._id} = {{' \
               f'label: {self._label.get_name()}, ' \
               f'first_property: {self.get_first_property()}, ' \
               f'used: {self._used}' \
               f'}}'
