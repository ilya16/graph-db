from typing import List, Union

from .property import Property
from graph_db.engine.types import DB_TYPE, INVALID_ID
from .label import Label


class Node:
    """ Node in a Graph. """

    def __init__(self,
                 label: Label = None,
                 id: int = INVALID_ID,
                 first_prop: Property = None,
                 first_rel=None,
                 used: bool = True):
        self._properties = []
        self._relationships = []
        self._id = id
        self._label = label
        self._first_prop = first_prop
        self._first_rel = first_rel
        self._used = used

    def set_id(self, id: int):
        self._id = id

    def get_id(self) -> int:
        return self._id

    def get_label(self) -> Label:
        return self._label

    def set_label(self, label: Label):
        self._label = label

    def get_first_property(self) -> Union[Property, None]:
        return self._first_prop

    def set_first_property(self, prop: Property):
        self._first_prop = prop

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

    def get_relationships(self):
        return self._relationships

    def get_first_relationship(self):
        return self._first_rel

    def set_first_relationship(self, rel):
        self._first_rel = rel

    def add_relationship(self, rel):
        return self._relationships.append(rel)

    def set_used(self, used: bool):
        self._used = used

    def is_used(self) -> bool:
        return self._used

    def __str__(self) -> str:
        prop = self.get_first_property()
        if prop is None:
            return f'Node #{self._id} = {{' \
                   f'label: {self._label.get_name()}, ' \
                   f'first_property: {None}, ' \
                   f'used: {self._used}' \
                   f'}}'
        else:
            return f'Node #{self._id} = {{' \
                   f'label: {self._label.get_name()}, ' \
                   f'first_property: {prop.get_key()}:{prop.get_value()}, ' \
                   f'used: {self._used}' \
                   f'}}'
