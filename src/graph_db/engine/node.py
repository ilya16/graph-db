from .label import Label


class Node:
    """ Define a single node """
    properties = []
    relationships = []

    def __init__(self, id: int = 0, label: Label = None, used: bool = True):
        self.id = id
        self.label = label
        self.used = used

    def get_id(self) -> int:
        return self.id

    def set_id(self, id: int):
        self.id = id

    def get_label(self) -> Label:
        return self.label

    def set_label(self, label: Label):
        self.label = label

    def add_property(self, prop):
        self.properties.append(prop)

    def get_property_value(self, key: object) -> object:
        if any(key in d.key for d in self.properties):
            for prop in self.properties:
                try:
                    return prop.key
                except KeyError:
                    continue
        else:
            return None

    def get_properties(self) -> list:
        return self.properties

    def get_first_property(self):
        if self.properties:
            return self.properties[0]
        else:
            return None

    def get_first_relationship(self):
        if self.relationships:
            return self.relationships[0]
        else:
            return None

    def is_used(self) -> bool:
        return self.used

    def set_used(self, used: bool):
        self.used = used

    def __str__(self) -> str:
        return f'Node #{self.id} = {{' \
               f'label: {self.label}, ' \
               f'first_property: {self.get_first_property()}, ' \
               f'first_relationship: {self.get_first_relationship()}, ' \
               f'used: {self.used}' \
               f'}}'
