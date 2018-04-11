from graph_db.engine.types import DB_TYPE


class Property:
    """ Property of node or relationship. """

    def __init__(self, key: DB_TYPE, value: DB_TYPE, id: int = 0):
        self._id = id
        self._key = key
        self._value = value

    def set_id(self, id: int):
        self._id = id

    def get_id(self) -> int:
        return self._id

    def set_key(self, key: DB_TYPE):
        self._key = key

    def get_key(self) -> DB_TYPE:
        return self._key

    def set_value(self, value: DB_TYPE):
        self._value = value

    def get_value(self) -> DB_TYPE:
        return self._value
