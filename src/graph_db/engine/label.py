class Label:
    """ Label of node or relationship. """

    def __init__(self, name: str, id: int = -1, used: bool = True):
        self._id = id
        self._name = name
        self._used = used

    def set_id(self, id: int):
        self._id = id

    def get_id(self) -> int:
        return self._id

    def set_name(self, name: str):
        self._name = name

    def get_name(self) -> str:
        return self._name

    def set_used(self, used: bool):
        self._used = used

    def is_used(self) -> bool:
        return self._used
