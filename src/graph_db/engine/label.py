class Label:
    """
    Label of the node or relationship
    """

    def __init__(self, name: str, id: int = 0, used: bool = True):
        self.id = id
        self.name = name
        self.used = used
