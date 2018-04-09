class Property:
    """
    Property of node or relationship
    """

    def __init__(self, key: object, value: object, id: int = 0):
        self.id = id
        self.key = key
        self.value = value

    # TODO: class methods
