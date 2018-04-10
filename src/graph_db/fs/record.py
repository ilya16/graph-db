class Record(bytearray):
    """
    Unit of physical stored information in the database
    """

    def __init__(self, data: bytes, index: int = 0):
        """
        Creates new record object from initial bytes of data.
        :param data:    initial bytes.
        :param index:   physical index of record in the storage.
        """
        super().__init__(data)

        self.idx = index
        self.size = len(data)

    @classmethod
    def empty(cls, size: int, index: int = 0) -> 'Record':
        """
        Creates empty record filled with zeroes.
        :param size:    size of empty record.
        :param index:   physical index of record in the storage.
        :return:        new record object.
        """
        return Record(b'\0' * size, index)

    def override(self, offset: int, data: bytes) -> None:
        """
        Overrides subset of bytes of record data.
        :param offset:  starting position of new data
        :param data:    data to be written
        :return:        None
        """
        assert offset + len(data) <= self.size

        self[offset:offset + len(data)] = data

    def set_index(self, index: int):
        self.idx = index
