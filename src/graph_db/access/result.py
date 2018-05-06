class ResultSet:
    def __init__(self, objects=None, message=None):
        self._objects = objects
        self._message = message

    def set_message(self, message):
        self._message = message

    def get_message(self):
        return self._message

    def __iter__(self):
        for obj in self._objects:
            yield obj

    def __len__(self):
        return len(self._objects)

    def __getitem__(self, item):
        return self._objects[item]

    def __setitem__(self, key, value):
        self._objects[key] = value
