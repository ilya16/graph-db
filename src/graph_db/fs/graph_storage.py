from graph_db.engine.types import *
from .record_storage import RecordStorage

NODE_RECORD_SIZE = 13
RELATIONSHIP_RECORD_SIZE = 34


class NodeStorage(RecordStorage):
    """
    Physical Node Storage.
    """

    def __init__(self, path: str = NODE_STORAGE):
        super().__init__(NODE_RECORD_SIZE, path)


class RelationshipStorage(RecordStorage):
    """
    Physical Node Storage.
    """

    def __init__(self, path: str = RELATIONSHIP_STORAGE):
        super().__init__(RELATIONSHIP_RECORD_SIZE, path)