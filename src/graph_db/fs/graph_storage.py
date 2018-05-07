from graph_db.engine.types import *
from .record_storage import RecordStorage


class NodeStorage(RecordStorage):
    """
    Physical Node Storage.
    """

    def __init__(self, path: str = NODE_STORAGE):
        super().__init__(NODE_RECORD_SIZE, path)


class RelationshipStorage(RecordStorage):
    """
    Physical Relationship Storage.
    """

    def __init__(self, path: str = RELATIONSHIP_STORAGE):
        super().__init__(RELATIONSHIP_RECORD_SIZE, path)


class PropertyStorage(RecordStorage):
    """
    Physical Property Storage.
    """

    def __init__(self, path: str = PROPERTY_STORAGE):
        super().__init__(PROPERTY_RECORD_SIZE, path)


class LabelStorage(RecordStorage):
    """
    Physical Label Storage.
    """

    def __init__(self, path: str = LABEL_STORAGE):
        super().__init__(LABEL_RECORD_SIZE, path)


class DynamicStorage(RecordStorage):
    """
    Physical Dynamic Storage.
    """

    def __init__(self, path: str = DYNAMIC_STORAGE):
        super().__init__(DYNAMIC_RECORD_SIZE, path)