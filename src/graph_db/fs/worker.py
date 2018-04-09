from graph_db.engine.types import *
from .graph_storage import NodeStorage, RelationshipStorage
from .record import Record
from .record_storage import RecordStorage


class WorkerConfig:
    """
    Worker machine File System configuration.
    """

    def __init__(self):
        storage_paths = dict()
        storage_paths[NodeStorage.__class__] = NODE_STORAGE
        storage_paths[RelationshipStorage.__class__] = RELATIONSHIP_STORAGE
        # config[PropertyStorage.__class__] = False
        # config[PropertyStorage.__class__] = False
        # config[PropertyStorage.__class__] = False
        self.storage_paths = storage_paths

    def parse_json(self):
        """
        Parsing configuration file
        :return:
        """
        pass


class WorkerFSManager:
    """
    Worker Machine File System manager.
    Manages distribution of a portion of database across several stores.
    """

    def __init__(self, base_path: str = MEMORY, config: WorkerConfig = WorkerConfig()):
        self.stores = dict()

        # self._init_from_config(config)
        self.stores[NodeStorage.__qualname__] = NodeStorage(path=base_path + NODE_STORAGE)
        self.stores[RelationshipStorage.__qualname__] = RelationshipStorage(path=base_path + RELATIONSHIP_STORAGE)
        # self.stores[PropertyStorage.__qualname__] = RelationshipStorage(path=base_path + PROPERTY_STORAGE)

        self.stats = dict()
        self.update_stats()

    # def _init_from_config(self, config: WorkerConfig):
    #     for storage_type in config.storage_paths:
    #         self.stores[storage_type.__qualname__] = storage_type.__init__(config.storage_paths[storage_type])

    def update_stats(self) -> {RecordStorage: int}:
        """
        Updates total number of records in each connected storage.
        :return:        dictionary with stats
        """
        self.stats = dict()
        for storage_type in self.stores:
            self.stats[storage_type] = self.stores[storage_type].count_records()
        return self.stats

    def get_stats(self) -> {RecordStorage: int}:
        """
        Returns total number of records in each connected storage.
        :return:        dictionary with stats
        """
        return self.stats

    def write_node(self, node_record: Record):
        """
        Writes new node data to node storage.
        :param node_record:    node record object
        """
        node_storage = self.stores[NodeStorage.__qualname__]
        node_storage.allocate_record()

        node_record.idx -= node_storage.offset
        node_storage.write_record(node_record)

        self.stats[NodeStorage.__qualname__] += 1

    def read_node(self, node_id: int):
        """
        Reads node record with `node_id` from node storage.
        :param node_id:     node id
        """
        node_storage = self.stores[NodeStorage.__qualname__]

        try:
            node_record = node_storage.read_record(node_id - node_storage.offset)
            node_record.idx += node_storage.offset
        except AssertionError as e:
            print(f'Error: {e}')
            raise e

        return node_record
