from typing import Dict

from graph_db.engine.types import *
from .graph_storage import NodeStorage, RelationshipStorage, PropertyStorage, LabelStorage, DynamicStorage
from .record import Record


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


class Worker:
    """
    Worker Machine File System manager.
    Manages distribution of a portion of database across several stores.
    """

    def __init__(self, base_path: str = MEMORY, config: WorkerConfig = WorkerConfig()):
        self.stores = dict()

        # self._init_from_config(config)
        self.stores['NodeStorage'] = NodeStorage(path=base_path + NODE_STORAGE)
        self.stores['RelationshipStorage'] = RelationshipStorage(path=base_path + RELATIONSHIP_STORAGE)
        self.stores['LabelStorage'] = LabelStorage(path=base_path + LABEL_STORAGE)
        self.stores['PropertyStorage'] = PropertyStorage(path=base_path + PROPERTY_STORAGE)
        self.stores['DynamicStorage'] = DynamicStorage(path=base_path + DYNAMIC_STORAGE)

        self.stats = dict()
        self.update_stats()

    # def _init_from_config(self, config: WorkerConfig):
    #     for storage_type in config.storage_paths:
    #         self.stores[storage_type.__qualname__] = storage_type.__init__(config.storage_paths[storage_type])

    def update_stats(self) -> Dict[str, int]:
        """
        Updates total number of records in each connected storage.
        :return:        dictionary with stats
        """
        self.stats = dict()
        for storage_type in self.stores:
            self.stats[storage_type] = self.stores[storage_type].count_records()
        return self.stats

    def get_stats(self) -> Dict[str, int]:
        """
        Returns total number of records in each connected storage.
        :return:        dictionary with stats
        """
        return self.stats

    def write_record(self, record: Record, storage_type: str):
        """
        Writes record data to specified storage.
        :param record:          record object
        :param storage_type:    storage type
        """
        storage = self.stores[storage_type]
        storage.allocate_record()

        # node_record.idx -= node_storage.offset
        record.set_index(self.stats[storage_type])
        storage.write_record(record)

        # if ok:
        self.stats[storage_type] += 1

    def read_record(self, record_id: int, storage_type: str):
        """
        Reads record with `record_id` from specified storage.
        :param record_id:       record id
        :param storage_type     storage type
        """
        storage = self.stores[storage_type]

        try:
            record = storage.read_record(record_id)
        except AssertionError as e:
            print(f'Error: {e}')
            raise e

        return record
