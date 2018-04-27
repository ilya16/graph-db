from typing import Dict

from graph_db.engine.types import *
from .graph_storage import NodeStorage, RelationshipStorage, PropertyStorage, LabelStorage, DynamicStorage
from .record import Record


base_config = {
    'NodeStorage': True,
    'RelationshipStorage': True,
    'LabelStorage': True,
    'PropertyStorage': True,
    'DynamicStorage': True
}


class Worker:
    """
    Worker Machine File System manager.
    Manages distribution of a portion of database across several stores.
    """

    def __init__(self, base_path: str = MEMORY, config: Dict[str, bool] = base_config):
        self.stores = dict()

        if config['NodeStorage']:
            self.stores['NodeStorage'] = NodeStorage(path=base_path + NODE_STORAGE)

        if config['RelationshipStorage']:
            self.stores['RelationshipStorage'] = RelationshipStorage(path=base_path + RELATIONSHIP_STORAGE)

        if config['LabelStorage']:
            self.stores['LabelStorage'] = LabelStorage(path=base_path + LABEL_STORAGE)

        if config['PropertyStorage']:
            self.stores['PropertyStorage'] = PropertyStorage(path=base_path + PROPERTY_STORAGE)

        if config['DynamicStorage']:
            self.stores['DynamicStorage'] = DynamicStorage(path=base_path + DYNAMIC_STORAGE)

        self.stats = dict()
        self.update_stats()

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

    def close(self):
        for storage in self.stores:
            self.stores[storage].close()
