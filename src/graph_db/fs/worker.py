from typing import Dict

from graph_db.engine.types import *
from .graph_storage import NodeStorage, RelationshipStorage, PropertyStorage, LabelStorage, DynamicStorage
from .record import Record

import rpyc
from rpyc.utils.server import ThreadedServer
from .conf import DEFAULT_MANAGER_PORTS, DEFAULT_WORKER_PORTS, LOG_DIR, base_config, base_path
import logging
import os


class WorkerService(rpyc.Service):
    class exposed_Worker(object):

        """
        Worker Machine File System manager.
        Manages distribution of a portion of database across several stores.
        """
        stores = dict()

        stats = dict()

        def __init__(self):
            self.update_stats()

        def update_stats(self) -> Dict[str, int]:
            """
            Updates total number of records in each connected storage.
            :return:        dictionary with stats
            """
            self.__class__.stats = dict()
            for storage_type in self.stores:
                self.__class__.stats[storage_type] = self.__class__.stores[storage_type].count_records()
            return self.__class__.stats

        def exposed_get_stats(self) -> Dict[str, int]:
            """
            Returns total number of records in each connected storage.
            :return:        dictionary with stats
            """
            return self.__class__.stats

        def exposed_write_record(self, record: Record, storage_type: str, update: bool = False):
            """
            Writes record data to specified storage.
            :param record:          record object
            :param storage_type:    storage type
            :param update:          is it an update of previous record or not
            """
            storage = self.stores[storage_type]

            if not update:
                new_record = storage.allocate_record()
                record.set_index(new_record.idx)

            storage.write_record(record)

            # if ok:
            if record.idx == self.stats[storage_type]:
                self.stats[storage_type] += 1

        def exposed_read_record(self, record_id: int, storage_type: str):
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


def startWorkerService(server_port):
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'minion'),
                        format='%(asctime)s--%(levelname)s:%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)

    worker = WorkerService.exposed_Worker

    if base_config['NodeStorage']:
        worker.stores['NodeStorage'] = NodeStorage(path=base_path + NODE_STORAGE)

    if base_config['RelationshipStorage']:
        worker.stores['RelationshipStorage'] = RelationshipStorage(path=base_path + RELATIONSHIP_STORAGE)

    if base_config['LabelStorage']:
        worker.stores['LabelStorage'] = LabelStorage(path=base_path + LABEL_STORAGE)

    if base_config['PropertyStorage']:
        worker.stores['PropertyStorage'] = PropertyStorage(path=base_path + PROPERTY_STORAGE)

    if base_config['DynamicStorage']:
        worker.stores['DynamicStorage'] = DynamicStorage(path=base_path + DYNAMIC_STORAGE)

    t = ThreadedServer(WorkerService, port=server_port)
    t.start()