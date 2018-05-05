from typing import Dict

from graph_db.engine.types import *
from graph_db.fs.error import RecordNotFoundError
from graph_db.fs.record import Record
from .worker import Worker


# TODO: distribution of data across different workers based on ids
# TODO: connections with remote machines

class DBFSManager:
    """
    Graph Database File System manager.
    Manages connections with local and remote database stores.
    Manages distribution of data across several stores.
    Processes and directs write and read requests through appropriate storage.
    Acts as an abstraction above distributed file system.
    """
    def __init__(self, base_path: str = MEMORY):
        self.workers = []
        self.stores = {}
        self.stats = {}

    def add_worker(self, worker: Worker):
        self.workers.append(worker)
        for storage_type in worker.stores:
            if storage_type in self.stores:
                self.stores[storage_type].append(worker.stores[storage_type])
            else:
                self.stores[storage_type] = [worker.stores[storage_type]]

        self.update_stats()

    def update_stats(self) -> Dict[str, int]:
        """
        Collects a sum of total number of records in each connected storage.
        :return:        dictionary with stats
        """
        self.stats = dict()
        for worker in self.workers:
            worker_stats = worker.get_stats()
            for storage_type in worker_stats:
                if storage_type not in self.stats:
                    self.stats[storage_type] = 0
                self.stats[storage_type] += worker_stats[storage_type]

        return self.stats

    def get_stats(self) -> Dict[str, int]:
        """
        Returns total number of records in each connected storage.
        :return:        dictionary with stats
        """
        return self.stats

    def write_record(self, record: Record, storage_type: str, update: bool = False):
        """
        Prepares records and select appropriate storage.
        :param record:          record object
        :param storage_type:    type of storage
        :param update:          is it an update of previous record or not
        """
        worker = self.workers[0]     # one local worker with storage

        # Reassign record_id for a worker
        if not update:
            # TODO: in dfs should be mapped
            record.set_index(self.stats[storage_type])
        else:
            pass

        worker.write_record(record, storage_type, update=update)

        # if ok:
        if record.idx == self.stats[storage_type]:
            self.stats[storage_type] += 1

    def read_record(self, record_id: int, storage_type: str):
        """
        Selects record with `id` from the appropriate storage.
        :param record_id:       record id
        :param storage_type     storage type
        :return:
        """
        worker = self.workers[0]    # one local worker with storage

        try:
            record = worker.read_record(record_id, storage_type)
        except AssertionError as e:
            print(f'Error at Worker #0: {e}')
            raise RecordNotFoundError(f'Record with id:{record_id} was not found')

        return record

    def close(self):
        for worker in self.workers:
            worker.close()
