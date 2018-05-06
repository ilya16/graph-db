from typing import Dict

from graph_db.engine.types import *
from graph_db.fs.error import RecordNotFoundError
from graph_db.fs.record import Record
#from .worker import Worker


from threading import Thread
from time import sleep

import rpyc
from rpyc.utils.server import ThreadedServer
from .conf import DEFAULT_MANAGER_PORTS, DEFAULT_WORKER_PORTS, base_path
import logging
import os


# TODO: distribution of data across different workers based on ids
# TODO: connections with remote machines

class ManagerService(rpyc.SlaveService):
    class exposed_Manager(object):
        """
        Graph Database File System manager.
        Manages connections with local and remote database stores.
        Manages distribution of data across several stores.
        Processes and directs write and read requests through appropriate storage.
        Acts as an abstraction above distributed file system.
        """

        # Map file name to block_id
        file_table = {} # {'file_name': [block_id1, block_id2, block_id3]}

        # Map block_id to where it's saved
        block_mapping = {}  # {'block_id': [worker_id1, worker_id2, worker_id3]}

        # Map mid to what's saved on it
        worker_content = {}  # {'worker_id': [block_id1, block_id2, block_id3]}

        # Register the information of every minion
        workers = {}  # {'worker_id': (host, port)}

        workers_conn_pool = {}  # {'worker_id': connection}

        manager_list = tuple()

        stores = {}

        stats = {}

        def exposed_add_worker(self, host, port):
            if not self.workers:
                worker_id = 0
            else:
                worker_id = max(self.workers) + 1
            self.workers[worker_id] = (host, port)
            self.workers_conn_pool[worker_id] = rpyc.classic.connect(host, port)

        def exposed_flush_workers(self):
            for worker in self.workers_conn_pool.values():
                worker.root.Worker().flush()

        def exposed_update_stats(self) -> Dict[str, int]:
            """
            Collects a sum of total number of records in each connected storage.
            :return:        dictionary with stats
            """
            self.stats = dict()
            for worker in self.workers.values():
                host, port = worker
                conn = rpyc.classic.connect(host, port)
                worker_stats = conn.root.Worker().get_stats()
                for storage_type in worker_stats:
                    if storage_type not in self.stats:
                        self.stats[storage_type] = 0
                    self.stats[storage_type] += worker_stats[storage_type]

            return self.stats

        def exposed_get_stats(self) -> Dict[str, int]:
            """
            Returns total number of records in each connected storage.
            :return:        dictionary with stats
            """
            return self.stats

        def exposed_write_record(self, record: Record, storage_type: str, update: bool = False):
            """
            Prepares records and select appropriate storage.
            :param record:          record object
            :param storage_type:    type of storage
            :param update:          is it an update of previous record or not
            """
            # Reassign record_id for a worker
            worker = self.workers_conn_pool[0].root.Worker()

            if not update:
                # TODO: in dfs should be mapped
                record.set_index(self.stats[storage_type])
            else:
                pass

            #for worker in self.workers_conn_pool.values():
            worker.write_record(record, storage_type, update=update)

            # if ok:
            if record.idx == self.stats[storage_type]:
                self.stats[storage_type] += 1

        def exposed_read_record(self, record_id: int, storage_type: str):
            """
            Selects record with `id` from the appropriate storage.
            :param record_id:       record id
            :param storage_type     storage type
            :return:
            """

            #worker = self.workers[0]    # one local worker with storage
            worker = self.workers_conn_pool[0].root.Worker()

            try:
                record = worker.read_record(record_id, storage_type)
            except AssertionError as e:
                print(f'Error at Worker #0: {e}')
                raise RecordNotFoundError(f'Record with id:{record_id} was not found')

            return record



def startManagerService(worker_ports=DEFAULT_WORKER_PORTS,
                        manager_port=DEFAULT_MANAGER_PORTS[0]):

    manager = ManagerService.exposed_Manager
    # manager.block_size = block_size
    # manager.replication_factor = replication_factor

    t = ThreadedServer(ManagerService, port=manager_port)
    t.start()

