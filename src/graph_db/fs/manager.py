from typing import Dict

from graph_db.engine.types import *
from graph_db.fs.record import Record
#from .worker import Worker


from threading import Thread
from time import sleep

import rpyc
from rpyc.utils.server import ThreadedServer
from .conf import DEFAULT_MANAGER_PORTS, DEFAULT_WORKER_PORTS, base_path, LOG_DIR
import logging
import os


# TODO: distribution of data across different workers based on ids
# TODO: connections with remote machines

class ManagerService(rpyc.Service):
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
            if not self.__class__.workers:
                worker_id = 0
            else:
                worker_id = max(self.__class__.workers) + 1
            self.__class__.workers[worker_id] = (host, port)
            self.__class__.workers_conn_pool[worker_id] = rpyc.connect(host, port)

            # self.workers.append(worker)
            # for storage_type in worker.stores:
            #     if storage_type in self.stores:
            #         self.stores[storage_type].append(worker.stores[storage_type])
            #     else:
            #         self.stores[storage_type] = [worker.stores[storage_type]]
            #
            # self.update_stats()

        def exposed_update_stats(self) -> Dict[str, int]:
            """
            Collects a sum of total number of records in each connected storage.
            :return:        dictionary with stats
            """
            self.__class__.stats = dict()
            for worker in self.__class__.workers.values():
                host, port = worker
                conn = rpyc.connect(host, port)
                worker_stats = conn.root.Worker().get_stats()
                for storage_type in worker_stats:
                    if storage_type not in self.__class__.stats:
                        self.__class__.stats[storage_type] = 0
                    self.__class__.stats[storage_type] += worker_stats[storage_type]

            return self.__class__.stats

        def exposed_get_stats(self) -> Dict[str, int]:
            """
            Returns total number of records in each connected storage.
            :return:        dictionary with stats
            """
            return self.__class__.stats

        def exposed_write_record(self, record: Record, storage_type: str, update: bool = False):
            """
            Prepares records and select appropriate storage.
            :param record:          record object
            :param storage_type:    type of storage
            :param update:          is it an update of previous record or not
            """
            #worker = self.workers[0]     # one local worker with storage
            worker = self.__class__.workers_conn_pool[0].root.Worker()
            # Reassign record_id for a worker
            if not update:
                # TODO: in dfs should be mapped
                record.set_index(self.__class__.stats[storage_type])
            else:
                pass

            worker.write_record(record, storage_type, update=update)

            # if ok:
            if record.idx == self.__class__.stats[storage_type]:
                self.__class__.stats[storage_type] += 1

        def exposed_read_record(self, record_id: int, storage_type: str):
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
                # should be rethrown
                record = None

            return record

        def close(self):
            for worker in self.workers:
                worker.close()


def startManagerService(worker_ports=DEFAULT_WORKER_PORTS,
                        manager_port=DEFAULT_MANAGER_PORTS[0]):
    logging.basicConfig(filename=os.path.join(LOG_DIR, 'manager'),
                        format='%(asctime)s--%(levelname)s:%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    manager = ManagerService.exposed_Manager
    # manager.block_size = block_size
    # manager.replication_factor = replication_factor

    logging.info('Current Config:')
    logging.info('Minions: %s', str(manager.workers))

    t = ThreadedServer(ManagerService, port=manager_port)
    t.start()
