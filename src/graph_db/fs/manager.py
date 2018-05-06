from typing import Dict

from graph_db.engine.types import *
from graph_db.fs.error import RecordNotFoundError
from graph_db.fs.record import Record
#from .worker import Worker


from threading import Thread
from time import *

from .worker import start_worker_service
from multiprocessing import Process
import rpyc
from rpyc.utils.server import ThreadedServer
from .conf import *
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

        workers = {}                    # {'worker_id': (host, port)}
        worker_pool = {}                # {port : worker_process}
        worker_replicas_pool = {}       # {worker_id : [replica_process, ...]}
        worker_pool_size = 0

        workers_conn_pool = {}          # {worker_id : connection}
        worker_replicas_conn_pool = {}  # {worker_id : [connection, ...]}

        stores = {}                     # {TypeStorage : RecordStorage}
        stats = {}                      # {TypeStorage : record_count}

        def __init__(self):
            self.setup_workers()

        def setup_workers(self):
            for port in DEFAULT_WORKER_PORTS:
                path = base_path + worker_path + str(self.worker_pool_size)+'/'
                self.worker_pool[port] = Process(target=start_worker_service, args=(port, path))
                self.worker_pool[port].start()
                sleep(0.1)
                self.workers_conn_pool[self.worker_pool_size] = rpyc.classic.connect('localhost', port)
                print(f'Worker node #{self.worker_pool_size+1} created at localhost:{port}')
                # If replicate mode is on - create replicas of workers
                self.worker_replicas_conn_pool[self.worker_pool_size] = []
                self.worker_replicas_pool[self.worker_pool_size] = []
                if dfs_mode['Replicate'] is True:
                    for i in range(1, REPLICATE_FACTOR + 1):
                        path = base_path + worker_path +str(self.worker_pool_size)+'/'+replica_path+str(i)+'/'
                        self.worker_replicas_pool[self.worker_pool_size].append(Process(target=start_worker_service, args=(port + i, path)))
                        self.worker_replicas_pool[self.worker_pool_size][i-1].start()
                        sleep(0.1)
                        self.worker_replicas_conn_pool[self.worker_pool_size].append(rpyc.classic.connect('localhost', port+i))

                        print(f'\tWorker replica #{i} has been created at localhost:{port+i}')
                self.worker_pool_size = self.worker_pool_size + 1
                if self.worker_pool_size == 1:
                    break

        def exposed_get_worker_processes(self):
            processes = []
            for p in self.worker_pool.keys():
                processes.append(self.worker_pool[p])
            for p in self.worker_replicas_pool.keys():
                for id in range(len(self.worker_replicas_pool[p])):
                    processes.append(self.worker_replicas_pool[p][id])
            return processes

        def exposed_close_workers(self):
            for p in self.worker_replicas_pool.keys():
                for id in range(len(self.worker_replicas_pool[p])):
                    self.worker_replicas_pool[p][id].terminate()
            for p in self.worker_pool.keys():
                self.worker_pool[p].terminate()


        def exposed_flush_workers(self):
            for worker_id in self.workers_conn_pool.keys():
                self.workers_conn_pool[worker_id].root.Worker().flush()
                for replica in self.worker_replicas_conn_pool[worker_id]:
                    replica.root.Worker().flush()

        def exposed_update_stats(self) -> Dict[str, int]:
            """
            Collects a sum of total number of records in each connected storage.
            :return:        dictionary with stats
            """
            self.stats = dict()
            for worker in self.workers_conn_pool.values():
                worker_stats = worker.root.Worker().get_stats()
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
            # worker = self.workers_conn_pool[0].root.Worker()

            if not update:
                # TODO: in dfs should be mapped
                record.set_index(self.stats[storage_type])
            else:
                pass

            for worker_id in self.workers_conn_pool.keys():
                self.workers_conn_pool[worker_id].root.Worker().write_record(record, storage_type, update=update)
                for replica in self.worker_replicas_conn_pool[worker_id]:
                    replica.root.Worker().write_record(record, storage_type, update=update)

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

            # worker = self.workers[0]    # one local worker with storage
            worker = self.workers_conn_pool[0].root.Worker()

            try:
                record = worker.read_record(record_id, storage_type)
            except AssertionError as e:
                print(f'Error at Worker #0: {e}')
                raise RecordNotFoundError(f'Record with id:{record_id} was not found')

            return record


def start_manager_service(manager_port):

    manager = ManagerService.exposed_Manager
    # manager.block_size = block_size
    # manager.replication_factor = replication_factor

    t = ThreadedServer(ManagerService, port=manager_port)
    t.start()

