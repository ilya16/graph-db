from typing import Dict

import graph_db.engine.types as types
from graph_db.fs.record import Record

from time import *

from .worker import start_worker_service
from multiprocessing import Process
import rpyc
from rpyc.utils.server import ThreadedServer
import json


class ManagerService(rpyc.SlaveService):
    class exposed_Manager(object):
        """
        Graph Database File System manager.
        Manages connections with local and remote database stores.
        Manages distribution of data across several stores.
        Processes and directs write and read requests through appropriate storage.
        Acts as an abstraction above distributed file system.
        """
        conf = {}
        worker_ports = []

        worker_pool = {}                # {port : worker_process}
        worker_replicas_pool = {}       # {worker_id : [replica_process, ...]}

        workers_conn_pool = {}          # {worker_id : connection}
        worker_replicas_conn_pool = {}  # {worker_id : [connection, ...]}

        stores = {}                     # {TypeStorage : RecordStorage}
        stats = {}                      # {TypeStorage : record_count}
        worker_pool_size = 0

        worker_stats = {}               # {worker_id : worker_stats={StorageType : count}}
        mapper = {}                     # {global_id : {StorageType : {worker_id : local_id}}}

        def __init__(self):
            self.setup_workers()

        def exposed_update_stats(self) -> Dict[str, int]:
            """
            Collects a sum of total number of records in each connected storage.
            :return:        dictionary with stats
            """
            self.stats = dict()
            for worker_id in self.workers_conn_pool.keys():
                w_stats = self.workers_conn_pool[worker_id].root.Worker().get_stats()
                self.worker_stats[worker_id] = w_stats
                for storage_type in w_stats:
                    if storage_type not in self.stats:
                        self.stats[storage_type] = 0
                    self.stats[storage_type] += w_stats[storage_type]

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

            if self.conf['dfs_mode']['Distribute']:
                worker_id = self.simple_balance(storage_type)
            else:
                worker_id = 0

            if not update:
                if self.conf['dfs_mode']['Distribute']:
                    self.mapper[self.stats[storage_type]] = {}
                    self.mapper[self.stats[storage_type]][storage_type] = (worker_id, self.worker_stats[worker_id][storage_type])
                    record.set_index(self.worker_stats[worker_id][storage_type])
                else:
                    record.set_index(self.stats[storage_type])
            else:
                if self.conf['dfs_mode']['Distribute']:
                    record.set_index(self.mapper[self.stats[storage_type]-1][storage_type][1])

            # print(f'Record {record.idx} to {storage_type} by  worker_{worker_id}')
            self.workers_conn_pool[worker_id].root.Worker().write_record(record, storage_type, update=update)
            if self.conf['dfs_mode']['Replicate']:
                for replica in self.worker_replicas_conn_pool[worker_id]:
                    replica.root.Worker().write_record(record, storage_type, update=update)

            # if ok:
            if self.conf['dfs_mode']['Distribute']:
                if record.idx == self.worker_stats[worker_id][storage_type]:
                    self.stats[storage_type] += 1
                    self.worker_stats[worker_id][storage_type] += 1
            else:
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
            #worker = self.workers_conn_pool[0].root.Worker()
            if self.conf['dfs_mode']['Distribute']:
                worker_id, local_record_id = self.mapper[record_id][storage_type]
            else:
                worker_id = 0
                local_record_id = record_id

            worker = self.workers_conn_pool[worker_id].root.Worker()
            record = worker.read_record(local_record_id, storage_type)
            # if record is None:
                # print(f'Record #{record_id} was not found')

            return record

        def simple_balance(self, storage_type):

            record_sum = 0
            for worker_id in self.worker_stats.keys():
                record_sum += self.worker_stats[worker_id][storage_type]

            return record_sum % self.worker_pool_size

        def setup_workers(self):
            """
            Looks in config file for worker ports and replication factor. After that for each worker port it creates
            corresponding process and replicas
            :return:
            """
            for port in self.worker_ports:
                path = self.conf['db_path'] + types.WORKER_PATH + str(self.worker_pool_size) + '/'
                self.worker_pool[port] = Process(target=start_worker_service, args=(port, path, self.conf['workers'][0]['stores']))
                self.worker_pool[port].start()
                sleep(0.1)
                self.workers_conn_pool[self.worker_pool_size] = rpyc.classic.connect('localhost', port)
                print(f'Worker node #{self.worker_pool_size+1} created at localhost:{port}')

                # If replicate mode is on - create replicas of workers
                if self.conf['dfs_mode']['Replicate'] is True:
                    self.worker_replicas_conn_pool[self.worker_pool_size] = []
                    self.worker_replicas_pool[self.worker_pool_size] = []
                    for i in range(1, self.conf['replica_factor'] + 1):
                        path = self.conf['db_path'] + types.WORKER_PATH + str(self.worker_pool_size) + '/' + types.REPLICA_PATH + str(i) + '/'
                        self.worker_replicas_pool[self.worker_pool_size].append(
                            Process(target=start_worker_service, args=(port + i, path, self.conf['workers'][0]['stores'])))
                        self.worker_replicas_pool[self.worker_pool_size][i - 1].start()
                        sleep(0.1)
                        self.worker_replicas_conn_pool[self.worker_pool_size].append(
                            rpyc.classic.connect('localhost', port + i))

                        print(f'\tWorker replica #{i} has been created at localhost:{port+i}')
                self.worker_pool_size = self.worker_pool_size + 1
                #if self.worker_pool_size == 1:
                #    break

        def exposed_get_worker_processes(self):
            processes = []
            for p in self.worker_pool.keys():
                processes.append(self.worker_pool[p])
            for p in self.worker_replicas_pool.keys():
                for id in range(len(self.worker_replicas_pool[p])):
                    processes.append(self.worker_replicas_pool[p][id])
            return processes

        def exposed_close_workers(self):
            if self.conf['dfs_mode']['Replicate'] is True:
                for p in self.worker_replicas_pool.keys():
                    for id in range(len(self.worker_replicas_pool[p])):
                        self.worker_replicas_pool[p][id].terminate()
            for p in self.worker_pool.keys():
                self.worker_pool[p].terminate()

        def exposed_flush_workers(self):
            for worker_id in self.workers_conn_pool.keys():
                self.workers_conn_pool[worker_id].root.Worker().flush()
                if self.conf['dfs_mode']['Replicate'] is True:
                    for replica in self.worker_replicas_conn_pool[worker_id]:
                        replica.root.Worker().flush()

        def parse_config(self, config_path):
            with open(config_path) as f:
                res = json.load(config_path)
            self.manager_address = [(res['manager_config']['ip'], res['manager_config']['port'])]


def start_manager_service(manager_port, config_path):

    manager = ManagerService.exposed_Manager
    try:
        with open(config_path, "r") as f:
            conf = json.load(f)
    except FileNotFoundError:
        with open('../../../' + config_path, "r") as f:
            conf = json.load(f)

    for worker in conf['manager_config']['workers']:
        manager.worker_ports.append(worker['port'])
    manager.conf = conf['manager_config']

    t = ThreadedServer(ManagerService, port=manager_port)
    t.start()

