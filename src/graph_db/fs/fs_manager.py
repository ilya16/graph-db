from graph_db.engine.label import Label
from graph_db.engine.node import Node
from graph_db.engine.types import *
from .decoder import RecordDecoder
from .encoder import RecordEncoder
from .graph_storage import NodeStorage, RelationshipStorage
from .record_storage import RecordStorage
from .worker import WorkerFSManager, WorkerConfig


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

    def add_worker(self, worker: WorkerFSManager):
        self.workers.append(worker)
        for storage_type in worker.stores:
            if storage_type in self.stores:
                self.stores[storage_type].append(worker.stores[storage_type])
            else:
                self.stores[storage_type] = [worker.stores[storage_type]]

        self.update_stats()

    def update_stats(self) -> {RecordStorage: int}:
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

    def get_stats(self) -> {RecordStorage: int}:
        """
        Returns total number of records in each connected storage.
        :return:        dictionary with stats
        """
        return self.stats

    def insert_node(self, node: Node):
        """
        Prepares node records and select appropriate node storage.
        :param node:    node object
        """
        # TODO: insert label, properties first
        worker = self.workers[0]     # one local worker with storage

        node.set_id(self.stats[NodeStorage.__qualname__])
        node_record = RecordEncoder.encode_node(node)

        worker.write_node(node_record)
        self.stats[NodeStorage.__qualname__] += 1

    def select_node(self, node_id: int):
        """
        Selects node with `id` from the appropriate storage.
        :return:
        """
        worker = self.workers[0]    # one local worker with storage

        try:
            node_record = worker.read_node(node_id)
            node = RecordDecoder.decode_node(node_record)
        except AssertionError as e:
            print(f'Error at Worker #0: {e}')
            # should be rethrown
            node = None

        return node
