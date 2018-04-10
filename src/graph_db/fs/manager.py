from typing import Dict

from graph_db.engine.label import Label
from graph_db.engine.node import Node
from graph_db.engine.property import Property
from graph_db.engine.relationship import Relationship
from graph_db.engine.types import *
from graph_db.fs.record import Record
from .decoder import RecordDecoder
from .encoder import RecordEncoder
from .graph_storage import NodeStorage, RelationshipStorage
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

    def write_node_record(self, node_record: Record):
        """
        Prepares node records and select appropriate node storage.
        :param node_record:    node record object
        """
        # TODO: insert label, properties first
        worker = self.workers[0]     # one local worker with storage

        node_record.set_index(self.stats[NodeStorage.__qualname__])
        worker.write_node_record(node_record)

        # if ok:
        self.stats[NodeStorage.__qualname__] += 1

    def read_node_record(self, node_id: int):
        """
        Selects node with `id` from the appropriate storage.
        :return:
        """
        worker = self.workers[0]    # one local worker with storage

        try:
            node_record = worker.read_node_record(node_id)
        except AssertionError as e:
            print(f'Error at Worker #0: {e}')
            # should be rethrown
            node_record = None

        return node_record

    # TODO: rename & implement

    def read_label_record(self, label_id):
        pass

    def update_node_record(self, node: Node):
        pass

    def insert_relationship(self, rel: Relationship):
        pass

    def select_relationship(self, rel_id: int) -> Relationship:
        pass

    def update_relationship(self, rel: Relationship):
        pass

    def insert_property(self, prop: Property):
        pass

    def select_property(self, prop_id: int) -> Property:
        pass

    def update_property(self, prop: Property):
        pass