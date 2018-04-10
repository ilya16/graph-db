from typing import Dict

from graph_db.engine.label import Label
from graph_db.engine.node import Node
from graph_db.engine.property import Property
from graph_db.engine.relationship import Relationship
from graph_db.engine.types import *
from .manager import DBFSManager
from .decoder import RecordDecoder
from .encoder import RecordEncoder
from .worker import Worker


# TODO: distribution of data across different workers based on ids
# TODO: connections with remote machines


class IOEngine:
    """
    Graph Database File System manager.
    Manages connections with local and remote database stores.
    Manages distribution of data across several stores.
    Processes and directs write and read requests through appropriate storage.
    Acts as an abstraction above distributed file system.
    """
    def __init__(self, base_path: str = MEMORY):
        self.dbfs_manager = DBFSManager(base_path)

    def add_worker(self, worker: Worker):
        self.dbfs_manager.add_worker(worker)

    def get_stats(self) -> Dict[str, int]:
        """
        Returns total number of records in each type of storage.
        :return:        dictionary with stats
        """
        return self.dbfs_manager.get_stats()

    def insert_node(self, node: Node):
        """
        Prepares node records and select appropriate node storage.
        :param node:    node object
        """
        node_record = RecordEncoder.encode_node(node)

        self.dbfs_manager.write_node_record(node_record)

    def select_node(self, node_id: int):
        """
        Selects node with `id` from the appropriate storage.
        Collects all data from other storages.
        :return:
        """

        try:
            node_record = self.dbfs_manager.read_node_record(node_id)
            node_data = RecordDecoder.decode_node_record(node_record)
        except AssertionError as e:
            print(f'Error at Worker #0: {e}')
            # should be rethrown
            node_data = None

        node = Node(id=node_data['id'])
        if node_data:
            # collecting data from other storages and building node

            label_record = self.dbfs_manager.read_label_record(node_data['label_id'])
            label_data = RecordDecoder.decode_label_record(label_record)

            if label_data:
                label_name = ''
                while True:
                    # read from dynamic storage until all data is collected
                    dynamic_record = self.dbfs_manager.read_dynamic_record(label_data['dynamic_id'])
                    dynamic_data = RecordDecoder.decode_dynamic_data_record(dynamic_record)

                    label_name += dynamic_data['data']

                    if dynamic_record['next_chunk_id'] == 0:
                        return

             # do the same for properties and relationships...

        # finally return node with all data
        return node

    # TODO: implement

    def update_node(self, node: Node):
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
