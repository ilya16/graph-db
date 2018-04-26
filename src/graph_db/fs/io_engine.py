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
    Graph Database IO Engine.
    Processes graph database queries on IO level.
    """
    def __init__(self, base_path: str = MEMORY):
        self.dbfs_manager = DBFSManager(base_path)

    def add_worker(self, worker: Worker):
        self.dbfs_manager.add_worker(worker)

    def close(self):
        self.dbfs_manager.close()

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
        node_id = self.get_stats()['NodeStorage']

        if node.get_label().get_id() == -1:
            label = self.insert_label(node.get_label())
            node.set_label(label)  # Update label

        if node.get_first_relationship():
            pass

        # TODO: Write properties of node

        node.set_id(node_id)
        node_record = RecordEncoder.encode_node(node)
        self.dbfs_manager.write_record(node_record, 'NodeStorage')

        return node

    def select_node(self, node_id: int):
        """
        Selects node with `id` from the appropriate storage.
        Collects all data from other storages.
        :return:
        """

        try:
            node_record = self.dbfs_manager.read_record(node_id, 'NodeStorage')
            node_data = RecordDecoder.decode_node_record(node_record)
        except AssertionError as e:
            print(f'Error at Worker #0: {e}')
            # should be rethrown
            node_data = None

        node = Node(id=node_data['id'], label=Label('temp'))
        if node_data:
            # collecting data from other storages and building node

            label_record = self.dbfs_manager.read_record(node_data['label_id'], 'LabelStorage')
            label_data = RecordDecoder.decode_label_record(label_record)

            if label_data:
                label_name = ''
                dynamic_id = label_data['dynamic_id']

                while True:
                    # read from dynamic storage until all data is collected
                    dynamic_record = self.dbfs_manager.read_record(dynamic_id, 'DynamicStorage')
                    dynamic_data = RecordDecoder.decode_dynamic_data_record(dynamic_record)

                    label_name += dynamic_data['data']
                    dynamic_id = dynamic_data['next_chunk_id']

                    if dynamic_id == INVALID_ID:
                        node.set_label(Label(label_name, node_data['label_id']))
                        break

        # TODO: implement for property and relationships

        # finally return node with all data
        return node

    def update_node(self, node: Node):
        """
        Updates node record in node storage.
        :param node:    node object
        """
        if node.get_label().get_id() == -1:
            self.insert_label(node.get_label())

        if node.get_first_relationship():
            pass

        # TODO: Check Property

        node_record = RecordEncoder.encode_node(node)
        self.dbfs_manager.write_record(node_record, 'NodeStorage')

    def insert_relationship(self, rel: Relationship):
        """
        Prepares relationship records and select appropriate relationship storage.
        :param rel:    node object
        """
        if rel.get_id() == -1:
            self.insert_label(rel.get_label())

        # Update start node if this relation is first for start node
        if rel.get_start_node().get_first_relationship() is None:
            rel.get_start_node().add_relationship(rel)
            start_node_record = RecordEncoder.encode_node(rel.get_start_node())
            self.dbfs_manager.write_record(start_node_record, 'NodeStorage')

        # Update end node if this relation is first for end node
        if rel.get_end_node().get_first_relationship() is None:
            rel.get_end_node().add_relationship(rel)
            end_node_record = RecordEncoder.encode_node(rel.get_end_node())
            self.dbfs_manager.write_record(end_node_record, 'NodeStorage')

        # TODO: 1) Write properties of relationship

        relationship_record = RecordEncoder.encode_relationship(rel)
        self.dbfs_manager.write_record(relationship_record, 'RelationshipStorage')

    def select_relationship(self, rel_id: int) -> Relationship:
        """
                Selects relationship with `id` from the appropriate storage.
                Collects all data from other storages.
                :return:
        """
        try:
            relationship_record = self.dbfs_manager.read_record(rel_id, 'RelationshipStorage')
            relationship_data = RecordDecoder.decode_relationship_record(relationship_record)
        except AssertionError as e:
            print(f'Error at Worker #0: {e}')
            # should be rethrown
            relationship_data = None

        relationship = Relationship(id=relationship_data['id'], label=Label('temp'), start_node=None, end_node=None)
        if relationship_data:
            # collecting data from other storages and building node

            label_record = self.dbfs_manager.read_record(relationship_data['label_id'], 'LabelStorage')
            label_data = RecordDecoder.decode_label_record(label_record)

            if label_data:
                label_name = ''

                while True:
                    # read from dynamic storage until all data is collected
                    dynamic_record = self.dbfs_manager.read_record(label_data['dynamic_id'], 'DynamicStorage')
                    dynamic_data = RecordDecoder.decode_dynamic_data_record(dynamic_record)

                    label_name += dynamic_data['data']

                    if dynamic_data['next_chunk_id'] == INVALID_ID:
                        relationship.set_label(Label(label_name, relationship_data['label_id']))
                        break

        # TODO: implement for property and relationships

        # finally return node with all data
        return relationship

    def update_relationship(self, rel: Relationship):
        pass

    def insert_label(self, label: Label):
        """
        Prepares label record and select appropriate label storage.
        :param label: label object
        """
        first_dynamic_id = self.get_stats()['DynamicStorage']
        label_id = self.get_stats()['LabelStorage']

        dynamic_records = RecordEncoder.encode_dynamic_data(label.get_name(), first_dynamic_id)

        for record in dynamic_records:
            self.dbfs_manager.write_record(record, 'DynamicStorage')

        label.set_id(label_id)
        label_record = RecordEncoder.encode_label(label, first_dynamic_id)
        self.dbfs_manager.write_record(label_record, 'LabelStorage')

        return label

    def select_label(self, label_id: int) -> Label:
        pass

    def update_label(self, label: Label):
        pass

    # TODO: Finish implementation of property-related methods

    def insert_property(self, prop: Property):
        pass

    def select_property(self, prop_id: int) -> Property:
        pass

    def update_property(self, prop: Property):
        pass
