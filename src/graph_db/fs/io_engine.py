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

    # Node

    def insert_node(self, node: Node):
        """
        Updates node record in node storage.
        :param node:    node object
        """
        return self._insert_node(node, update=False)

    def update_node(self, node: Node):
        """
        Updates node record in node storage.
        :param node:    node object
        """
        return self._insert_node(node, update=True)

    def _insert_node(self, node: Node, update: bool = False):
        """
        Prepares node record and selects appropriate node storage.
        :param node:    node object
        """
        node_record = RecordEncoder.encode_node(node)
        self.dbfs_manager.write_record(node_record, 'NodeStorage', update=update)

        return node

    def select_node(self, node_id: int) -> Dict[str, DB_TYPE]:
        """
        Selects node with `id` from the appropriate storage.
        Collects all data from other stores.
        :return:
        """
        try:
            node_record = self.dbfs_manager.read_record(node_id, 'NodeStorage')
            return RecordDecoder.decode_node_record(node_record)
        except AssertionError as e:
            print(f'Error at Worker #0: {e}')
            # should be rethrown
            return dict()

    # Relationship

    def insert_relationship(self, rel: Relationship):
        """
        Updates relationship record in node storage.
        :param rel:     relationship object
        """
        return self._insert_relationship(rel, update=False)

    def update_relationship(self, rel: Relationship):
        """
        Updates relationship record in node storage.
        :param rel:     relationship object
        """
        return self._insert_relationship(rel, update=True)

    def _insert_relationship(self, rel: Relationship, update: bool = False):
        """
        Prepares relationship records and select appropriate relationship storage.
        :param rel:    node object
        """
        relationship_record = RecordEncoder.encode_relationship(rel)
        self.dbfs_manager.write_record(relationship_record, 'RelationshipStorage', update=update)

        return rel

    def select_relationship(self, rel_id: int) -> Dict[str, DB_TYPE]:
        """
        Selects relationship with `id` from the appropriate storage.
        Collects all data from other storages.
        :return:
        """
        try:
            relationship_record = self.dbfs_manager.read_record(rel_id, 'RelationshipStorage')
            return RecordDecoder.decode_relationship_record(relationship_record)
        except AssertionError as e:
            print(f'Error at Worker #0: {e}')
            # should be rethrown
            return dict()

    # Label

    def insert_label(self, label: Label):
        """
        Updates label record in node storage.
        :param label:   label object
        """
        return self._insert_label(label, update=False)

    def update_label(self, label: Label):
        """
        Updates node record in node storage.
        :param label:   label object
        """
        return self._insert_label(label, update=True)

    def _insert_label(self, label: Label, update: bool = False):
        """
        Prepares label record and select appropriate label storage.
        :param label: label object
        """
        first_dynamic_id = self.get_stats()['DynamicStorage']

        dynamic_records = RecordEncoder.encode_dynamic_data(label.get_name(), first_dynamic_id)

        for record in dynamic_records:
            self.dbfs_manager.write_record(record, 'DynamicStorage')

        label_record = RecordEncoder.encode_label(label, first_dynamic_id)
        self.dbfs_manager.write_record(label_record, 'LabelStorage', update=update)

        return label

    def select_label(self, label_id: int) -> Dict[str, DB_TYPE]:
        """
        Selects label with `id` from the appropriate storage.
        Collects all data from other stores.
        :param label_id:
        :return:
        """
        label_record = self.dbfs_manager.read_record(label_id, 'LabelStorage')
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
                    label_data['label_name'] = label_name
                    return label_data

    # Property

    def insert_property(self, prop: Property):
        """
        Updates property record in node storage.
        :param prop:    prop object
        """
        return self._insert_property(prop, update=False)

    def update_property(self, prop: Property):
        """
        Updates property record in node storage.
        :param prop:    prop object
        """
        return self._insert_property(prop, update=True)

    def _insert_property(self, prop: Property, update: bool = False):
        """
        Prepares property record and select appropriate property storage.
        :param prop:
        :return:
        """
        if prop.get_id() == INVALID_ID:
            prop.set_id(self.get_stats()['PropertyStorage'])

        # encode key
        key_dynamic_id = self.get_stats()['DynamicStorage']
        key_dynamic_records = RecordEncoder.encode_dynamic_data(prop.get_key(), key_dynamic_id)
        for record in key_dynamic_records:
            self.dbfs_manager.write_record(record, 'DynamicStorage')

        # encode value
        value_dynamic_id = self.get_stats()['DynamicStorage']
        value_dynamic_records = RecordEncoder.encode_dynamic_data(prop.get_value(), value_dynamic_id)
        for record in value_dynamic_records:
            self.dbfs_manager.write_record(record, 'DynamicStorage')

        next_prop_id = prop.get_next_property().get_id() if prop.get_next_property() else INVALID_ID

        property_record = RecordEncoder.encode_property(used=prop.is_used(),
                                                        key_id=key_dynamic_id,
                                                        value_id=value_dynamic_id,
                                                        next_prop_id=next_prop_id)
        self.dbfs_manager.write_record(property_record, 'PropertyStorage', update=update)

        return prop

    def select_property(self, prop_id: int) -> Dict[str, DB_TYPE]:
        """
        Selects property with `id` from the appropriate storage.
        Collects all data from other stores.
        :param prop_id:
        :return:
        """
        property_record = self.dbfs_manager.read_record(prop_id, 'PropertyStorage')
        property_data = RecordDecoder.decode_property_record(property_record)

        # Collect key: string now only
        key = ''
        key_id = property_data['key_id']
        while True:
            # read from dynamic storage until all data is collected
            dynamic_record = self.dbfs_manager.read_record(key_id, 'DynamicStorage')
            dynamic_data = RecordDecoder.decode_dynamic_data_record(dynamic_record)

            key += dynamic_data['data']
            key_id = dynamic_data['next_chunk_id']

            if key_id == INVALID_ID:
                break

        # Collect value: string now only
        value = ''
        value_id = property_data['value_id']
        while True:
            # read from dynamic storage until all data is collected
            dynamic_record = self.dbfs_manager.read_record(value_id, 'DynamicStorage')
            dynamic_data = RecordDecoder.decode_dynamic_data_record(dynamic_record)

            value += dynamic_data['data']
            value_id = dynamic_data['next_chunk_id']

            if value_id == INVALID_ID:
                break

        property_data['key'] = key
        property_data['value'] = value
        return property_data
