from typing import Dict, Optional

from graph_db.engine.label import Label
from graph_db.engine.node import Node
from graph_db.engine.property import Property
from graph_db.engine.relationship import Relationship
from graph_db.engine.types import *
from graph_db.fs.error import RecordNotFoundError
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
        if node.get_id() == INVALID_ID:
            node.set_id(self.get_stats()['NodeStorage'])

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
        except RecordNotFoundError as e:
            print(e)
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
        if rel.get_id() == INVALID_ID:
            rel.set_id(self.get_stats()['RelationshipStorage'])

        relationship_record = RecordEncoder.encode_relationship(rel)
        self.dbfs_manager.write_record(relationship_record, 'RelationshipStorage', update=update)

        return rel

    def select_relationship(self, rel_id: object) -> object:
        """
        Selects relationship with `id` from the appropriate storage.
        Collects all data from other storages.
        :return:
        """
        try:
            relationship_record = self.dbfs_manager.read_record(rel_id, 'RelationshipStorage')
            return RecordDecoder.decode_relationship_record(relationship_record)
        except RecordNotFoundError as e:
            print(e)
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
        if label.get_id() == INVALID_ID:
            label.set_id(self.get_stats()['LabelStorage'])

        dynamic_id = self.get_stats()['DynamicStorage']
        self._write_dynamic_data(label.get_name(), dynamic_id)

        label_record = RecordEncoder.encode_label(label, dynamic_id)
        self.dbfs_manager.write_record(label_record, 'LabelStorage', update=update)

        return label

    def select_label(self, label_id: int) -> Dict[str, DB_TYPE]:
        """
        Selects label with `id` from the appropriate storage.
        Collects all data from other stores.
        :param label_id:
        :return:
        """
        try:
            label_record = self.dbfs_manager.read_record(label_id, 'LabelStorage')
        except RecordNotFoundError as e:
            print(e)
            return dict()

        label_data = RecordDecoder.decode_label_record(label_record)

        name = self._build_dynamic_data(label_data['dynamic_id'])
        if name:
            label_data['name'] = name
        else:
            label_data = dict()
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

        if update:
            old_property_record = self.dbfs_manager.read_record(prop.get_id(), 'PropertyStorage')
            old_property_data = RecordDecoder.decode_property_record(old_property_record)

            key_dynamic_id = old_property_data['key_id']
            value_dynamic_id = old_property_data['value_id']

            old_key = self._build_dynamic_data(key_dynamic_id)
            old_value = self._build_dynamic_data(value_dynamic_id)

            if old_key != prop.get_key():
                # key has changed
                key_dynamic_id = self.get_stats()['DynamicStorage']
                self._write_dynamic_data(prop.get_key(), key_dynamic_id)
            elif old_value != prop.get_value():
                # value has changed
                value_dynamic_id = self.get_stats()['DynamicStorage']
                self._write_dynamic_data(prop.get_value(), value_dynamic_id)
        else:
            key_dynamic_id = self.get_stats()['DynamicStorage']
            self._write_dynamic_data(prop.get_key(), key_dynamic_id)
            value_dynamic_id = self.get_stats()['DynamicStorage']
            self._write_dynamic_data(prop.get_value(), value_dynamic_id)

        next_prop_id = prop.get_next_property().get_id() if prop.get_next_property() else INVALID_ID

        property_record = RecordEncoder.encode_property(prop_id=prop.get_id(),
                                                        used=prop.is_used(),
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
        try:
            property_record = self.dbfs_manager.read_record(prop_id, 'PropertyStorage')
        except RecordNotFoundError as e:
            print(e)
            return dict()

        property_data = RecordDecoder.decode_property_record(property_record)

        # String data now only
        property_data['key'] = self._build_dynamic_data(property_data['key_id'])
        property_data['value'] = self._build_dynamic_data(property_data['value_id'])

        return property_data

    def _write_dynamic_data(self, data, dynamic_id):
        dynamic_records = RecordEncoder.encode_dynamic_data(data, dynamic_id)
        for record in dynamic_records:
            self.dbfs_manager.write_record(record, 'DynamicStorage')

    def _build_dynamic_data(self, dynamic_id) -> Optional[DB_TYPE]:
        data = ''
        while True:
            # read from dynamic storage until all data is collected
            try:
                dynamic_record = self.dbfs_manager.read_record(dynamic_id, 'DynamicStorage')
            except RecordNotFoundError as e:
                print(e)
                return None
            dynamic_data = RecordDecoder.decode_dynamic_data_record(dynamic_record)

            data += dynamic_data['data']
            dynamic_id = dynamic_data['next_chunk_id']

            if dynamic_id == INVALID_ID:
                break

        if data == 'True':
            return True
        elif data == 'False':
            return False
        else:
            try:
                data = int(data)
            except ValueError:
                pass

        return data
