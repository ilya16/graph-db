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
        Prepares node records and select appropriate node storage.
        :param node:    node object
        """
        if node.get_id() == INVALID_ID:
            node.set_id(self.get_stats()['NodeStorage'])

        if node.get_label().get_id() == INVALID_ID:
            label = self.insert_label(node.get_label())
            node.set_label(label)  # Update label

        if not update:
            first_property_id = self.get_stats()['PropertyStorage']
            for i in range(len(node.get_properties())):
                node.get_properties()[i].set_id(first_property_id + i)
                try:
                    node.get_properties()[i].set_next_property(node.get_properties()[i + 1])
                except IndexError:
                    pass

            for i in range(len(node.get_properties())):
                self.insert_property(node.get_properties()[i])

        node_record = RecordEncoder.encode_node(node)
        self.dbfs_manager.write_record(node_record, 'NodeStorage', update=update)

        return node

    def select_node(self, node_id: int):
        """
        Selects node with `id` from the appropriate storage.
        Collects all data from other stores.
        :return:
        """
        node = self.collect_and_link_objects(node_id, type='Node')

        return node

    def _get_node_data(self, node_id: int):
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
            return None

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

        if rel.get_label().get_id() == INVALID_ID:
            label = self.insert_label(rel.get_label())
            rel.set_label(label)  # Update label

        # Update start node if this relation is first for start node
        if rel.get_start_node().get_first_relationship() is None:
            rel.get_start_node().add_relationship(rel)
            self.update_node(rel.get_start_node())
        else:
            # If there are previous relationships - update dependency fields
            rel.get_start_node().get_relationships()[-1].set_start_next_rel(rel)
            self.update_relationship(rel.get_start_node().get_relationships()[-1])
            rel.set_end_prev_rel(rel.get_start_node().get_relationships()[-1])

        # Update end node if this relation is first for end node
        if rel.get_end_node().get_first_relationship() is None:
            rel.get_end_node().add_relationship(rel)
            self.update_node(rel.get_end_node())
        else:
            # If there are previous relationships - update dependency fields
            rel.get_end_node().get_relationships()[-1].set_end_next_rel(rel)
            self.update_relationship(rel.get_end_node().get_relationships()[-1])
            rel.set_end_prev_rel(rel.get_end_node().get_relationships()[-1])

        if not update:
            first_property_id = self.get_stats()['PropertyStorage']
            for i in range(len(rel.get_properties())):
                rel.get_properties()[i].set_id(first_property_id + i)
                try:
                    rel.get_properties()[i].set_next_property(rel.get_properties()[i + 1])
                except IndexError:
                    pass

            for i in range(len(rel.get_properties())):
                self.insert_property(rel.get_properties()[i])

        relationship_record = RecordEncoder.encode_relationship(rel)
        self.dbfs_manager.write_record(relationship_record, 'RelationshipStorage', update=update)

        return rel

    def _get_relationship_data(self, rel_id: int):
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
            return None

    def select_relationship(self, rel_id: int) -> Relationship:
        """
        Selects relationship with `id` from the appropriate storage.
        Collects all data from other storages.
        :return:
        """
        relationship = self.collect_and_link_objects(rel_id, type='Relationship')
        return relationship

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
        first_dynamic_id = self.get_stats()['DynamicStorage']

        dynamic_records = RecordEncoder.encode_dynamic_data(label.get_name(), first_dynamic_id)

        for record in dynamic_records:
            self.dbfs_manager.write_record(record, 'DynamicStorage')

        label_record = RecordEncoder.encode_label(label, first_dynamic_id)
        self.dbfs_manager.write_record(label_record, 'LabelStorage', update=update)

        return label

    def select_label(self, label_id: int) -> Label:
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
                    return Label(id=label_id, name=label_name)

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

    def select_property(self, prop_id: int) -> Property:
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

        # Collect next property
        if property_data['next_prop_id'] != INVALID_ID:
            next_property = self.select_property(property_data['next_prop_id'])
        else:
            next_property = None

        return Property(used=property_data['used'],
                        id=property_data['id'],
                        key=key,
                        value=value,
                        next_property=next_property)

    # Data Collector and Linker
    def collect_and_link_objects(self, entry_obj_id, type='Node'):
        node_ids_to_read = set()
        rel_ids_to_read = set()
        label_ids_to_read = set()
        property_ids_to_read = set()

        nodes_data = {}
        relationships_data = {}
        nodes = {-1: None}
        relationships = {-1: None}
        labels = {-1: None}
        first_properties = {-1: None}

        if type == 'Node':
            node_ids_to_read.add(entry_obj_id)
        elif type == 'Relationship':
            rel_ids_to_read.add(entry_obj_id)
        else:
            return None

        while label_ids_to_read or node_ids_to_read or rel_ids_to_read or property_ids_to_read:
            for label_id in label_ids_to_read:
                labels[label_id] = self.select_label(label_id)
            label_ids_to_read = set()

            for prop_id in property_ids_to_read:
                first_properties[prop_id] = self.select_property(prop_id)
            property_ids_to_read = set()

            for node_id in node_ids_to_read:
                if node_id != INVALID_ID:
                    node_data = self._get_node_data(node_id)
                    nodes_data[node_id] = node_data
                    nodes[node_id] = Node(id=node_id, used=node_data['used'])

                    if node_data['first_rel_id'] not in relationships_data:
                        rel_ids_to_read.add(node_data['first_rel_id'])

                    if node_data['label_id'] not in labels:
                        label_ids_to_read.add(node_data['label_id'])

                    if node_data['first_prop_id'] not in first_properties:
                        property_ids_to_read.add(node_data['first_prop_id'])
            node_ids_to_read = set()

            new_rel_ids = set()
            for rel_id in rel_ids_to_read:
                if rel_id != INVALID_ID and rel_id not in relationships_data:
                    rel_data = self._get_relationship_data(rel_id)
                    relationships_data[rel_id] = rel_data
                    relationships[rel_id] = Relationship(id=rel_id, used=rel_data['used'])

                    if rel_data['start_node'] not in relationships_data:
                        node_ids_to_read.add(rel_data['start_node'])

                    if rel_data['end_node'] not in relationships_data:
                        node_ids_to_read.add(rel_data['end_node'])

                    if rel_data['label_id'] not in labels:
                        label_ids_to_read.add(rel_data['label_id'])

                    if rel_data['start_prev_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['start_prev_id'])

                    if rel_data['start_next_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['start_next_id'])

                    if rel_data['end_prev_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['end_prev_id'])

                    if rel_data['end_next_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['end_next_id'])

                    if rel_data['first_prop_id'] not in first_properties:
                        property_ids_to_read.add(rel_data['first_prop_id'])
            rel_ids_to_read = new_rel_ids

        for node_id in nodes:
            if node_id != INVALID_ID:
                node = nodes[node_id]
                nodes_data = nodes_data[node_id]

                node.set_label(labels[nodes_data['label_id']])
                node.set_first_property(first_properties[nodes_data['first_prop_id']])
                node.set_first_relationship(relationships[nodes_data['first_rel_id']])

        for rel_id in relationships:
            if rel_id != INVALID_ID:
                rel = relationships[rel_id]
                rel_data = relationships_data[rel_id]

                rel.set_label(labels[rel_data['label_id']])
                rel.set_start_node(nodes[rel_data['start_node']])
                rel.set_end_node(nodes[rel_data['end_node']])

                rel.set_start_prev_rel(nodes[rel_data['start_prev_id']])
                rel.set_start_next_rel(nodes[rel_data['start_next_id']])
                rel.set_end_prev_rel(nodes[rel_data['end_prev_id']])
                rel.set_end_next_rel(nodes[rel_data['end_next_id']])

                rel.set_first_property(first_properties[rel_data['first_prop_id']])

        if type == 'Node':
            return nodes[entry_obj_id]
        elif type == 'Relationship':
            return relationships[entry_obj_id]
