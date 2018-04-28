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

        # TODO: PLEASE REFACTOR this BAD bad code below...
        if node.get_first_property():
            if len(node.get_properties()) > 1:
                for i in range(1, len(node.get_properties())):
                    idx = self.get_stats()['PropertyStorage'] + 1  # next property id
                    node.get_properties()[i].set_id(idx)
                    node.get_properties()[i-1].set_next_property(node.get_properties()[i])
                    property = self.insert_property(node.get_properties()[i-1])
                    node.get_properties()[i-1].set_id(property.get_id())
                property = self.insert_property(node.get_properties()[-1])
                node.get_properties()[-1].set_id(property.get_id())
            else:
                property = self.insert_property(node.get_first_property())
                node.get_first_property().set_id(property.get_id())

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

        # collecting data from other storages and building node
        node_label = self.select_label(node_data['label_id'])

        # TODO: implement for property and relationships

        # finally return node with all data
        return Node(id=node_data['id'],
                    label=node_label,
                    used=node_data['used'])

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

        relationship_id = self.get_stats()['RelationshipStorage']

        if rel.get_label().get_id() == -1:
            label = self.insert_label(rel.get_label())
            rel.set_label(label)  # Update label

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
        rel.set_id(relationship_id)
        relationship_record = RecordEncoder.encode_relationship(rel)
        self.dbfs_manager.write_record(relationship_record, 'RelationshipStorage')

        return rel

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

        # Below, we are collecting all relationship's data from storage
        rel_start_node = self.select_node(relationship_data['start_node'])
        rel_end_node = self.select_node(relationship_data['end_node'])

        rel_label = self.select_label(relationship_data['label_id'])

        # Here we run through all relations... This is kinda bad. I think so...
        # rel_start_prev = self.select_relationship(relationship_data['start_prev_id'])
        # rel_start_next = self.select_relationship(relationship_data['start_next_id'])
        # rel_end_prev = self.select_relationship(relationship_data['end_prev_id'])
        # rel_end_next = self.select_relationship(relationship_data['end_next_id'])

        # finally return node with all data
        return Relationship(id=relationship_data['id'],
                            label=rel_label,
                            start_node=rel_start_node,
                            end_node=rel_end_node,
                            # start_prev_rel=rel_start_prev,
                            # start_next_rel=rel_start_next,
                            # end_prev_rel=rel_end_prev,
                            # end_next_rel=rel_end_next,
                            used=relationship_data['used'])

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
        """
        Selects label with `id` from the appropriate storage.
        Collects all data from other storages.
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
                    return Label(id=label_id,
                                 name=label_name)

    def update_label(self, label: Label):
        pass

    def insert_property(self, prop: Property):
        """
        Prepares property record and select appropriate property storage.
        :param prop:
        :return:
        """
        property_id = self.get_stats()['PropertyStorage']

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

        prop.set_id(property_id)
        property_record = RecordEncoder.encode_property(used=prop.is_used(),
                                                        key_id=key_dynamic_id,
                                                        value_id=value_dynamic_id)
        self.dbfs_manager.write_record(property_record, 'PropertyStorage')

        return prop

    def select_property(self, prop_id: int) -> Property:
        """
        Selects property with `id` from the appropriate storage.
        Collects all data from other storages.
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
        # next_property = self.select_property(property_data['next_prop_id'])

        return Property(used=property_data['used'],
                        id=property_data['id'],
                        key=key,
                        value=value)

    def update_property(self, prop: Property):
        pass
