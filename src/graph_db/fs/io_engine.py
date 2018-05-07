from typing import Dict, Optional

from graph_db.engine.label import Label
from graph_db.engine.node import Node
from graph_db.engine.property import Property
from graph_db.engine.relationship import Relationship
from graph_db.engine.types import *
from graph_db.engine.types import DFS_CONFIG_PATH

from .decoder import RecordDecoder
from .encoder import RecordEncoder

from multiprocessing import Process
import rpyc
from .manager import start_manager_service
import time
import json


class IOEngine:
    """
    Graph Database IO Engine.
    Processes graph database queries on IO level.
    """
    def __init__(self, config_path: str = DFS_CONFIG_PATH):
        self.config_path = config_path
        self.manager_address = [(str, int)]

        self.manager_pool = {}  # {port : manager_process}

        self.parse_config(config_path)

        # Setup manager node
        self.create_manager_node(self.manager_address[0][1])
        self.con = rpyc.classic.connect(self.manager_address[0][0], self.manager_address[0][1])  # Connect to manager
        self.manager = self.con.root.Manager()

        # Initialize managers statistics
        self.manager.update_stats()

    def get_stats(self) -> Dict[str, int]:
        """
        Returns total number of records in each type of storage.
        :return:        dictionary with stats
        """
        return self.manager.get_stats()

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
        self.manager.write_record(node_record, 'NodeStorage', update=update)

        return node

    def select_node(self, node_id: int) -> Dict[str, DB_TYPE]:
        """
        Selects node with `id` from the appropriate storage.
        Collects all data from other stores.
        :return:
        """
        node_record = self.manager.read_record(node_id, 'NodeStorage')
        if node_record:
            return RecordDecoder.decode_node_record(node_record)
        else:
            print(f'Node #{node_id} was not found')
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
        self.manager.write_record(relationship_record, 'RelationshipStorage', update=update)

        return rel

    def select_relationship(self, rel_id: object) -> object:
        """
        Selects relationship with `id` from the appropriate storage.
        Collects all data from other storages.
        :return:
        """
        relationship_record = self.manager.read_record(rel_id, 'RelationshipStorage')
        if relationship_record:
            return RecordDecoder.decode_relationship_record(relationship_record)
        else:
            print(f'Relationship #{rel_id} was not found')
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
        self.manager.write_record(label_record, 'LabelStorage', update=update)

        return label

    def select_label(self, label_id: int) -> Dict[str, DB_TYPE]:
        """
        Selects label with `id` from the appropriate storage.
        Collects all data from other stores.
        :param label_id:
        :return:
        """
        label_record = self.manager.read_record(label_id, 'LabelStorage')
        if label_record is None:
            print(f'Label #{label_id} was not found')
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
            old_property_record = self.manager.read_record(prop.get_id(), 'PropertyStorage')
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
        self.manager.write_record(property_record, 'PropertyStorage', update=update)

        return prop

    def select_property(self, prop_id: int) -> Dict[str, DB_TYPE]:
        """
        Selects property with `id` from the appropriate storage.
        Collects all data from other stores.
        :param prop_id:
        :return:
        """
        property_record = self.manager.read_record(prop_id, 'PropertyStorage')
        if property_record is None:
            print(f'Property #{prop_id} was not found')
            return dict()

        property_data = RecordDecoder.decode_property_record(property_record)

        # String data now only
        property_data['key'] = self._build_dynamic_data(property_data['key_id'])
        property_data['value'] = self._build_dynamic_data(property_data['value_id'])

        return property_data

    def _write_dynamic_data(self, data, dynamic_id):
        dynamic_records = RecordEncoder.encode_dynamic_data(data, dynamic_id)
        for record in dynamic_records:
            self.manager.write_record(record, 'DynamicStorage')

    def _build_dynamic_data(self, dynamic_id) -> Optional[DB_TYPE]:
        data = ''
        while True:
            # read from dynamic storage until all data is collected
            dynamic_record = self.manager.read_record(dynamic_id, 'DynamicStorage')
            if dynamic_record is None:
                print(f'Dynamic #{dynamic_id} was not found')
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

    # DFS control methods

    def create_manager_node(self, port=None):
        """
        Creates process bind to specific port, stores it in memory and starts it
        :param port:
        :return:
        """
        if port in self.manager_pool:
            return
        self.manager_pool[port] = Process(target=start_manager_service, args=(port, self.config_path))
        self.manager_pool[port].start()
        time.sleep(0.1)
        print(f'Manager UP at localhost:{port}')

    def get_processes(self):
        """
        Collect all processes: worker processes from manager and manager processes
        :return:
        """
        processes = [self.manager.get_worker_processes()]
        for p in self.manager_pool.keys():
            processes.append(self.manager_pool[p])
        return processes

    def print_processes(self, arg):
        print('Manager: ')
        print(self.manager_pool)
        print('\nWorkers: ')
        print(self.worker_pool)

    def close(self):
        """
        Closes file connections of all workers, then terminates all worker processes and finally terminates manager
        process
        :return:
        """
        self.manager.flush_workers()
        self.manager.close_workers()
        self.manager_pool[self.manager_address[0][1]].terminate()

    def parse_config(self, config_path):
        try:
            with open(config_path, "r") as f:
                res = json.load(f)
        except FileNotFoundError:
            with open('../../../' + config_path, "r") as f:
                res = json.load(f)
        self.manager_address = [(res['manager_config']['ip'], res['manager_config']['port'])]

