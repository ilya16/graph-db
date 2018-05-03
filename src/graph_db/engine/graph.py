from typing import Union, List

from graph_db.engine.label import Label
from graph_db.engine.relationship import Relationship
from graph_db.engine.node import Node
from graph_db.engine.property import Property
from graph_db.engine.types import INVALID_ID
from graph_db.fs.io_engine import IOEngine
from graph_db.fs.worker import Worker


class Graph:

    def __init__(self, name, temp_dir):
        temp_storage = Worker(base_path=temp_dir)
        self.name = name
        self.label_names = dict()
        self.nodes = dict()
        self.relationships = dict()
        self.labels = dict()

        self.io_engine = IOEngine()
        self.io_engine.add_worker(temp_storage)
        self.labels = {}
        self.ids_nodes = dict()
        self.properties = dict()
        self.ids_edges = dict()

    def get_stats(self):
        return self.io_engine.get_stats()

    def close_engine(self):
        self.io_engine.close()

    def create_node(self, label_name: str, properties=list()):
        self.insert_node(label_name, properties)

    def create_edge(self, label_name: str, start_node: Node, end_node: Node, properties=list()):
        self.insert_relationship(label_name, start_node, end_node, properties)

    def create_property(self, obj: Union[Node, Relationship], prop: Property):
        self.insert_property(obj, prop)

    def insert_node(self, label_name, properties=list()) -> Node:
        # label
        label = self._insert_label(label_name)

        # node itself
        node = Node(label=label, properties=properties)

        # properties
        self._insert_properties(properties, node)

        node = self.io_engine.insert_node(node)

        if label_name in self.ids_nodes:
            self.ids_nodes[label_name].append(node)
        else:
            self.ids_nodes[label_name] = [node]

        self.nodes[node.get_id()] = node

        return node

    def insert_relationship(self, label_name: str, start_node: Node, end_node: Node, properties=list()) -> Relationship:
        # label
        label = self._insert_label(label_name)

        # relationship itself
        rel = Relationship(label=label,
                           start_node=start_node,
                           end_node=end_node,
                           properties=properties)

        # properties
        self._insert_properties(properties, rel)

        rel = self.io_engine.insert_relationship(rel)

        # updating start/end nodes data in io
        if rel.get_start_prev_rel():
            # if there are previous relationships - update previous relationship
            self.io_engine.update_relationship(rel.get_start_prev_rel())
        else:
            # update start node if this relation is first for start node
            self.io_engine.update_node(start_node)

        if rel.get_end_prev_rel():
            # if there are previous relationships - update previous relationship
            end_node.add_relationship(rel.get_end_prev_rel())
        else:
            # update end node if this relation is first for end node
            self.io_engine.update_node(end_node)

        if label_name in self.ids_edges:
            self.ids_edges[label_name].append(rel)
        else:
            self.ids_edges[label_name] = [rel]

        self.relationships[rel.get_id()] = rel

        return rel

    def insert_property(self, obj: Union[Node, Relationship], prop: Property):
        prop = self.io_engine.insert_property(prop)

        if isinstance(obj, Node) or isinstance(obj, Relationship):
            last_prop = obj.get_last_property()
            obj.add_property(prop)
            if last_prop:
                self.io_engine.update_property(last_prop)
            elif isinstance(obj, Node):
                self.io_engine.update_node(obj)
            else:
                self.io_engine.update_relationship(obj)
            return obj
        else:
            return None

    def select_node(self, node_id):
        if node_id not in self.nodes:
            self.collect_new_objects(node_id, 'Node')
        return self.nodes[node_id]

    def select_relationship(self, rel_id):
        if rel_id not in self.relationships:
            self.collect_new_objects(rel_id, 'Relationship')
        return self.relationships[rel_id]

    def select_label(self, label_id):
        if label_id not in self.labels:
            label = self.io_engine.select_label(label_id)
            self.labels[label_id] = label
        return self.labels[label_id]

    def select_properties(self, first_prop_id) -> List[Property]:
        properties = list()

        if first_prop_id == INVALID_ID:
            return properties

        property_data = self.io_engine.select_property(first_prop_id)
        prop = Property(used=property_data['used'],
                        id=property_data['id'],
                        key=property_data['key'],
                        value=property_data['value'])
        properties.append(prop)

        while property_data['next_prop_id'] != INVALID_ID:
            property_data = self.io_engine.select_property(property_data['next_prop_id'])
            next_property = Property(used=property_data['used'],
                                     id=property_data['id'],
                                     key=property_data['key'],
                                     value=property_data['value'])
            prop.set_next_property(next_property)
            prop = next_property
            properties.append(prop)

        return properties

    def select_edge_by_label(self, label):
        return self.ids_edges[label]

    def select_node_by_label(self, label):
        return self.ids_nodes[label]

    def select_with_condition(self, key, value, cond, match_of):
        objects = []
        for prop in self.properties:
            prop_key = list(prop)[0][0]
            prop_value = list(prop)[0][1]
            if key == prop_key:
                if cond == '=' and prop_value == value:
                    for obj in self.properties[prop]:
                        if match_of == 'node' and type(obj) is Node:
                            objects.append(obj)
                        elif match_of == 'edge' and type(obj) is Relationship:
                            objects.append(obj)
                elif cond == '>' and int(prop_value) > int(value):
                    for obj in self.properties[prop]:
                        if match_of == 'node' and type(obj) is Node:
                            objects.append(obj)
                        elif match_of == 'edge' and type(obj) is Relationship:
                            objects.append(obj)
                elif cond == '<' and int(prop_value) < int(value):
                    for obj in self.properties[prop]:
                        if match_of == 'node' and type(obj) is Node:
                            objects.append(obj)
                        elif match_of == 'edge' and type(obj) is Relationship:
                            objects.append(obj)
                elif cond == '>=' and int(prop_value) >= int(value):
                    for obj in self.properties[prop]:
                        if match_of == 'node' and type(obj) is Node:
                            objects.append(obj)
                        elif match_of == 'edge' and type(obj) is Relationship:
                            objects.append(obj)
                elif cond == '<=' and int(prop_value) <= int(value):
                    for obj in self.properties[prop]:
                        if match_of == 'node' and type(obj) is Node:
                            objects.append(obj)
                        elif match_of == 'edge' and type(obj) is Relationship:
                            objects.append(obj)
        return objects

    def traverse_graph(self):
        objects = []
        for node in self.ids_nodes:
            for n in self.ids_nodes[node]:
                objects.append(n)
        for edge in self.ids_edges:
            for e in self.ids_edges[edge]:
                objects.append(e)
        return objects

    def _insert_label(self, label_name):
        if label_name in self.label_names:
            label = self.labels[self.label_names[label_name]]
        else:
            label_id = self.get_stats()['LabelStorage']
            label = self.io_engine.insert_label(Label(id=label_id, name=label_name))
            self.label_names[label_name] = label.get_id()
            self.labels[label.get_id()] = label
        return label

    def _insert_properties(self, properties, obj=None):
        first_prop_id = self.get_stats()['PropertyStorage']
        i = 0
        for prop in properties:
            prop.set_id(first_prop_id + i)
            self.io_engine.insert_property(prop)
            i += 1

            if obj:
                p = {prop.get_key(): prop.get_value()}
                key = frozenset(p.items())
                if key in self.properties:
                    self.properties[key].append(obj)
                else:
                    self.properties[key] = [obj]

        return properties

    def collect_new_objects(self, entry_obj_id, type='Node'):
        node_ids_to_read = set()
        rel_ids_to_read = set()
        label_ids_to_read = set()

        nodes_data = {}
        relationships_data = {}

        if type == 'Node':
            node_ids_to_read.add(entry_obj_id)
        elif type == 'Relationship':
            rel_ids_to_read.add(entry_obj_id)
        else:
            return None

        while label_ids_to_read or node_ids_to_read or rel_ids_to_read:
            for label_id in label_ids_to_read:
                self.select_label(label_id)
            label_ids_to_read = set()

            for node_id in node_ids_to_read:
                if node_id != INVALID_ID:
                    node_data = self.io_engine.select_node(node_id)

                    if node_data['first_rel_id'] != INVALID_ID and node_data['first_rel_id'] not in relationships_data:
                        rel_ids_to_read.add(node_data['first_rel_id'])

                    if node_data['label_id'] not in self.labels:
                        label_ids_to_read.add(node_data['label_id'])

                        nodes_data['properties'] = self.select_properties(nodes_data['first_prop_id'])
                    nodes_data[node_id] = node_data
            node_ids_to_read = set()

            new_rel_ids = set()
            for rel_id in rel_ids_to_read:
                if rel_id != INVALID_ID and rel_id not in relationships_data:
                    rel_data = self.io_engine.select_relationship(rel_id)

                    if rel_data['start_node'] not in relationships_data:
                        node_ids_to_read.add(rel_data['start_node'])

                    if rel_data['end_node'] not in relationships_data:
                        node_ids_to_read.add(rel_data['end_node'])

                    if rel_data['label_id'] not in self.labels:
                        label_ids_to_read.add(rel_data['label_id'])

                    if rel_data['start_prev_id'] != INVALID_ID and rel_data['start_prev_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['start_prev_id'])

                    if rel_data['start_next_id'] != INVALID_ID and rel_data['start_next_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['start_next_id'])

                    if rel_data['end_prev_id'] != INVALID_ID and rel_data['end_prev_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['end_prev_id'])

                    if rel_data['end_next_id'] != INVALID_ID and rel_data['end_next_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['end_next_id'])

                    rel_data['properties'] = self.select_properties(rel_data['first_prop_id'])
                    relationships_data[rel_id] = rel_data
            rel_ids_to_read = new_rel_ids

        for node_id in nodes_data:
            if node_id != INVALID_ID:
                node_data = nodes_data[node_id]
                node = Node(id=node_id, label=self.labels[node_data['label_id']])

                for prop in nodes_data['properties']:
                    node.add_property(prop)

                self.nodes[node_id] = node

        for rel_id in relationships_data:
            if rel_id != INVALID_ID:
                rel_data = relationships_data[rel_id]
                rel = Relationship(id=rel_id,
                                   label=self.labels[rel_data['label_id']],
                                   start_node=self.nodes[rel_data['start_node']],
                                   end_node=self.nodes[rel_data['end_node']])

                for prop in rel_data['properties']:
                    rel.add_property(prop)

                # rel.set_start_prev_rel(self.nodes[rel_data['start_prev_id']])
                # rel.set_start_next_rel(self.nodes[rel_data['start_next_id']])
                # rel.set_end_prev_rel(self.nodes[rel_data['end_prev_id']])
                # rel.set_end_next_rel(self.nodes[rel_data['end_next_id']])

                self.relationships[rel_id] = rel
