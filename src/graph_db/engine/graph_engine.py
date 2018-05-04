from typing import Union, List

from graph_db.engine.engine import Engine
from graph_db.engine.graph import Graph
from graph_db.engine.label import Label
from graph_db.engine.relationship import Relationship
from graph_db.engine.node import Node
from graph_db.engine.property import Property
from graph_db.engine.types import INVALID_ID
from graph_db.fs.io_engine import IOEngine
from graph_db.fs.worker import Worker


class GraphEngine(Engine):
    def __init__(self, base_dir):
        self.io_engine = IOEngine()
        self.io_engine.add_worker(Worker(base_path=base_dir))

        # graph object
        self.graph = None

        # small indexes
        self.label_names = dict()
        self.node_labels = dict()
        self.rel_labels = dict()
        self.properties = dict()

        self._init_graph()

    def _init_graph(self):
        # is there some data on disk?
        stats = self.get_stats()
        for storage_type in stats:
            if stats[storage_type] != 0:
                self.graph = Graph('init')
                self.graph.set_empty(False)
                break

        if self.graph:
            self.graph.set_consistent(False)
            # TODO: read data from disk
            self.graph.set_consistent(True)

    def get_stats(self):
        return self.io_engine.get_stats()

    def close(self):
        self.graph = None
        self.io_engine.close()

    def create_graph(self, graph_name):
        self.graph = Graph(name=graph_name)

    def get_graph(self) -> Graph:
        return self.graph

    # Create

    def create_node(self,
                    label_name: str,
                    properties: List[Property] = None):
        # label
        label = self._insert_label(label_name)

        properties = list() if not properties else properties

        # node itself
        node = self.io_engine.insert_node(Node(label=label, properties=properties))

        # properties
        self._insert_properties(properties)

        # updating index
        if label_name in self.node_labels:
            self.node_labels[label_name].append(node)
        else:
            self.node_labels[label_name] = [node]

        self._update_properties_index(properties, node)

        self.graph.add_node(node)

        return node

    def create_relationship(self,
                            label_name: str,
                            start_node: Node,
                            end_node: Node,
                            properties: List[Property] = None):
        # label
        label = self._insert_label(label_name)

        properties = list() if not properties else properties

        # relationship itself
        rel = self.io_engine.insert_relationship(Relationship(label=label,
                                                              start_node=start_node,
                                                              end_node=end_node,
                                                              properties=properties))

        # properties
        self._insert_properties(properties)

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

        # updating index
        if label_name in self.rel_labels:
            self.rel_labels[label_name].append(rel)
        else:
            self.rel_labels[label_name] = [rel]

        self._update_properties_index(properties, rel)

        self.graph.add_relationship(rel)

        return rel

    def add_property(self, obj: Union[Node, Relationship], prop: Property):
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

    # Select

    def select_node(self, node_id: int) -> Node:
        node = self.graph.get_node(node_id)
        if node is None:
            # node = self.io_engine.select_node(node_id)
            self._collect_objects(entry_obj_id=node_id, obj_type='Node')
        node = self.graph.get_node(node_id)
        return node

    def select_nodes(self, label: Label = None) -> List[Node]:
        if not self.graph.is_consistent():
            # TODO: collect absent data
            pass
        nodes = list(self.graph.get_nodes().values())

        if label:
            nodes = self.node_labels[label]
            # nodes = filter(lambda n: n.get_label() == label, nodes)

        return nodes

    def select_relationship(self, rel_id: int) -> Relationship:
        rel = self.graph.get_relationship(rel_id)
        if rel is None:
            # rel = self.io_engine.select_relationship(rel_id)
            self._collect_objects(entry_obj_id=rel_id, obj_type='Relationship')
        rel = self.graph.get_relationship(rel_id)
        return rel

    def select_relationships(self, label: Label = None) -> List[Relationship]:
        if not self.graph.is_consistent():
            # TODO: collect absent data
            pass
        relationships = list(self.graph.get_relationships().values())

        if label:
            relationships = self.rel_labels[label]
            # relationships = filter(lambda r: r.get_label() == label, relationships)

        return relationships

    def select_label(self, label_id: int) -> Label:
        label = self.graph.get_label(label_id)
        if label is None:
            label = self._collect_label(label_id)
        return label

    def select_labels(self) -> List[Label]:
        if not self.graph.is_consistent():
            # TODO: collect absent data
            pass
        return list(self.graph.get_labels().values())

    # Update

    def update_node(self, node_id, prop):
        node = self.select_node_by_id(node_id)
        if node is not None:
            p = {prop.get_key(): prop.get_value()}
            key = frozenset(p.items())
            if key in self.properties:
                self.properties[key].append(node)
            else:
                self.properties[key] = [node]
            node.add_property(prop)
            return self.io_engine.update_node(node)
        else:
            return None

    def update_relationship(self, rel_id, prop):
        rel = self.select_relationship_by_id(rel_id)
        if rel is not None:
            p = {prop.get_key(): prop.get_value()}
            key = frozenset(p.items())
            if key in self.properties:
                self.properties[key].append(rel)
            else:
                self.properties[key] = [rel]
            rel.add_property(prop)
            return self.io_engine.update_relationship(rel)
        else:
            return None

    # Delete

    def delete_node(self, node_id):
        node = self.select_node(node_id)
        if node is not None:
            node.set_used(False)
            return self.io_engine.update_node(node)
        else:
            return None

    def delete_relationship(self, rel_id):
        rel = self.select_relationship(rel_id)
        if rel is not None:
            rel.set_used(False)
            return self.io_engine.update_relationship(rel)
        else:
            return None

    # Parametrized queries

    def select_edge_by_label(self, label):
        return self.select_relationships(label=label)

    def select_node_by_label(self, label):
        return self.select_nodes(label=label)

    def select_with_condition(self, key, value, cond, match_of):
        objects = []
        for prop in self.properties:
            prop_key = list(prop)[0][0]
            prop_value = list(prop)[0][1]
            if key == prop_key:
                if cond == '=' and prop_value == value:
                    for obj in self.properties[prop]:
                        if match_of == 'node' and isinstance(obj, Node):
                            objects.append(obj)
                        elif match_of == 'relationship' and isinstance(obj, Relationship):
                            objects.append(obj)
                elif cond == '>' and int(prop_value) > int(value):
                    for obj in self.properties[prop]:
                        if match_of == 'node' and isinstance(obj, Node):
                            objects.append(obj)
                        elif match_of == 'relationship' and isinstance(obj, Relationship):
                            objects.append(obj)
                elif cond == '<' and int(prop_value) < int(value):
                    for obj in self.properties[prop]:
                        if match_of == 'node' and isinstance(obj, Node):
                            objects.append(obj)
                        elif match_of == 'relationship' and isinstance(obj, Relationship):
                            objects.append(obj)
                elif cond == '>=' and int(prop_value) >= int(value):
                    for obj in self.properties[prop]:
                        if match_of == 'node' and isinstance(obj, Node):
                            objects.append(obj)
                        elif match_of == 'relationship' and isinstance(obj, Relationship):
                            objects.append(obj)
                elif cond == '<=' and int(prop_value) <= int(value):
                    for obj in self.properties[prop]:
                        if match_of == 'node' and isinstance(obj, Node):
                            objects.append(obj)
                        elif match_of == 'relationship' and isinstance(obj, Relationship):
                            objects.append(obj)
        return objects

    def select_by_property(self, key, value):
        # TODO: oops, deleted, or was not present? :(
        pass

    def traverse_graph(self):
        objects = []
        for node in self.node_labels:
            for n in self.node_labels[node]:
                objects.append(n)
        for edge in self.rel_labels:
            for e in self.rel_labels[edge]:
                objects.append(e)
        return objects

    # Internal methods

    def _insert_label(self, label_name):
        if label_name in self.label_names:
            label = self.graph.get_label(self.label_names[label_name])
        else:
            label = self.io_engine.insert_label(Label(name=label_name))
            self.label_names[label_name] = label.get_id()
            self.graph.add_label(label)
        return label

    def _insert_properties(self, properties: List[Property]):
        first_prop_id = self.get_stats()['PropertyStorage']
        i = 0
        for prop in properties:
            prop.set_id(first_prop_id + i)
            i += 1

        for prop in properties:
            self.io_engine.insert_property(prop)

        return properties

    def _update_properties_index(self, properties: List[Property], obj=None):
        for prop in properties:
            p = {prop.get_key(): prop.get_value()}
            key = frozenset(p.items())
            if key in self.properties:
                self.properties[key].append(obj)
            else:
                self.properties[key] = [obj]

    def _collect_objects(self, entry_obj_id: int, obj_type: str = 'Node'):
        node_ids_to_read = set()
        rel_ids_to_read = set()

        relationships_data = {}

        if obj_type == 'Node':
            node_ids_to_read.add(entry_obj_id)
        elif obj_type == 'Relationship':
            rel_ids_to_read.add(entry_obj_id)
        else:
            return None

        while node_ids_to_read or rel_ids_to_read:
            for node_id in node_ids_to_read:
                if node_id != INVALID_ID:
                    node_data = self.io_engine.select_node(node_id)

                    if node_data['first_rel_id'] != INVALID_ID and node_data['first_rel_id'] not in relationships_data:
                        rel_ids_to_read.add(node_data['first_rel_id'])

                    label = self.graph.get_label(node_data['label_id'])
                    if label is None:
                        label = self._collect_label(node_data['label_id'])

                    properties = self._collect_properties(node_data['first_prop_id'])

                    node = Node(id=node_id, label=label, properties=properties)

                    self.graph.add_node(node)

            node_ids_to_read = set()

            new_rel_ids = set()
            for rel_id in rel_ids_to_read:
                if rel_id != INVALID_ID and rel_id not in relationships_data:
                    rel_data = self.io_engine.select_relationship(rel_id)

                    start_node = self.graph.get_node(rel_data['start_node'])
                    if start_node is None:
                        node_ids_to_read.add(rel_data['start_node'])

                    end_node = self.graph.get_node(rel_data['end_node'])
                    if end_node is None:
                        node_ids_to_read.add(rel_data['end_node'])

                    label = self.graph.get_label(rel_data['label_id'])
                    if label is None:
                        label = self._collect_label(rel_data['label_id'])

                    rel_data['label'] = label

                    if rel_data['start_prev_id'] != INVALID_ID and rel_data['start_prev_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['start_prev_id'])

                    if rel_data['start_next_id'] != INVALID_ID and rel_data['start_next_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['start_next_id'])

                    if rel_data['end_prev_id'] != INVALID_ID and rel_data['end_prev_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['end_prev_id'])

                    if rel_data['end_next_id'] != INVALID_ID and rel_data['end_next_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['end_next_id'])

                    rel_data['properties'] = self._collect_properties(rel_data['first_prop_id'])
                    relationships_data[rel_id] = rel_data
            rel_ids_to_read = new_rel_ids

        for rel_id in relationships_data:
            if rel_id != INVALID_ID:
                rel_data = relationships_data[rel_id]
                rel = Relationship(id=rel_id,
                                   label=rel_data['label'],
                                   start_node=self.graph.get_node(rel_data['start_node']),
                                   end_node=self.graph.get_node(rel_data['end_node']),
                                   properties=rel_data['properties'])

                self.graph.add_relationship(rel)

    def _collect_label(self, label_id) -> Label:
        label_data = self.io_engine.select_label(label_id)
        label = Label(id=label_data['id'], name=label_data['name'], used=label_data['used'])

        self.label_names[label.get_name()] = label.get_id()
        self.graph.add_label(label)

        return label

    def _collect_properties(self, first_prop_id) -> List[Property]:
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
