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
        self.ids_nodes = dict()
        self.properties = dict()
        self.ids_edges = dict()

    def create_node(self, label_name, properties=list()):
        self.insert_node(label_name, properties)

    def create_edge(self, label_name, start_node, end_node, properties=list()):
        self.insert_relationship(label_name, start_node, end_node, properties)

    def insert_node(self, label_name, properties=list()):
        node_id = self.get_stats()['NodeStorage']
        node = Node(id=node_id)

        # label
        label = self._insert_label(label_name)
        node.set_label(label)

        if label in self.ids_nodes:
            self.ids_nodes[label].append(node)
        else:
            self.ids_nodes[label] = [node]

        # properties
        if properties:
            first_property = self._insert_properties(properties)
            node.set_first_property(first_property)

        # node itself
        node = self.io_engine.insert_node(node)
        self.nodes[node_id] = node

        return node

    def insert_relationship(self, label_name, start_node, end_node, properties=list()):
        rel_id = self.get_stats()['RelationshipStorage']
        rel = Relationship(id=rel_id, start_node=start_node, end_node=end_node)

        # label
        label = self._insert_label(label_name)
        rel.set_label(label)

        if label in self.ids_edges:
            self.ids_edges[label].append(rel)
        else:
            self.ids_edges[label] = [rel]

        # properties
        if properties:
            first_property = self._insert_properties(properties)
            rel.set_first_property(first_property)

        # updating start/end nodes data
        # update start node if this relation is first for start node
        if not start_node.get_first_relationship():
            start_node.set_first_relationship(rel)
            self.io_engine.update_node(start_node)
        else:
            # if there are previous relationships - update dependency fields
            last_rel = start_node.get_relationships()[-1]
            last_rel.set_start_next_rel(rel)
            rel.set_start_prev_rel(last_rel)
            self.io_engine.update_relationship(last_rel)
        start_node.add_relationship(rel)

        # update end node if this relation is first for end node
        if rel.get_end_node().get_first_relationship() is None:
            end_node.set_first_relationship(rel)
            self.io_engine.update_node(end_node)
        else:
            # if there are previous relationships - update dependency fields
            last_rel = end_node.get_relationships()[-1]
            last_rel.set_end_next_rel(rel)
            rel.set_end_prev_rel(last_rel)
            self.io_engine.update_relationship(last_rel)
        end_node.add_relationship(rel)

        # relationship itself
        rel = self.io_engine.insert_relationship(rel)
        self.relationships[rel_id] = rel

        return rel

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

    def select_properties(self, first_prop_id):
        if first_prop_id == INVALID_ID:
            return None

        property_data = self.io_engine.select_property(first_prop_id)
        prop = Property(used=property_data['used'],
                        id=property_data['id'],
                        key=property_data['key'],
                        value=property_data['value'])
        first_prop = prop
        while property_data['next_prop_id'] != INVALID_ID:
            next_property_data = self.io_engine.select_property(property_data['next_prop_id'])
            next_property = Property(used=next_property_data['used'],
                                     id=next_property_data['id'],
                                     key=next_property_data['key'],
                                     value=next_property_data['value'])
            prop.set_next_property(next_property)
            prop = next_property
            property_data = next_property

        return first_prop

    def select_nth_node(self, n):
        return self.io_engine.select_node(n)

    def select_nth_edge(self, n):
        return self.io_engine.select_relationship(n)

    def select_edge_by_label(self, label):
        return self.ids_edges[label]

    def select_node_by_label(self, label):
        return self.ids_nodes[label]

    def close_engine(self):
        self.io_engine.close()

    def get_stats(self):
        return self.io_engine.get_stats()

    # def select_with_condition(self, key, value, cond):
    #     to_return = []
    #     if cond == '=':
    #         for prop in self.properties:
    #             if self.properties[prop].get_ ==
    #     elif cond == '>':
    #
    #     elif cond == '<':

    def select_by_property(self, prop):
        objects = []
        for p in self.properties:
            if p.get_key() == prop.get_key():
                objects.append(self.properties[p])
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

    def _insert_properties(self, properties):
        first_prop_id = self.get_stats()['PropertyStorage']
        for i in range(len(properties)):
            properties[i].set_id(first_prop_id + i)
            try:
                properties[i].set_next_property(properties[i + 1])
            except IndexError:
                pass

        for prop in properties:
            self.io_engine.insert_property(prop)

        return properties[0]

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
                    self.nodes[node_id] = Node(id=node_id, used=node_data['used'])

                    if node_data['first_rel_id'] not in relationships_data:
                        rel_ids_to_read.add(node_data['first_rel_id'])

                    if node_data['label_id'] not in self.labels:
                        label_ids_to_read.add(node_data['label_id'])

                    first_prop = self.select_properties(node_data['first_prop_id'])
                    node_data['first_prop'] = first_prop
                    nodes_data[node_id] = node_data
            node_ids_to_read = set()

            new_rel_ids = set()
            for rel_id in rel_ids_to_read:
                if rel_id != INVALID_ID and rel_id not in relationships_data:
                    rel_data = self.io_engine.select_relationship(rel_id)
                    self.relationships[rel_id] = Relationship(id=rel_id, used=rel_data['used'])

                    if rel_data['start_node'] not in relationships_data:
                        node_ids_to_read.add(rel_data['start_node'])

                    if rel_data['end_node'] not in relationships_data:
                        node_ids_to_read.add(rel_data['end_node'])

                    if rel_data['label_id'] not in self.labels:
                        label_ids_to_read.add(rel_data['label_id'])

                    if rel_data['start_prev_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['start_prev_id'])

                    if rel_data['start_next_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['start_next_id'])

                    if rel_data['end_prev_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['end_prev_id'])

                    if rel_data['end_next_id'] not in relationships_data:
                        new_rel_ids.add(rel_data['end_next_id'])

                    first_prop = self.io_engine.select_property(rel_data['first_prop_id'])
                    rel_data['first_prop'] = first_prop
                    relationships_data[rel_id] = rel_data
            rel_ids_to_read = new_rel_ids

        for node_id in self.nodes:
            if node_id != INVALID_ID:
                node = self.nodes[node_id]
                nodes_data = nodes_data[node_id]

                node.set_label(self.labels[nodes_data['label_id']])
                node.set_first_property(nodes_data['first_prop'])
                node.set_first_relationship(self.relationships[nodes_data['first_rel_id']])

        for rel_id in self.relationships:
            if rel_id != INVALID_ID:
                rel = self.relationships[rel_id]
                rel_data = relationships_data[rel_id]

                rel.set_label(self.labels[rel_data['label_id']])
                rel.set_start_node(self.nodes[rel_data['start_node']])
                rel.set_end_node(self.nodes[rel_data['end_node']])

                rel.set_start_prev_rel(self.nodes[rel_data['start_prev_id']])
                rel.set_start_next_rel(self.nodes[rel_data['start_next_id']])
                rel.set_end_prev_rel(self.nodes[rel_data['end_prev_id']])
                rel.set_end_next_rel(self.nodes[rel_data['end_next_id']])

                rel.set_first_property(rel_data['first_prop'])
