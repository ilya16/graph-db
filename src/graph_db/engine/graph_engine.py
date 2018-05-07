from typing import Union, List, Dict, Callable, Optional

from graph_db.engine.api import EngineAPI
from graph_db.engine.error import GraphEngineError
from graph_db.engine.graph import Graph
from graph_db.engine.label import Label
from graph_db.engine.relationship import Relationship
from graph_db.engine.node import Node
from graph_db.engine.property import Property
from graph_db.engine.types import INVALID_ID, DB_TYPE, DFS_CONFIG_PATH
from graph_db.fs.io_engine import IOEngine


class GraphEngine(EngineAPI):
    def __init__(self, conf_path: str = DFS_CONFIG_PATH):
        self.io_engine = IOEngine(conf_path)

        # graph object
        self.graph = None

        # small indexes
        self.label_names = dict()
        self.node_labels = dict()
        self.rel_labels = dict()
        self.properties = dict()

        self.open()

    # Connection and Status

    def open(self):
        """ Prepares Graph Engine. """
        stats = self.get_stats()
        for storage_type in stats:
            if stats[storage_type] != 0:
                self.graph = Graph('init')
                self.graph.set_consistent(False)
                self.graph.set_empty(False)
                break

    def close(self):
        """ Closes Graph Engine and all underlying resources. """
        self.graph = None
        self.io_engine.close()

    def clear(self):
        """ Clears Graph Engine (removes all objects from main memory) """
        self.graph.clear()
        self.label_names = dict()
        self.node_labels = dict()
        self.rel_labels = dict()
        self.properties = dict()

    # Stats methods

    def get_stats(self) -> Dict[str, int]:
        """
        Returns stats for each storage.
        :return:            dict with number of nodes
        """
        return self.io_engine.get_stats()

    # Graph

    def create_graph(self, graph_name):
        """
        Creates graph in the database.
        :param graph_name:  name of the graph
        :return:            created graph object
        :raises:            GraphEngineError if graph already exists
        """
        if self.graph:
            raise GraphEngineError('Graph already exists, please, delete it and try again.')
        self.graph = Graph(name=graph_name)
        return self.graph

    def get_graph(self) -> Graph:
        """
        Returns graph object.
        :return:            graph object
        """
        return self.graph

    # Create statements

    def create_node(self,
                    label_name: str,
                    properties: List[Property] = None) -> Node:
        """
        Creates new node with optional properties.
        :param label_name:  node label
        :param properties:  node properties
        :return:            created node
        """
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

        self._update_properties_index(node)

        self.graph.add_node(node)

        return node

    def create_relationship(self,
                            label_name: str,
                            start_node: Node,
                            end_node: Node,
                            properties: List[Property] = None) -> Relationship:
        """
        Creates new relationship between two nodes with optional properties.
        :param label_name:  relationship label
        :param start_node:  start node
        :param end_node:    end node
        :param properties:  relationship properties
        :return:            created relationship
        :raises:            GraphEngineError if start or end nodes are not specified
        """
        if not (start_node and end_node):
            raise GraphEngineError('Start or End nodes for relationship were not found')

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
            self.io_engine.update_relationship(rel.get_end_prev_rel())
        else:
            # update end node if this relation is first for end node
            self.io_engine.update_node(end_node)

        # updating index
        if label_name in self.rel_labels:
            self.rel_labels[label_name].append(rel)
        else:
            self.rel_labels[label_name] = [rel]

        self._update_properties_index(rel)

        self.graph.add_relationship(rel)

        return rel

    # Update statements

    def add_property(self, obj: Union[Node, Relationship], prop: Property) -> Union[Node, Relationship]:
        """
        Adds new property to a node.
        :param obj:         object to which property is added (node or relationship)
        :param prop:        added property
        :return:            object with added property
        :raises:            GraphEngineError if object is not node or relationship
        """
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

            p = {prop.get_key(): prop.get_value()}
            key = frozenset(p.items())
            if key in self.properties:
                self.properties[key].append(obj)
            else:
                self.properties[key] = [obj]

            return obj
        else:
            raise GraphEngineError('Passed object is nor a node, neither relationship')

    # Select statements

    def select_node(self, node_id: int) -> Node:
        """
        Selects node from graph by `node_id`.
        :param node_id:     node identifier
        :return:            found node
        :raises:            GraphEngineError if node is not found
        """
        node = self.graph.get_node(node_id)

        if node is None:
            self._collect_objects(entry_obj_id=node_id, obj_type='Node')
            node = self.graph.get_node(node_id)

        if not node:
            raise GraphEngineError(f'Node #{node_id} was not found')
        else:
            self._update_properties_index(node)

        return node

    def select_nodes(self,
                     label: str = None,
                     prop_key: str = None,
                     prop_value: DB_TYPE = None,
                     query_cond: str = None) -> List[Node]:
        """
        Selects all nodes in the graph by conditions.
        :param label:       optional node label
        :param prop_key:    optional property key
        :param prop_value:  optional property value
        :param query_cond:  optional property comparison condition
        :return:            found nodes
        """
        if not self.graph.is_consistent():
            self._collect_graph()

        if label:
            nodes = self.node_labels[label] if label in self.node_labels else list()
        else:
            nodes = list(self.graph.get_nodes().values())

        if query_cond:
            cond_func = self._cond_function(query_cond)
            nodes = list(filter(lambda node:
                                any(prop_key == prop.get_key() and cond_func(prop.get_value(), prop_value)
                                    for prop in node.get_properties()), nodes))

        return nodes

    def select_relationship(self, rel_id: int) -> Relationship:
        """
        Selects relationship from graph by `rel_id`.
        :param rel_id:      relationship identifier
        :return:            found relationship
        :raises:            GraphEngineError if relationship is not found
        """
        rel = self.graph.get_relationship(rel_id)
        if rel is None:
            self._collect_objects(entry_obj_id=rel_id, obj_type='Relationship')
            rel = self.graph.get_relationship(rel_id)

        if not rel:
            raise GraphEngineError(f'Relationship #{rel_id} was not found')
        else:
            self._update_properties_index(rel)

        return rel

    def select_relationships(self,
                             label: str = None,
                             prop_key: str = None,
                             prop_value: DB_TYPE = None,
                             query_cond: str = None) -> List[Relationship]:
        """
        Selects all relationships in the graph by conditions.
        :param label:       optional relationship label
        :param prop_key:    optional property key
        :param prop_value:  optional property value
        :param query_cond:  optional property comparison condition
        :return:            found relationships
        """
        if not self.graph.is_consistent():
            self._collect_graph()

        if label:
            relationships = self.rel_labels[label] if label in self.rel_labels else list()
        else:
            relationships = list(self.graph.get_relationships().values())

        if query_cond:
            cond_func = self._cond_function(query_cond)
            relationships = list(filter(lambda node:
                                        any(prop_key == prop.get_key() and cond_func(prop.get_value(), prop_value)
                                            for prop in node.get_properties()), relationships))

        return relationships

    def select_label(self, label_id: int) -> Label:
        """
        Selects label from graph by `label_id`.
        :param label_id:    label identifier
        :return:            found label
        """
        label = self.graph.get_label(label_id)

        if label is None:
            label = self._collect_label(label_id)

        if not label:
            raise GraphEngineError(f'Label #{label_id} was not found')

        return label

    def select_labels(self) -> List[Label]:
        """
        Selects all labels in the graph.
        :return:            list of labels
        """
        if not self.graph.is_consistent():
            self._collect_graph()
        return list(self.graph.get_labels().values())

    def select_graph_objects(self) -> List[Union[Node, Relationship]]:
        """
        Returns all graph nodes and relationships in one list.
        :return:            list of graph objects
        """
        if not self.graph.is_consistent():
            self._collect_graph()
        return self.select_nodes() + self.select_relationships()

    # Delete statements

    def delete_node(self, node_id) -> Node:
        """
        Deletes node from graph by `node_id`.
        :param node_id:     node identifier
        :return:            deleted node (not present in the graph anymore)
        :raises:            GraphEngineError if node is not found
        """
        node = self.select_node(node_id)
        if node:
            node.set_used(False)
            self.node_labels[node.get_label().get_name()].remove(node)
            self.graph.remove_node(node_id)
            rel_ids = [rel.get_id() for rel in node.get_relationships()]
            for rel_id in rel_ids:
                self.delete_relationship(rel_id)
            return self.io_engine.update_node(node)
        else:
            raise GraphEngineError(f'Node #{node_id} was not found')

    def delete_relationship(self, rel_id) -> Relationship:
        """
        Deletes relationship from graph by `node_id`.
        :param rel_id:      relationship identifier
        :return:            deleted relationship (not present in the graph anymore)
        :raises:            GraphEngineError if relationship is not found
        """
        rel = self.select_relationship(rel_id)
        if rel:
            rel.set_used(False)
            self.rel_labels[rel.get_label().get_name()].remove(rel)
            self.graph.remove_relationship(rel_id)
            rel.remove_dependencies()
            return self.io_engine.update_relationship(rel)
        else:
            raise GraphEngineError(f'Relationship #{rel_id} was not found')

    # Internal methods

    def _insert_label(self, label_name) -> Label:
        """
        Inserts new label into storage.
        :param label_name:  label name
        :return:            inserted label
        """
        if label_name in self.label_names:
            label = self.graph.get_label(self.label_names[label_name])
        else:
            label = self.io_engine.insert_label(Label(name=label_name))
            self.label_names[label_name] = label.get_id()
            self.graph.add_label(label)
        return label

    def _insert_properties(self, properties: List[Property]) -> List[Property]:
        """
        Inserts properties into storage.
        :param properties:  list of properties
        :return:            inserted properties
        """
        first_prop_id = self.get_stats()['PropertyStorage']
        i = 0
        for prop in properties:
            prop.set_id(first_prop_id + i)
            i += 1

        for prop in properties:
            self.io_engine.insert_property(prop)

        return properties

    def _update_properties_index(self, obj: Union[Node, Relationship]):
        """
        Updates properties index.
        :param obj:         obj with new properties
        """
        for prop in obj.get_properties():
            p = {prop.get_key(): prop.get_value()}
            key = frozenset(p.items())
            if key in self.properties:
                self.properties[key].append(obj)
            else:
                self.properties[key] = [obj]

    def _collect_graph(self):
        """ Collects graph into memory. """
        last_node_it = self.get_stats()['NodeStorage']
        last_rel_id = self.get_stats()['RelationshipStorage']

        for idx in range(last_node_it):
            if self.graph.get_node(idx) is None:
                self._collect_objects(idx)

        for idx in range(last_rel_id):
            if self.graph.get_relationship(idx) is None:
                self._collect_objects(idx)

        self.graph.set_consistent(True)

    def _collect_objects(self,
                         entry_obj_id: int,
                         obj_type: str = 'Node',
                         hops: int = 10) -> Union[Node, Relationship]:
        """
        Collects all objects connected with objects in the distance of `hops` hops.
        :param entry_obj_id:    identifier of entry object
        :param obj_type:        entry object type
        :return:
        """
        node_ids_to_read = set()
        rel_ids_to_read = set()

        relationships_data = {}

        if obj_type == 'Node':
            node_ids_to_read.add(entry_obj_id)
        elif obj_type == 'Relationship':
            rel_ids_to_read.add(entry_obj_id)
        else:
            raise GraphEngineError('Object type is incorrect')

        count = 0

        while (node_ids_to_read or rel_ids_to_read) and count < hops:
            for node_id in node_ids_to_read:
                if node_id != INVALID_ID:
                    node_data = self.io_engine.select_node(node_id)
                    if node_data and node_data['used']:
                        if node_data['first_rel_id'] != INVALID_ID \
                                and node_data['first_rel_id'] not in relationships_data:
                            rel_ids_to_read.add(node_data['first_rel_id'])

                        label = self.graph.get_label(node_data['label_id'])
                        if label is None:
                            label = self._collect_label(node_data['label_id'])

                        properties = self._collect_properties(node_data['first_prop_id'])

                        node = Node(id=node_id, label=label, properties=properties)

                        # updating index
                        label_name = node.get_label().get_name()
                        if label_name in self.node_labels:
                            self.node_labels[label_name].append(node)
                        else:
                            self.node_labels[label_name] = [node]
                        self._update_properties_index(node)

                        self.graph.add_node(node)
            node_ids_to_read = set()

            new_rel_ids = set()
            for rel_id in rel_ids_to_read:
                if rel_id != INVALID_ID and self.graph.get_relationship(rel_id) is None \
                        and rel_id not in relationships_data:
                    rel_data: Dict[str, Union[DB_TYPE, Label, List[Property]]] \
                        = self.io_engine.select_relationship(rel_id)
                    if rel_data and rel_data['used']:
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

            count += 1

        for rel_id in relationships_data:
            if rel_id != INVALID_ID:
                rel_data = relationships_data[rel_id]
                rel = Relationship(id=rel_id,
                                   label=rel_data['label'],
                                   start_node=self.graph.get_node(rel_data['start_node']),
                                   end_node=self.graph.get_node(rel_data['end_node']),
                                   properties=rel_data['properties'])

                self.graph.add_relationship(rel)

                # updating index
                label_name = rel.get_label().get_name()
                if label_name in self.rel_labels:
                    self.rel_labels[label_name].append(rel)
                else:
                    self.rel_labels[label_name] = [rel]
                self._update_properties_index(rel)

        if count < hops:
            self.graph.set_consistent(True)

        if obj_type == 'Node':
            return self.graph.get_node(entry_obj_id)
        else:
            return self.graph.get_relationship(entry_obj_id)

    def _collect_label(self, label_id) -> Optional[Label]:
        """
        Collects label from storage.
        :param label_id:    label identifier
        :return:            collected label object
        """
        label_data = self.io_engine.select_label(label_id)
        if label_data and label_data['used']:
            label = Label(id=label_data['id'], name=label_data['name'], used=label_data['used'])

            self.label_names[label.get_name()] = label.get_id()
            self.graph.add_label(label)
        else:
            raise GraphEngineError(f'Label #{label_id} was not found')

        return label

    def _collect_properties(self, first_prop_id) -> List[Property]:
        """
        Collects properties from storage.
        :param first_prop_id:   identifier of first property
        :return:                collected list of properties
        """
        properties = list()

        if first_prop_id == INVALID_ID:
            return properties

        property_data = self.io_engine.select_property(first_prop_id)
        if not property_data:
            return properties
        prop = Property(used=property_data['used'],
                        id=property_data['id'],
                        key=property_data['key'],
                        value=property_data['value'])
        properties.append(prop)

        while property_data['next_prop_id'] != INVALID_ID:
            property_data = self.io_engine.select_property(property_data['next_prop_id'])
            if not property_data:
                return properties

            next_property = Property(used=property_data['used'],
                                     id=property_data['id'],
                                     key=property_data['key'],
                                     value=property_data['value'])
            prop.set_next_property(next_property)
            prop = next_property
            properties.append(prop)

        return properties

    @staticmethod
    def _cond_function(cond) -> Callable:
        """
        Constructs comparison method by condition string.
        :param cond:    condition string
        :return:        callable object
        """
        if cond == '=':
            return lambda x, y: x == y
        elif cond == '>':
            return lambda x, y: float(x) > float(y)
        elif cond == '<':
            return lambda x, y: float(x) < float(y)
        elif cond == '>=':
            return lambda x, y: float(x) >= float(y)
        elif cond == '<=':
            return lambda x, y: float(x) <= float(y)
