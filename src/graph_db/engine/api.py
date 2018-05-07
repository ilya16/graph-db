import abc
from typing import Union, List, Dict

from graph_db.engine.graph import Graph
from graph_db.engine.label import Label
from graph_db.engine.node import Node
from graph_db.engine.property import Property
from graph_db.engine.relationship import Relationship
from graph_db.engine.types import DB_TYPE


class EngineAPI(metaclass=abc.ABCMeta):

    # Connection and Status

    @abc.abstractmethod
    def open(self):
        """ Prepares Graph Engine. """

    @abc.abstractmethod
    def close(self):
        """ Closes Graph Engine and all underlying resources. """

    @abc.abstractmethod
    def clear(self):
        """ Clears Graph Engine (removes all objects from main memory) """

    # Stats methods

    @abc.abstractmethod
    def get_stats(self) -> Dict[str, int]:
        """
        Returns stats for each storage.
        :return:            dict with number of nodes
        """

    # Graph

    @abc.abstractmethod
    def create_graph(self, graph_name):
        """
        Creates graph in the database.
        :param graph_name:  name of the graph
        :return:            created graph object
        :raises:            GraphEngineError if graph already exists
        """

    @abc.abstractmethod
    def get_graph(self) -> Graph:
        """
        Returns graph object.
        :return:            graph object
        """

    # Create statements

    @abc.abstractmethod
    def create_node(self, label_name: str, properties: List[Property] = None) -> Node:
        """
        Creates new node with optional properties.
        :param label_name:  node label
        :param properties:  node properties
        :return:            created node
        """

    @abc.abstractmethod
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

    # Update

    @abc.abstractmethod
    def add_property(self, obj: Union[Node, Relationship], prop: Property) -> Union[Node, Relationship]:
        """
        Adds new property to a node.
        :param obj:         object to which property is added (node or relationship)
        :param prop:        added property
        :return:            object with added property
        :raises:            GraphEngineError if object is not node or relationship
        """

    # Select statements

    @abc.abstractmethod
    def select_node(self, node_id: int) -> Node:
        """
        Selects node from graph by `node_id`.
        :param node_id:     node identifier
        :return:            found node
        :raises:            GraphEngineError if node is not found
        """

    @abc.abstractmethod
    def select_nodes(self,
                     label: str = None,
                     prop_key: DB_TYPE = None,
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

    @abc.abstractmethod
    def select_relationship(self, rel_id: int) -> Relationship:
        """
        Selects relationship from graph by `rel_id`.
        :param rel_id:      relationship identifier
        :return:            found relationship
        :raises:            GraphEngineError if relationship is not found
        """

    @abc.abstractmethod
    def select_relationships(self,
                             label: str = None,
                             prop_key: DB_TYPE = None,
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

    @abc.abstractmethod
    def select_label(self, label_id: int) -> Label:
        """
        Selects label from graph by `label_id`.
        :param label_id:    label identifier
        :return:            found label
        """

    @abc.abstractmethod
    def select_labels(self) -> List[Label]:
        """
        Selects all labels in the graph.
        :return:            list of labels
        """

    @abc.abstractmethod
    def select_graph_objects(self) -> List[Union[Node, Relationship]]:
        """
        Returns all graph nodes and relationships in one list.
        :return:            list of graph objects
        """

    # Delete statements

    @abc.abstractmethod
    def delete_node(self, node_id) -> Node:
        """
        Deletes node from graph by `node_id`.
        :param node_id:     node identifier
        :return:            deleted node (not present in the graph anymore)
        :raises:            GraphEngineError if node is not found
        """

    @abc.abstractmethod
    def delete_relationship(self, rel_id) -> Relationship:
        """
        Deletes relationship from graph by `node_id`.
        :param rel_id:      relationship identifier
        :return:            deleted relationship (not present in the graph anymore)
        :raises:            GraphEngineError if relationship is not found
        """