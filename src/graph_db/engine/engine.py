import abc
from typing import Union, List

from graph_db.engine.graph import Graph
from graph_db.engine.label import Label
from graph_db.engine.node import Node
from graph_db.engine.property import Property
from graph_db.engine.relationship import Relationship


class Engine(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def create_graph(self, graph_name):
        """

        :param graph_name:
        :return:
        """

    @abc.abstractmethod
    def get_graph(self) -> Graph:
        """

        :return:
        """

    @abc.abstractmethod
    def create_node(self, label_name: str, properties=list()) -> Node:
        """

        :param label_name:
        :param properties:
        :return:
        """

    @abc.abstractmethod
    def create_relationship(self, label_name: str, start_node: Node, end_node: Node, properties=list()):
        """

        :param label_name:
        :param start_node:
        :param end_node:
        :param properties:
        :return:
        """

    @abc.abstractmethod
    def add_property(self, obj: Union[Node, Relationship], prop: Property):
        """

        :param obj:
        :param prop:
        :return:
        """

    @abc.abstractmethod
    def select_node(self, node_id: int) -> Node:
        """

        :param node_id:
        :return:
        """

    @abc.abstractmethod
    def select_nodes(self, label: Label = None) -> List[Node]:
        """

        :param label:
        :return:
        """

    @abc.abstractmethod
    def select_relationship(self, rel_id: int) -> Relationship:
        """

        :param rel_id:
        :return:
        """

    @abc.abstractmethod
    def select_relationships(self, label: Label = None) -> List[Relationship]:
        """

        :param label:
        :return:
        """

    @abc.abstractmethod
    def select_label(self, label_id: int) -> Label:
        """

        :param label_id:
        :return:
        """

    @abc.abstractmethod
    def select_labels(self) -> List[Label]:
        """

        :return:
        """