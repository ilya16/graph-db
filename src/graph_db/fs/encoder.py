from typing import Dict

from graph_db.engine.label import Label
from graph_db.engine.node import Node
from graph_db.engine.relationship import Relationship
from graph_db.engine.types import *
from .record import Record


class RecordEncoder:
    def __init__(self):
        pass

    @staticmethod
    def encode_node(node: Node) -> Record:
        """
        Encodes node object into a physical node record.
        Node record format:
            1 byte      `is_used` byte
            4 bytes     pointer to `id` of node label
            4 bytes     pointer to `id` of first relationship
            4 bytes     pointer to `id` of first property
        Total: 13 bytes
        :param node:    node object
        :return:
        """
        node_bytes = node.used.to_bytes(1, byteorder=BYTEORDER)

        node_bytes += node.get_label().id.to_bytes(4, byteorder=BYTEORDER)

        first_prop = node.get_first_property()
        if first_prop:
            node_bytes += first_prop.id.to_bytes(4, byteorder=BYTEORDER)
        else:
            node_bytes += b'0' * 4

        first_rel = node.get_first_relationship()
        if first_rel:
            node_bytes += first_rel.id.to_bytes(4, byteorder=BYTEORDER)
        else:
            node_bytes += b'0' * 4

        record = Record(node_bytes, node.id)

        assert record.size == NODE_RECORD_SIZE

        return record

    @staticmethod
    def encode_relationship(rel: Relationship) -> Record:
        pass

    @staticmethod
    def encode_label(label: Label) -> Record:
        pass

    @staticmethod
    def encode_property(prop: Dict[object, object]) -> Record:
        pass

    @staticmethod
    def encode_dynamic_data(record: object) -> Record:
        pass