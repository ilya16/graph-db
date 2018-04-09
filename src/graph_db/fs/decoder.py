from typing import Dict, List

from graph_db.engine.label import Label
from graph_db.engine.node import Node
from graph_db.engine.relationship import Relationship
from graph_db.engine.types import *
from .record import Record


class RecordDecoder:
    def __init__(self):
        pass

    @staticmethod
    def decode_node(record: Record) -> Node:
        """
        Decodes node object from a physical node record.
        Node record format:
            1 byte      `is_used` byte
            4 bytes     pointer to `id` of node label
            4 bytes     pointer to `id` of first relationship
            4 bytes     pointer to `id` of first property
        Total: 13 bytes
        :param record:  record object
        :return:
        """
        id = record.idx
        used = bool.from_bytes(record[:1], byteorder=BYTEORDER)
        label_id = int.from_bytes(record[1:5], byteorder=BYTEORDER)
        first_prop_id = int.from_bytes(record[5:9], byteorder=BYTEORDER)
        first_rel_id = int.from_bytes(record[9:13], byteorder=BYTEORDER)

        return Node(id, used=used, label_id=label_id, first_prop_id=first_prop_id, first_rel_id=first_rel_id)

    @staticmethod
    def decode_relationship(record: Record) -> Relationship:
        pass

    @staticmethod
    def decode_label(record: Record) -> Label:
        pass

    @staticmethod
    def decode_property(record: Record) -> Dict[object, object]:
        pass

    @staticmethod
    def decode_dynamic_data(record: List[Record]) -> object:
        pass
