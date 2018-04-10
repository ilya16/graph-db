from typing import Dict, List

from graph_db.engine.label import Label
from graph_db.engine.relationship import Relationship
from graph_db.engine.types import *
from graph_db.engine.types import DB_TYPE
from .record import Record


class RecordDecoder:
    def __init__(self):
        pass

    @staticmethod
    def decode_node_record(record: Record) -> Dict[str, DB_TYPE]:
        """
        Decodes node data from a physical node record.
        Returns a dictionary with
        Node record format:
            1 byte      `is_used` byte
            4 bytes     pointer to `id` of node label
            4 bytes     pointer to `id` of first relationship
            4 bytes     pointer to `id` of first property
        Total: 13 bytes
        :param record:  record object
        :return:        a dictionary with parsed data
        """
        id = record.idx
        used = bool.from_bytes(record[:1], byteorder=BYTEORDER)
        label_id = int.from_bytes(record[1:5], byteorder=BYTEORDER)
        first_prop_id = int.from_bytes(record[5:9], byteorder=BYTEORDER)
        first_rel_id = int.from_bytes(record[9:13], byteorder=BYTEORDER)

        return {'id': id,
                'used': used,
                'label_id': label_id,
                'first_prop_id': first_prop_id,
                'first_rel_id': first_rel_id}

    @staticmethod
    def decode_relationship_record(record: Record) -> Dict[str, DB_TYPE]:
        pass

    @staticmethod
    def decode_label_record(record: Record) -> Dict[str, DB_TYPE]:
        pass

    @staticmethod
    def decode_property_record(record: Record) -> Dict[str, DB_TYPE]:
        pass

    @staticmethod
    def decode_dynamic_data_record(record: List[Record]) -> Dict[str, DB_TYPE]:
        pass
