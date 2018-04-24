from typing import Dict

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
        Node record format:
            1 byte      `in_use` byte
            4 bytes     `label_id` – pointer to record with label in label storage
            4 bytes     `first_rel_id` – pointer to record with first relationship
            4 bytes     `first_prop_id` – pointer to record with first property
        Total: 13 bytes
        :param record:  record object
        :return:        a dictionary with parsed node data
        """
        idx = record.idx
        used = bool.from_bytes(record[:1], byteorder=BYTEORDER)
        label_id = int.from_bytes(record[1:5], byteorder=BYTEORDER)
        first_rel_id = int.from_bytes(record[5:9], byteorder=BYTEORDER)
        first_prop_id = int.from_bytes(record[9:13], byteorder=BYTEORDER)

        return {'id': idx,
                'used': used,
                'label_id': label_id,
                'first_rel_id': first_rel_id,
                'first_prop_id': first_prop_id}

    @staticmethod
    def decode_relationship_record(record: Record) -> Dict[str, DB_TYPE]:
        """
        Decodes relationship data from a physical relationship record.
        Relationship record format:
            1 byte      `in_use` byte
            4 bytes     `start_node` - pointer to record with start node of the relationship
            4 bytes     `end_node` – pointer to record with end node of the relationship
            4 bytes     `label_id` – pointer to record with relationship type in label storage
            4 bytes     `start_prev_id` – pointer to record with prev relationship of start node
            4 bytes     `start_next_id` – pointer to record with next relationship of start node
            4 bytes     `end_prev_id` – pointer to record with prev relationship of end node
            4 bytes     `end_next_id` – pointer to record with next relationship of end node
            4 bytes     `first_prop_id` – pointer to record with first property
            1 byte      `is_first` byte – is this relationship first in relationship chain
        Total: 34 bytes
        :param record:  record object
        :return:        a dictionary with parsed relationship data
        """
        idx = record.idx
        used = bool.from_bytes(record[:1], byteorder=BYTEORDER)
        start_node = int.from_bytes(record[1:5], byteorder=BYTEORDER)
        end_node = int.from_bytes(record[5:9], byteorder=BYTEORDER)
        label_id = int.from_bytes(record[9:13], byteorder=BYTEORDER)
        start_prev_id = int.from_bytes(record[13:17], byteorder=BYTEORDER)
        start_next_id = int.from_bytes(record[17:21], byteorder=BYTEORDER)
        end_prev_id = int.from_bytes(record[21:25], byteorder=BYTEORDER)
        end_next_id = int.from_bytes(record[25:29], byteorder=BYTEORDER)
        first_prop_id = int.from_bytes(record[29:33], byteorder=BYTEORDER)
        is_first = bool.from_bytes(record[33:34], byteorder=BYTEORDER)

        return {'id': idx,
                'used': used,
                'start_node': start_node,
                'end_node': end_node,
                'label_id': label_id,
                'start_prev_id': start_prev_id,
                'start_next_id': start_next_id,
                'end_prev_id': end_prev_id,
                'end_next_id': end_next_id,
                'first_prop_id': first_prop_id,
                'is_first': is_first}

    @staticmethod
    def decode_label_record(record: Record) -> Dict[str, DB_TYPE]:
        """
        Decodes label data from a physical label record.
        Label record format:
            1 byte      `in_use` byte
            4 bytes     `dynamic_id` – pointer to first record with label data in dynamic storage
        Total: 5 bytes
        :param record:  record object
        :return:        a dictionary with parsed label data
        """
        idx = record.idx
        used = bool.from_bytes(record[:1], byteorder=BYTEORDER)
        dynamic_id = int.from_bytes(record[1:5], byteorder=BYTEORDER)

        return {'id': idx,
                'used': used,
                'dynamic_id': dynamic_id}

    @staticmethod
    def decode_property_record(record: Record) -> Dict[str, DB_TYPE]:
        """
        Decodes property data from a physical property record.
        Property record format:
            1 byte      `in_use` byte
            4 bytes     `key_id` – pointer to key data in dynamic storage
            4 bytes     `value_id` – pointer to value data in dynamic storage
            4 bytes     `next_prop_id` – pointer to `id` of next property
        Total: 13 bytes
        :param record:  record object
        :return:        a dictionary with parsed property data
        """
        idx = record.idx
        used = bool.from_bytes(record[:1], byteorder=BYTEORDER)
        key_id = int.from_bytes(record[1:5], byteorder=BYTEORDER)
        value_id = int.from_bytes(record[5:9], byteorder=BYTEORDER)
        next_prop_id = int.from_bytes(record[9:13], byteorder=BYTEORDER)

        return {'id': idx,
                'used': used,
                'key_id': key_id,
                'value_id': value_id,
                'next_prop_id': next_prop_id}

    @staticmethod
    def decode_dynamic_data_record(record: Record) -> Dict[str, DB_TYPE]:
        """
        Decodes dynamic data from a physical dynamic record.
        Dynamic record format:
            28 bytes    data
            4 bytes     pointer to `id` of next_chunk
        Total: 32 bytes
        :param record:  record object
        :return:        a dictionary with parsed dynamic data
        """
        idx = record.idx
        data = record[:28].decode(encoding=ENCODING)
        next_chunk_id = int.from_bytes(record[28:32], byteorder=BYTEORDER)

        return {'id': idx,
                'data': data,
                'next_chunk_id': next_chunk_id}
