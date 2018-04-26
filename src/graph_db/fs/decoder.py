from typing import Dict

from graph_db.engine.types import *
from graph_db.engine.types import DB_TYPE
from .record import Record


class RecordDecoder:
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
        assert idx >= 0

        used = RecordDecoder._decode_bool(record[:1])

        label_id = RecordDecoder._decode_int(record[1:5])
        assert label_id >= 0

        first_rel_id = RecordDecoder._decode_int(record[5:9])
        first_prop_id = RecordDecoder._decode_int(record[9:13])

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
        Total: 33 bytes
        :param record:  record object
        :return:        a dictionary with parsed relationship data
        """
        idx = record.idx
        assert idx >= 0

        used = RecordDecoder._decode_bool(record[:1])
        start_node = RecordDecoder._decode_int(record[1:5])
        end_node = RecordDecoder._decode_int(record[5:9])

        label_id = RecordDecoder._decode_int(record[9:13])
        assert label_id >= 0

        start_prev_id = RecordDecoder._decode_int(record[13:17])
        start_next_id = RecordDecoder._decode_int(record[17:21])
        end_prev_id = RecordDecoder._decode_int(record[21:25])
        end_next_id = RecordDecoder._decode_int(record[25:29])
        first_prop_id = RecordDecoder._decode_int(record[29:33])

        return {'id': idx,
                'used': used,
                'start_node': start_node,
                'end_node': end_node,
                'label_id': label_id,
                'start_prev_id': start_prev_id,
                'start_next_id': start_next_id,
                'end_prev_id': end_prev_id,
                'end_next_id': end_next_id,
                'first_prop_id': first_prop_id}

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
        assert idx >= 0

        used = RecordDecoder._decode_bool(record[:1])
        dynamic_id = RecordDecoder._decode_int(record[1:5])

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
        assert idx >= 0

        used = RecordDecoder._decode_bool(record[:1])

        key_id = RecordDecoder._decode_int(record[1:5])
        assert key_id >= 0

        value_id = RecordDecoder._decode_int(record[5:9])
        assert value_id >= 0

        next_prop_id = RecordDecoder._decode_int(record[9:13])

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
            1 byte      number of bytes taken by data
            27 bytes    data
            4 bytes     pointer to `id` of next_chunk
        Total: 32 bytes
        :param record:  record object
        :return:        a dictionary with parsed dynamic data
        """
        idx = record.idx
        assert idx >= 0

        data_size = RecordDecoder._decode_int(record[:1])
        data = RecordDecoder._decode_str(record[1:data_size+1])
        next_chunk_id = RecordDecoder._decode_int(record[28:32])

        return {'id': idx,
                'data': data,
                'next_chunk_id': next_chunk_id}

    @staticmethod
    def _decode_int(data: bytes, n_bytes: int = 4) -> int:
        return int.from_bytes(data[:n_bytes], byteorder=BYTEORDER)

    @staticmethod
    def _decode_bool(data: bytes, n_bytes: int = 1) -> bool:
        return bool.from_bytes(data[:n_bytes], byteorder=BYTEORDER)

    @staticmethod
    def _decode_str(data: bytes) -> str:
        return data.decode(encoding=ENCODING)
