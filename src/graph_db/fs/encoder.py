from typing import List

from graph_db.engine.label import Label
from graph_db.engine.node import Node
from graph_db.engine.relationship import Relationship
from graph_db.engine.types import *
from .record import Record


class RecordEncoder:
    @staticmethod
    def encode_node(node: Node) -> Record:
        """
        Encodes node object into a physical node record.
        Node record format:
            1 byte      `in_use` byte
            4 bytes     `label_id` – pointer to record with label in label storage
            4 bytes     `first_rel_id` – pointer to record with first relationship
            4 bytes     `first_prop_id` – pointer to record with first property
        Total: 13 bytes
        :param node:    node object
        :return:        encoded node record
        """
        node_bytes = RecordEncoder._encode_bool(node.is_used())

        label_id = node.get_label().get_id()
        assert label_id >= 0
        node_bytes += RecordEncoder._encode_int(label_id)

        first_rel_id = node.get_first_relationship().get_id() if node.get_first_relationship() else INVALID_ID
        node_bytes += RecordEncoder._encode_int(first_rel_id)

        first_prop_id = node.get_first_property().get_id() if node.get_first_property() else INVALID_ID
        node_bytes += RecordEncoder._encode_int(first_prop_id)

        record = Record(node_bytes, node.get_id())

        assert record.size == NODE_RECORD_SIZE

        return record

    @staticmethod
    def encode_relationship(rel: Relationship) -> Record:
        """
        Encodes node object into a physical node record.
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
        :param rel:     relationship object
        :return:        encoded node record
        """
        rel_bytes = RecordEncoder._encode_bool(rel.is_used())

        rel_bytes += RecordEncoder._encode_int(rel.get_start_node().get_id())
        rel_bytes += RecordEncoder._encode_int(rel.get_end_node().get_id())

        label_id = rel.get_label().get_id()
        assert label_id >= 0
        rel_bytes += RecordEncoder._encode_int(label_id)

        start_prev_id = rel.get_start_prev_rel().get_id() if rel.get_start_prev_rel() else INVALID_ID
        rel_bytes += RecordEncoder._encode_int(start_prev_id)

        start_next_id = rel.get_start_next_rel().get_id() if rel.get_start_next_rel() else INVALID_ID
        rel_bytes += RecordEncoder._encode_int(start_next_id)

        end_prev_id = rel.get_end_prev_rel().get_id() if rel.get_end_prev_rel() else INVALID_ID
        rel_bytes += RecordEncoder._encode_int(end_prev_id)

        end_next_id = rel.get_end_next_rel().get_id() if rel.get_end_next_rel() else INVALID_ID
        rel_bytes += RecordEncoder._encode_int(end_next_id)

        first_prop_id = rel.get_first_property().get_id() if rel.get_first_property() else INVALID_ID
        rel_bytes += RecordEncoder._encode_int(first_prop_id)

        record = Record(rel_bytes, rel.get_id())

        assert record.size == RELATIONSHIP_RECORD_SIZE

        return record

    @staticmethod
    def encode_label(label: Label, dynamic_id: int) -> Record:
        """
        Encodes label object into a physical label record.
        Label record format:
            1 byte      `in_use` byte
            4 bytes     `dynamic_id` – pointer to first record with label data in dynamic storage
        Total: 5 bytes
        :param label:       label object
        :param dynamic_id   `id` of first data chunk in dynamic storage
        :return:            encoded label record
        """
        label_bytes = RecordEncoder._encode_bool(label.is_used())

        assert dynamic_id >= 0
        label_bytes += RecordEncoder._encode_int(dynamic_id)

        record = Record(label_bytes, label.get_id())

        assert record.size == LABEL_RECORD_SIZE

        return record

    @staticmethod
    def encode_property(prop_id: int, used: bool, key_id: int, value_id: int, next_prop_id: int = INVALID_ID) -> Record:
        """
        Encodes property into a physical property record.
        Property record format:
            1 byte      `in_use` byte
            4 bytes     `key_id` – pointer to key data in dynamic storage
            4 bytes     `value_id` – pointer to value data in dynamic storage
            4 bytes     `next_prop_id` – pointer to `id` of next property
        Total: 13 bytes
        :param prop_id      id of the property
        :param used:        is property used or not
        :param key_id       property `key_id` in dynamic storage
        :param value_id     property `value_id` in dynamic storage
        :param next_prop_id `id` of next property of a node or relationship
                            -1 if no other property exist
        :return:            encoded property object
        """
        property_bytes = RecordEncoder._encode_bool(used)

        assert key_id >= 0
        property_bytes += RecordEncoder._encode_int(key_id)

        assert value_id >= 0
        property_bytes += RecordEncoder._encode_int(value_id)

        property_bytes += RecordEncoder._encode_int(next_prop_id)

        record = Record(property_bytes, prop_id)

        assert record.size == PROPERTY_RECORD_SIZE

        return record

    @staticmethod
    def encode_dynamic_data(data: DB_TYPE,
                            first_record_id: int,
                            payload_size: int = DYNAMIC_RECORD_PAYLOAD_SIZE) -> List[Record]:
        """
        Encodes data object of any supported type into a list of physical records.
        Dynamic record format:
            1 byte      number of bytes taken by data
            27 bytes    data
            4 bytes     pointer to `id` of next_chunk
        Total: 32 bytes
        If data cannot fit into one dynamic record, it is divided into multiple records
        which are connected using `next_chunk_id`
        :param data:            object to be encoded
        :param first_record_id  `id` of first chunk of data in dynamic storage
        :param payload_size     size of data in one dynamic record
        :return:                list of encoded dynamic records
        """
        records = []
        data = str(data)

        data_bytes = RecordEncoder._encode_str(data)

        n_records = len(data_bytes) // payload_size
        if len(data_bytes) % payload_size != 0:
            n_records += 1

        for i in range(n_records):
            record_bytes = data_bytes[i * payload_size:(i+1) * payload_size]

            data_size = len(record_bytes)
            if data_size != payload_size:
                record_bytes += b'0' * (payload_size - data_size)

            record_bytes = RecordEncoder._encode_int(data_size, n_bytes=1) + record_bytes

            next_chunk_id = first_record_id + i + 1 if i < n_records - 1 else INVALID_ID

            record_bytes += RecordEncoder._encode_int(next_chunk_id)
            records.append(Record(record_bytes, first_record_id + i))

        assert all(r.size == DYNAMIC_RECORD_SIZE for r in records)

        return records

    @staticmethod
    def _encode_int(value: int, n_bytes: int = 4) -> bytes:
        return value.to_bytes(n_bytes, byteorder=BYTEORDER, signed=SIGNED)

    @staticmethod
    def _encode_bool(value: bool, n_bytes: int = 1) -> bytes:
        return value.to_bytes(n_bytes, byteorder=BYTEORDER, signed=SIGNED)

    @staticmethod
    def _encode_str(value: str) -> bytes:
        return value.encode(encoding=ENCODING)
