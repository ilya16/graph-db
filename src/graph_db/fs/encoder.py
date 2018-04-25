from typing import List

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
            1 byte      `in_use` byte
            4 bytes     `label_id` – pointer to record with label in label storage
            4 bytes     `first_rel_id` – pointer to record with first relationship
            4 bytes     `first_prop_id` – pointer to record with first property
        Total: 13 bytes
        :param node:    node object
        :return:        encoded node record
        """
        node_bytes = node.is_used().to_bytes(1, byteorder=BYTEORDER, signed=SIGNED)

        label = node.get_label()
        assert label.get_id() >= 0
        node_bytes += label.get_id().to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)

        if node.get_first_relationship():
            node_bytes += node.get_first_relationship().get_id().to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)
        else:
            node_bytes += b'0' * 4

        if node.get_first_property():
            node_bytes += node.get_first_property().get_id().to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)
        else:
            node_bytes += b'0' * 4

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
            1 byte      `is_first` byte – is this relationship first in relationship chain
        Total: 34 bytes
        :param rel:     relationship object
        :return:        encoded node record
        """
        rel_bytes = rel.is_used().to_bytes(1, byteorder=BYTEORDER, signed=SIGNED)

        rel_bytes += rel.get_start_node().get_id().to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)
        rel_bytes += rel.get_end_node().get_id().to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)

        label = rel.get_label()
        assert label.get_id() >= 0
        rel_bytes += label.get_id().to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)

        if rel.get_start_prev_rel():
            rel_bytes += rel.get_start_prev_rel().get_id().to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)
        else:
            rel_bytes += b'0' * 4

        if rel.get_start_next_rel():
            rel_bytes += rel.get_start_next_rel().get_id().to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)
        else:
            rel_bytes += b'0' * 4

        if rel.get_end_prev_rel():
            rel_bytes += rel.get_end_prev_rel().get_id().to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)
        else:
            rel_bytes += b'0' * 4

        if rel.get_end_next_rel():
            rel_bytes += rel.get_end_next_rel().get_id().to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)
        else:
            rel_bytes += b'0' * 4

        if rel.get_first_property():
            rel_bytes += rel.get_first_property().get_id().to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)
        else:
            rel_bytes += b'0' * 4

        #todo
        rel_bytes += rel.is_used().to_bytes(1, byteorder=BYTEORDER, signed=SIGNED)

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
        label_bytes = label.is_used().to_bytes(1, byteorder=BYTEORDER, signed=SIGNED)

        assert dynamic_id >= 0
        label_bytes += dynamic_id.to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)

        record = Record(label_bytes, label.get_id())

        assert record.size == LABEL_RECORD_SIZE

        return record

    @staticmethod
    def encode_property(used: bool, key_id: int, value_id: int, next_prop_id: int = -1) -> Record:
        """
        Encodes property into a physical property record.
        Property record format:
            1 byte      `in_use` byte
            4 bytes     `key_id` – pointer to key data in dynamic storage
            4 bytes     `value_id` – pointer to value data in dynamic storage
            4 bytes     `next_prop_id` – pointer to `id` of next property
        Total: 13 bytes
        :param used:        is property used or not
        :param key_id       property `key_id` in dynamic storage
        :param value_id     property `value_id` in dynamic storage
        :param next_prop_id `id` of next property of a node or relationship
                            -1 if no other property exist
        :return:            encoded property object
        """
        property_bytes = used.to_bytes(1, byteorder=BYTEORDER, signed=SIGNED)

        assert key_id >= 0
        property_bytes += key_id.to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)

        assert value_id >= 0
        property_bytes += value_id.to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)

        property_bytes += next_prop_id.to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)

        record = Record(property_bytes, -1)

        assert record.size == PROPERTY_RECORD_SIZE

        return record

    @staticmethod
    def encode_dynamic_data(data: DB_TYPE,
                            first_record_id: int,
                            payload_size: int = DYNAMIC_RECORD_PAYLOAD_SIZE) -> List[Record]:
        """
        Encodes data object of any supported type into a list of physical records.
        Dynamic record format:
            28 bytes    data
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

        data_bytes = data.encode(encoding=ENCODING)

        data_bytes += b'0' * (payload_size - (len(data_bytes) % payload_size))

        assert len(data_bytes) % payload_size == 0

        n_records = len(data_bytes) // payload_size
        for i in range(n_records):
            record_bytes = data_bytes[i * payload_size:(i+1) * payload_size]
            if i < n_records - 1:
                record_bytes += (first_record_id + i + 1).to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)
            else:
                record_bytes += int(-1).to_bytes(4, byteorder=BYTEORDER, signed=SIGNED)
            records.append(Record(record_bytes, 0))

        assert all(r.size == DYNAMIC_RECORD_SIZE for r in records)

        return records
