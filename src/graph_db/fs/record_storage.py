import io
import os
import abc

from .connection import open_file
from .record import Record


class RecordStorage(metaclass=abc.ABCMeta):
    """
    Physical record storage interface.
    """

    def __init__(self, record_size: int, path: str, offset: int = 0):
        """
        Initializes a record storage
        :param record_size:     size of one record.
        """
        self.record_size = record_size
        self.path: str = path
        self.file: io.BufferedIOBase = open_file(self.path, record_size)
        self.records = self.storage_size() // self.record_size
        self.offset = offset
        self.record_idx = 0
        self._init_base()

    def storage_size(self) -> int:
        if isinstance(self.file, io.BytesIO):
            return len(self.file.getbuffer())
        else:
            return os.stat(self.file.fileno()).st_size

    def _init_base(self) -> None:
        size = self.storage_size()

        if size % self.record_size != 0:
            raise ValueError(f'File {self.path} size is not a multiple of {self.record_size}')

    def read_record(self, index: int) -> Record:
        """
        Reads physical record at index `index` from the storage.
        :param index:           record index starting from 0.
        :return:                record object.
        :raise AssertionError:  if record does not exist.
        """
        assert index < self.records, f'Record {index} does not exist'

        self.file.seek(index * self.record_size)
        return Record(self.file.read(self.record_size), index)

    def write_record(self, record: Record) -> None:
        """
        Writes physical record at index `index` into the storage, replacing old data in-place.
        :param record:          record object.
        :raise AssertionError:  if block does not exist.
        """
        assert record.idx < self.records, f'Block {record.idx} does not exist'

        self.file.seek(record.idx * self.record_size)
        self.file.write(bytearray(record))

    def allocate_record(self) -> Record:
        """
        Allocates bytes for a new record at the end of the storage.
        :return:                new record object.
        """
        record = Record.empty(self.records, self.record_idx)
        self.record_idx += 1
        self.records += 1
        self.write_record(record)
        return record

    def count_records(self) -> int:
        """
        Returns total number of physical records present in the storage
        including unused records.
        :return:                non-negative number of blocks.
        """
        return self.records

    def close(self) -> None:
        self.file.flush()
        self.file.close()
