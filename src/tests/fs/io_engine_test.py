from unittest import TestCase
import os

from graph_db.engine.label import Label
from graph_db.engine.node import Node
from graph_db.fs.io_engine import IOEngine
from graph_db.fs.worker import Worker


class IOEngineCase(TestCase):
    temp_dir = 'temp_db/'

    def setUp(self):
        self.io_engine = IOEngine()

        temp_storage = Worker(base_path=self.temp_dir)
        self.io_engine.add_worker(temp_storage)

    def tearDown(self):
        self.io_engine.close()

        # deleting created temp stores
        for path in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, path))
        os.removedirs(self.temp_dir)

        with self.assertRaises(FileNotFoundError):
            os.listdir(self.temp_dir)

    def test_writes(self):
        label = Label('test')
        node = Node(label)

        node = self.io_engine.insert_node(node)
        self.assertEqual(1, self.io_engine.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.io_engine.select_node(node.get_id())

        self.assertEqual(retrieved_node.get_id(), node.get_id(), 'Node ids have changed')
        self.assertEqual(0, retrieved_node.get_label().get_id(), 'Label id is incorrect')
        self.assertEqual('test', retrieved_node.get_label().get_name(), 'Label name is incorrect')
