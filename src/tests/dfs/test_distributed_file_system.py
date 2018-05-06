from unittest import TestCase
import os

from graph_db.engine.node import Node
from graph_db.engine.label import Label
from graph_db.fs.io_engine import IOEngine


class ParserCase(TestCase):
    temp_dir = 'temp_db/'
    def setUp(self):
        self.io_engine = IOEngine()

    def tearDown(self):
        self.io_engine.close()

        # deleting created temp stores
        # for path in os.listdir(self.temp_dir):
        #     os.remove(os.path.join(self.temp_dir, path))
        # os.removedirs(self.temp_dir)
        #
        # with self.assertRaises(FileNotFoundError):
        #     os.listdir(self.temp_dir)

    def test_queries(self):

        stats = self.io_engine.get_stats()
        print(stats)
        self.assertEqual(0, stats['NodeStorage'])

        label = Label(name="Bobbi", id=0)
        node = Node(label=label)

        self.io_engine.insert_node(node)
        self.assertEqual(1, self.io_engine.get_stats()['NodeStorage'])

        retrieved_node = self.io_engine.select_node(0)
        print(retrieved_node)
        #self.assertEqual('Bobbi', retrieved_node.get_label())
        # self.assertEqual(1, 0, ":)")
