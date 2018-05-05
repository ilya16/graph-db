from unittest import TestCase
import os


from graph_db.fs.io_engine import IOEngine


class ParserCase(TestCase):
    temp_dir = 'temp_db/'
    def setUp(self):
        self.io_engine = IOEngine()

    def tearDown(self):
        self.io_engine.stut_down()

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
        print("hello")

