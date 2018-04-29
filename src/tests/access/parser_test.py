from unittest import TestCase
import os

from graph_db.engine.graph import Graph
from graph_db.access.parser import Parser


class ParserCase(TestCase):
    temp_dir = 'temp_db/'
    queries = [
            'CREATE node: Warrior',
            'CREATE node: Node2',
            'CREATE edge: e FROM Warrior TO Node2',
            'CREATE node: Warrior',
            'MATCH node: Warrior',
            'MATCH node: Node2',
            'MATCH edge: e',
            'CREATE node: Jack Animal:Dog',
            'CREATE node: Tom Animal:Cat',
            'CREATE edge: catches FROM Jack TO Tom Durability:2',
            'CREATE edge: fights FROM Tom TO Jack Time:10'
    ]

    def setUp(self):
        self.graph = Graph('test_graph', temp_dir=self.temp_dir)
        self.parser = Parser()

    def tearDown(self):
        self.graph.close_engine()

        # deleting created temp stores
        for path in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, path))
        os.removedirs(self.temp_dir)

        with self.assertRaises(FileNotFoundError):
            os.listdir(self.temp_dir)

    def test_queries(self):
        # Node creation #1
        self.graph = self.parser.parse_query(self.graph, self.queries[0])
        self.assertEqual(1, self.graph.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph.select_nth_node(0)
        self.assertEqual(0, retrieved_node.get_id(), 'Node id is incorrect')

        label = retrieved_node.get_label()
        self.assertEqual(0, label.get_id(), 'Label id is incorrect')
        self.assertEqual('Warrior', label.get_name(), 'Label name is incorrect')

        # Node creation #2
        self.graph = self.parser.parse_query(self.graph, self.queries[1])
        self.assertEqual(2, self.graph.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph.select_nth_node(1)
        self.assertEqual(1, retrieved_node.get_id(), 'Node id is incorrect')

        label = retrieved_node.get_label()
        self.assertEqual(1, label.get_id(), 'Label id is incorrect')
        self.assertEqual('Node2', label.get_name(), 'Label name is incorrect')

        # Edge creation #1
        self.graph = self.parser.parse_query(self.graph, self.queries[2])
        self.assertEqual(3, self.graph.get_stats()['LabelStorage'], 'Label storage contains extra data')

        retrieved_edge = self.graph.select_nth_edge(0)
        self.assertEqual(0, retrieved_edge.get_id(), 'Edge id is incorrect')

        label = retrieved_edge.get_label()
        self.assertEqual(2, label.get_id(), 'Edge label id is incorrect')
        self.assertEqual('e', label.get_name(), 'Edge label name is incorrect')

        # Node creation with the same 'Warrior' label
        self.graph = self.parser.parse_query(self.graph, self.queries[3])
        self.assertEqual(3, self.graph.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph.select_nth_node(2)
        self.assertEqual(2, retrieved_node.get_id(), 'Node id is incorrect')

        label = retrieved_node.get_label()
        self.assertEqual(0, label.get_id(), 'Label id is incorrect')
        self.assertEqual(3, self.graph.get_stats()['LabelStorage'], 'Label storage contains extra data')
        self.assertEqual('Warrior', label.get_name(), 'Label name is incorrect')

        # Match nodes with 'Warrior' label
        retrieved_object = self.parser.parse_query(self.graph, self.queries[4])
        self.assertEqual(2, len(retrieved_object), 'Number of nodes with label is incorrect')

        # Match 'Node2' node
        retrieved_object = self.parser.parse_query(self.graph, self.queries[5])
        self.assertEqual(1, len(retrieved_object), 'Number of nodes with label is incorrect')

        label = retrieved_object[0].get_label()
        self.assertEqual('Node2', label.get_name(), 'Label of matched node is incorrect')

        # Match 'e' edge
        retrieved_object = self.parser.parse_query(self.graph, self.queries[6])
        self.assertEqual(1, len(retrieved_object), 'Number of edges with label is incorrect')

        label = retrieved_object[0].get_label()
        self.assertEqual('e', label.get_name(), 'Label of matched edge is incorrect')

        # Create nodes with property
        self.graph = self.parser.parse_query(self.graph, self.queries[7])
        retrieved_node = self.graph.select_nth_node(3)
        self.assertEqual(3, retrieved_node.get_id(), 'Node id is incorrect')
        prop = retrieved_node.get_first_property()
        self.assertEqual(prop._key, 'Animal', 'Key of property is incorrect')
        self.assertEqual(prop._value, 'Dog', 'Value of property is incorrect')

        self.graph = self.parser.parse_query(self.graph, self.queries[8])
        retrieved_node = self.graph.select_nth_node(4)
        self.assertEqual(4, retrieved_node.get_id(), 'Node id is incorrect')
        prop = retrieved_node.get_first_property()
        self.assertEqual(prop._key, 'Animal', 'Key of property is incorrect')
        self.assertEqual(prop._value, 'Cat', 'Value of property is incorrect')

        # Create edges with property
        self.graph = self.parser.parse_query(self.graph, self.queries[9])
        retrieved_edge = self.graph.select_nth_edge(1)
        self.assertEqual(1, retrieved_edge.get_id(), 'Edge id is incorrect')
        prop = retrieved_edge.get_first_property()
        self.assertEqual(prop._key, 'Durability', 'Key of property is incorrect')
        self.assertEqual(prop._value, '2', 'Value of property is incorrect')

        self.graph = self.parser.parse_query(self.graph, self.queries[10])
        retrieved_edge = self.graph.select_nth_edge(2)
        self.assertEqual(2, retrieved_edge.get_id(), 'Edge id is incorrect')
        prop = retrieved_edge.get_first_property()
        self.assertEqual(prop._key, 'Time', 'Key of property is incorrect')
        self.assertEqual(prop._value, '10', 'Value of property is incorrect')