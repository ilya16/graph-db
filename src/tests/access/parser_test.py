from unittest import TestCase
import os

from graph_db.access.execute import QueryExecutor
from graph_db.access.parser import Parser, InputError
from graph_db.engine.error import GraphEngineError
from graph_db.engine.graph_engine import GraphEngine


class ParserCase(TestCase):
    temp_dir = 'temp_db/'
    queries = [
        'create graph: test_graph',
        'create node: Cat',
        'create node: Mouse',
        'create relationship: catches from Cat to Mouse',
        'create node: Cat',
        'match node: Cat',
        'match node: Mouse',
        'match relationship: catches',
        'create node: Jerry Animal:Mouse',
        'create node: Tom Animal:Cat',
        'create relationship: catches from Jerry to Tom Durability:2',
        'create relationship: fights from Tom to Jerry Time:10',
        'create node: system Type:PC CPU:Intel GPU:NVidia',
        'create relationship: plays from Tom To system Since:2016 Game:MadMax',
        'match graph: test_graph',
        'create node: boy age:20 sex:male',
        'create node: girl age:19 sex:female',
        'create relationship: loves from boy to girl since:2015',
        'match node: age>19',
        'match node: sex=male',
        'match node: age<100',
        'match relationship: since=2015',
        'match node: Cats',
        'match relationship: False',
        'create node: a',
        'create node: b',
        'create relationship: ab from a to b',
        'delete relationship: id:5',
        'match relationship: ab',
        'match relationship: id:5'
    ]

    queries_invalid = [
        'create graph:test_graph',
        'create graph test_graph',
        'create graph',
        'create node:Cat',
        'create node: ',
        'create',
        'match node: ',
        'match node',
        'create relationship: catches',
        'create relationship: catches from Cat',
        'create relationship: catches from id:3 to id:4',
        'create node: Jerry Animal:',
        'create node: Tom Animal',
        'create node: Tom :Cat',
        'create relationship: catches from Jerry to Tom :2',
        'create node: system Type:PC CPU:Intel GPU:',
        'create node: system Type:PC CPU:Intel :',
        'create node: system Type:PC CPU:Intel:NVidia',
        'match graph: ',
        'match node: age>',
        'match node: =',
        'match node: <100',
        'match relationship: since= =2015',
    ]

    def setUp(self):
        self.parser = Parser()
        self.query_executor = QueryExecutor()
        self.graph_engine = GraphEngine('temp_db/')

    def tearDown(self):
        self.graph_engine.close()

        # deleting created temp stores
        for path in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, path))
        os.removedirs(self.temp_dir)

        with self.assertRaises(FileNotFoundError):
            os.listdir(self.temp_dir)

    def test_queries(self):
        # Graph creation
        func, params = self.parser.parse_query(self.queries[0])
        self.query_executor.execute(self.graph_engine, func, **params)

        # Node creation #1
        func, params = self.parser.parse_query(self.queries[1])
        self.query_executor.execute(self.graph_engine, func, **params)
        self.assertEqual(1, self.graph_engine.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph_engine.select_node(node_id=0)
        self.assertEqual(0, retrieved_node.get_id(), 'Node id is incorrect')

        label = retrieved_node.get_label()
        self.assertEqual(0, label.get_id(), 'Label id is incorrect')
        self.assertEqual('Cat', label.get_name(), 'Label name is incorrect')

        # Node creation #2
        func, params = self.parser.parse_query(self.queries[2])
        self.query_executor.execute(self.graph_engine, func, **params)
        self.assertEqual(2, self.graph_engine.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph_engine.select_node(node_id=1)
        self.assertEqual(1, retrieved_node.get_id(), 'Node id is incorrect')

        label = retrieved_node.get_label()
        self.assertEqual(1, label.get_id(), 'Label id is incorrect')
        self.assertEqual('Mouse', label.get_name(), 'Label name is incorrect')

        # Relationship creation #1
        func, params = self.parser.parse_query(self.queries[3])
        self.query_executor.execute(self.graph_engine, func, **params)
        self.assertEqual(3, self.graph_engine.get_stats()['LabelStorage'], 'Label storage contains extra data')

        retrieved_relationship = self.graph_engine.select_relationship(rel_id=0)
        self.assertEqual(0, retrieved_relationship.get_id(), 'relationship id is incorrect')

        label = retrieved_relationship.get_label()
        self.assertEqual(2, label.get_id(), 'relationship label id is incorrect')
        self.assertEqual('catches', label.get_name(), 'relationship label name is incorrect')

        # Node creation with the same 'Cat' label
        func, params = self.parser.parse_query(self.queries[4])
        self.query_executor.execute(self.graph_engine, func, **params)
        self.assertEqual(3, self.graph_engine.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph_engine.select_node(node_id=2)
        self.assertEqual(2, retrieved_node.get_id(), 'Node id is incorrect')

        label = retrieved_node.get_label()
        self.assertEqual(0, label.get_id(), 'Label id is incorrect')
        self.assertEqual(3, self.graph_engine.get_stats()['LabelStorage'], 'Label storage contains extra data')
        self.assertEqual('Cat', label.get_name(), 'Label name is incorrect')

        # Match nodes with 'Cat' label
        func, params = self.parser.parse_query(self.queries[5])
        result = self.query_executor.execute(self.graph_engine, func, **params)
        self.assertIsInstance(result, list)
        self.assertEqual(2, len(result), 'Number of nodes with label is incorrect')

        # Match 'Mouse' node
        func, params = self.parser.parse_query(self.queries[6])
        result = self.query_executor.execute(self.graph_engine, func, **params)
        self.assertIsInstance(result, list)
        self.assertEqual(1, len(result), 'Number of nodes with label is incorrect')

        label = result[0].get_label()
        self.assertEqual('Mouse', label.get_name(), 'Label of matched node is incorrect')

        # Match 'catches' relationship
        func, params = self.parser.parse_query(self.queries[7])
        result = self.query_executor.execute(self.graph_engine, func, **params)
        self.assertIsInstance(result, list)
        self.assertEqual(1, len(result), 'Number of relationships with label is incorrect')

        label = result[0].get_label()
        self.assertEqual('catches', label.get_name(), 'Label of matched relationship is incorrect')

        # Create nodes with property
        func, params = self.parser.parse_query(self.queries[8])
        self.query_executor.execute(self.graph_engine, func, **params)
        retrieved_node = self.graph_engine.select_node(node_id=3)
        self.assertEqual(3, retrieved_node.get_id(), 'Node id is incorrect')
        prop = retrieved_node.get_first_property()
        self.assertEqual(prop._key, 'Animal', 'Key of property is incorrect')
        self.assertEqual(prop._value, 'Mouse', 'Value of property is incorrect')

        func, params = self.parser.parse_query(self.queries[9])
        self.query_executor.execute(self.graph_engine, func, **params)
        retrieved_node = self.graph_engine.select_node(node_id=4)
        self.assertEqual(4, retrieved_node.get_id(), 'Node id is incorrect')
        prop = retrieved_node.get_first_property()
        self.assertEqual(prop._key, 'Animal', 'Key of property is incorrect')
        self.assertEqual(prop._value, 'Cat', 'Value of property is incorrect')

        # Create relationships with property
        func, params = self.parser.parse_query(self.queries[10])
        self.query_executor.execute(self.graph_engine, func, **params)
        retrieved_relationship = self.graph_engine.select_relationship(rel_id=1)
        self.assertEqual(1, retrieved_relationship.get_id(), 'relationship id is incorrect')
        prop = retrieved_relationship.get_first_property()
        self.assertEqual(prop._key, 'Durability', 'Key of property is incorrect')
        self.assertEqual(prop._value, '2', 'Value of property is incorrect')

        func, params = self.parser.parse_query(self.queries[11])
        self.query_executor.execute(self.graph_engine, func, **params)
        retrieved_relationship = self.graph_engine.select_relationship(rel_id=2)
        self.assertEqual(2, retrieved_relationship.get_id(), 'relationship id is incorrect')
        prop = retrieved_relationship.get_first_property()
        self.assertEqual(prop._key, 'Time', 'Key of property is incorrect')
        self.assertEqual(prop._value, '10', 'Value of property is incorrect')

        # Create a node with multiple properties
        func, params = self.parser.parse_query(self.queries[12])
        self.query_executor.execute(self.graph_engine, func, **params)
        retrieved_node = self.graph_engine.select_node(node_id=5)
        self.assertEqual(3, len(retrieved_node.get_properties()), 'Number of properties is incorrect')
        self.assertEqual('CPU', retrieved_node.get_properties()[1].get_key(), 'Retrieved key is incorrect')
        self.assertEqual('NVidia', retrieved_node.get_properties()[2].get_value(), 'Retrieved value is incorrect')

        # Create an relationship with multiple properties
        func, params = self.parser.parse_query(self.queries[13])
        self.query_executor.execute(self.graph_engine, func, **params)
        retrieved_relationship = self.graph_engine.select_relationship(rel_id=3)
        self.assertEqual(2, len(retrieved_relationship.get_properties()), 'Number of properties is incorrect')
        self.assertEqual('MadMax', retrieved_relationship.get_properties()[1].get_value(), 'Retrieved value is incorrect')
        self.assertEqual(9, self.graph_engine.get_stats()['PropertyStorage'], 'Incorrect number of properties')

        # Graph traverse with match graph: graph
        func, params = self.parser.parse_query(self.queries[14])
        result = self.query_executor.execute(self.graph_engine, func, **params)
        self.assertIsInstance(result, list)
        self.assertEqual(10, len(result), 'Number of objects in graph is incorrect')

        # Create 2 nodes and 1 relationship with properties to match
        func, params = self.parser.parse_query(self.queries[15])
        self.query_executor.execute(self.graph_engine, func, **params)
        func, params = self.parser.parse_query(self.queries[16])
        self.query_executor.execute(self.graph_engine, func, **params)
        func, params = self.parser.parse_query(self.queries[17])
        self.query_executor.execute(self.graph_engine, func, **params)

        # Match nodes by property
        func, params = self.parser.parse_query(self.queries[18])
        result = self.query_executor.execute(self.graph_engine, func, **params)
        self.assertIsInstance(result, list)
        self.assertEqual(1, len(result), 'Incorrect number of matched nodes')
        self.assertEqual(20, int(result[0].get_first_property().get_value()),
                         'Retrieved value of property is incorrect')

        # Node
        func, params = self.parser.parse_query(self.queries[19])
        result = self.query_executor.execute(self.graph_engine, func, **params)
        self.assertIsInstance(result, list)
        self.assertEqual(1, len(result), 'Incorrect number of matched nodes')

        # Node
        func, params = self.parser.parse_query(self.queries[20])
        result = self.query_executor.execute(self.graph_engine, func, **params)
        self.assertIsInstance(result, list)
        self.assertEqual(2, len(result), 'Incorrect number of matched nodes')

        # Relationship
        func, params = self.parser.parse_query(self.queries[21])
        result = self.query_executor.execute(self.graph_engine, func, **params)
        self.assertIsInstance(result, list)
        self.assertEqual(1, len(result), 'Incorrect number of matched relationships')

        # Unexisting objects
        func, params = self.parser.parse_query(self.queries[22])
        result = self.query_executor.execute(self.graph_engine, func, **params)
        self.assertIsInstance(result, list)
        self.assertEqual(0, len(result), 'Incorrect number of matched nodes')

        func, params = self.parser.parse_query(self.queries[23])
        result = self.query_executor.execute(self.graph_engine, func, **params)
        self.assertIsInstance(result, list)
        self.assertEqual(0, len(result), 'Incorrect number of matched nodes')

        # Delete relationship
        func, params = self.parser.parse_query(self.queries[24])
        self.query_executor.execute(self.graph_engine, func, **params)
        func, params = self.parser.parse_query(self.queries[25])
        self.query_executor.execute(self.graph_engine, func, **params)
        func, params = self.parser.parse_query(self.queries[26])
        self.query_executor.execute(self.graph_engine, func, **params)
        func, params = self.parser.parse_query(self.queries[27])
        self.query_executor.execute(self.graph_engine, func, **params)
        func, params = self.parser.parse_query(self.queries[28])
        result = self.query_executor.execute(self.graph_engine, func, **params)
        self.assertEqual(0, len(result), 'Relationship was not deleted')
        func, params = self.parser.parse_query(self.queries[29])
        result = self.query_executor.execute(self.graph_engine, func, **params)
        self.assertIsInstance(result, None)
        # self.assertEqual(0, len(result), 'Relationship was not deleted')

    def test_queries_invalid(self):
        # Graph creation
        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[0])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[1])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[2])

        func, params = self.parser.parse_query(self.queries[0])
        self.query_executor.execute(self.graph_engine, func, **params)

        # Node creation
        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[3])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[4])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[5])

        # Node match
        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[6])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[7])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[8])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[9])

        with self.assertRaises(GraphEngineError):
            func, params = self.parser.parse_query(self.queries_invalid[10])
            self.query_executor.execute(self.graph_engine, func, **params)

        # Create nodes with property
        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[11])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[12])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[13])

        # Create relationships with property
        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[14])

        # Create a node with multiple properties
        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[15])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[16])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[17])

        # Graph traverse with match graph: graph
        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[18])

        # Match nodes by property
        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[19])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[20])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[21])

        with self.assertRaises(InputError):
            self.parser.parse_query(self.queries_invalid[22])
