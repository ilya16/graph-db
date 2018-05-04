from unittest import TestCase
import os

from graph_db.access.parser import Parser


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
        'match relationship: since=2015'
    ]

    def setUp(self):
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
        # Graph creation
        self.graph = self.parser.parse_query(None, self.queries[0])

        # Node creation #1
        self.graph = self.parser.parse_query(self.graph, self.queries[1])
        self.assertEqual(1, self.graph.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph.select_node(node_id=0)
        self.assertEqual(0, retrieved_node.get_id(), 'Node id is incorrect')

        label = retrieved_node.get_label()
        self.assertEqual(0, label.get_id(), 'Label id is incorrect')
        self.assertEqual('Cat', label.get_name(), 'Label name is incorrect')

        # Node creation #2
        self.graph = self.parser.parse_query(self.graph, self.queries[2])
        self.assertEqual(2, self.graph.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph.select_node(node_id=1)
        self.assertEqual(1, retrieved_node.get_id(), 'Node id is incorrect')

        label = retrieved_node.get_label()
        self.assertEqual(1, label.get_id(), 'Label id is incorrect')
        self.assertEqual('Mouse', label.get_name(), 'Label name is incorrect')

        # Relationship creation #1
        self.graph = self.parser.parse_query(self.graph, self.queries[3])
        self.assertEqual(3, self.graph.get_stats()['LabelStorage'], 'Label storage contains extra data')

        retrieved_relationship = self.graph.select_relationship(rel_id=0)
        self.assertEqual(0, retrieved_relationship.get_id(), 'relationship id is incorrect')

        label = retrieved_relationship.get_label()
        self.assertEqual(2, label.get_id(), 'relationship label id is incorrect')
        self.assertEqual('catches', label.get_name(), 'relationship label name is incorrect')

        # Node creation with the same 'Cat' label
        self.graph = self.parser.parse_query(self.graph, self.queries[4])
        self.assertEqual(3, self.graph.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph.select_node(node_id=2)
        self.assertEqual(2, retrieved_node.get_id(), 'Node id is incorrect')

        label = retrieved_node.get_label()
        self.assertEqual(0, label.get_id(), 'Label id is incorrect')
        self.assertEqual(3, self.graph.get_stats()['LabelStorage'], 'Label storage contains extra data')
        self.assertEqual('Cat', label.get_name(), 'Label name is incorrect')

        # Match nodes with 'Cat' label
        retrieved_object = self.parser.parse_query(self.graph, self.queries[5])
        self.assertEqual(2, len(retrieved_object), 'Number of nodes with label is incorrect')

        # Match 'Mouse' node
        retrieved_object = self.parser.parse_query(self.graph, self.queries[6])
        self.assertEqual(1, len(retrieved_object), 'Number of nodes with label is incorrect')

        label = retrieved_object[0].get_label()
        self.assertEqual('Mouse', label.get_name(), 'Label of matched node is incorrect')

        # Match 'catches' relationship
        retrieved_object = self.parser.parse_query(self.graph, self.queries[7])
        self.assertEqual(1, len(retrieved_object), 'Number of relationships with label is incorrect')

        label = retrieved_object[0].get_label()
        self.assertEqual('catches', label.get_name(), 'Label of matched relationship is incorrect')

        # Create nodes with property
        self.graph = self.parser.parse_query(self.graph, self.queries[8])
        retrieved_node = self.graph.select_node(node_id=3)
        self.assertEqual(3, retrieved_node.get_id(), 'Node id is incorrect')
        prop = retrieved_node.get_first_property()
        self.assertEqual(prop._key, 'Animal', 'Key of property is incorrect')
        self.assertEqual(prop._value, 'Mouse', 'Value of property is incorrect')

        self.graph = self.parser.parse_query(self.graph, self.queries[9])
        retrieved_node = self.graph.select_node(node_id=4)
        self.assertEqual(4, retrieved_node.get_id(), 'Node id is incorrect')
        prop = retrieved_node.get_first_property()
        self.assertEqual(prop._key, 'Animal', 'Key of property is incorrect')
        self.assertEqual(prop._value, 'Cat', 'Value of property is incorrect')

        # Create relationships with property
        self.graph = self.parser.parse_query(self.graph, self.queries[10])
        retrieved_relationship = self.graph.select_relationship(rel_id=1)
        self.assertEqual(1, retrieved_relationship.get_id(), 'relationship id is incorrect')
        prop = retrieved_relationship.get_first_property()
        self.assertEqual(prop._key, 'Durability', 'Key of property is incorrect')
        self.assertEqual(prop._value, '2', 'Value of property is incorrect')

        self.graph = self.parser.parse_query(self.graph, self.queries[11])
        retrieved_relationship = self.graph.select_relationship(rel_id=2)
        self.assertEqual(2, retrieved_relationship.get_id(), 'relationship id is incorrect')
        prop = retrieved_relationship.get_first_property()
        self.assertEqual(prop._key, 'Time', 'Key of property is incorrect')
        self.assertEqual(prop._value, '10', 'Value of property is incorrect')

        # Create a node with multiple properties
        self.graph = self.parser.parse_query(self.graph, self.queries[12])
        retrieved_node = self.graph.select_node(node_id=5)
        self.assertEqual(3, len(retrieved_node.get_properties()), 'Number of properties is incorrect')
        self.assertEqual('CPU', retrieved_node.get_properties()[1].get_key(), 'Retrieved key is incorrect')
        self.assertEqual('NVidia', retrieved_node.get_properties()[2].get_value(), 'Retrieved value is incorrect')

        # Create an relationship with multiple properties
        self.graph = self.parser.parse_query(self.graph, self.queries[13])
        retrieved_relationship = self.graph.select_relationship(rel_id=3)
        self.assertEqual(2, len(retrieved_relationship.get_properties()), 'Number of properties is incorrect')
        self.assertEqual('MadMax', retrieved_relationship.get_properties()[1].get_value(), 'Retrieved value is incorrect')
        self.assertEqual(9, self.graph.io_engine.get_stats()['PropertyStorage'], 'Incorrect number of properties')

        # Graph traverse with match graph: graph
        objects = self.parser.parse_query(self.graph, self.queries[14])
        self.assertEqual(10, len(objects), 'Number of objects in graph is incorrect')

        # Create 2 nodes and 1 relationship with properties to match
        self.graph = self.parser.parse_query(self.graph, self.queries[15])
        self.graph = self.parser.parse_query(self.graph, self.queries[16])
        self.graph = self.parser.parse_query(self.graph, self.queries[17])

        # Match nodes by property
        retrieved_objects = self.parser.parse_query(self.graph, self.queries[18])
        self.assertEqual(1, len(retrieved_objects), 'Incorrect number of matched nodes')
        self.assertEqual(20, int(retrieved_objects[0].get_first_property().get_value()),
                         'Retrieved value of property is incorrect')

        # Node
        retrieved_objects = self.parser.parse_query(self.graph, self.queries[19])
        self.assertEqual(1, len(retrieved_objects), 'Incorrect number of matched nodes')

        # Node
        retrieved_objects = self.parser.parse_query(self.graph, self.queries[20])
        self.assertEqual(2, len(retrieved_objects), 'Incorrect number of matched nodes')

        # Relationship
        retrieved_objects = self.parser.parse_query(self.graph, self.queries[21])
        self.assertEqual(1, len(retrieved_objects), 'Incorrect number of matched relationships')
