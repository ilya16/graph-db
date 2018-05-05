from unittest import TestCase
import os

from graph_db.access import db
from graph_db.access.parser import InputError
from graph_db.engine.error import GraphEngineError


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
        'match relationship: id:5',
        'create relationship: catches from id:17 to id:18',
        'update node: id:3 color:brown'
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
        'create relationship: catches from to id:4',
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
        self.db = db.connect('temp_db/')
        self.cursor = self.db.cursor()
        self.graph_engine = self.db.get_engine()

    def tearDown(self):
        self.db.close()

        # deleting created temp stores
        for path in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, path))
        os.removedirs(self.temp_dir)

        with self.assertRaises(FileNotFoundError):
            os.listdir(self.temp_dir)

    def test_queries(self):
        # Graph creation
        self.cursor.execute(self.queries[0])

        # Node creation #1
        self.cursor.execute(self.queries[1])
        self.assertEqual(1, self.db.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph_engine.select_node(node_id=0)
        self.assertEqual(0, retrieved_node.get_id(), 'Node id is incorrect')

        label = retrieved_node.get_label()
        self.assertEqual(0, label.get_id(), 'Label id is incorrect')
        self.assertEqual('Cat', label.get_name(), 'Label name is incorrect')

        # Node creation #2
        self.cursor.execute(self.queries[2])
        self.assertEqual(2, self.db.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph_engine.select_node(node_id=1)
        self.assertEqual(1, retrieved_node.get_id(), 'Node id is incorrect')

        label = retrieved_node.get_label()
        self.assertEqual(1, label.get_id(), 'Label id is incorrect')
        self.assertEqual('Mouse', label.get_name(), 'Label name is incorrect')

        # Relationship creation #1
        self.cursor.execute(self.queries[3])
        self.assertEqual(3, self.db.get_stats()['LabelStorage'], 'Label storage contains extra data')

        retrieved_relationship = self.graph_engine.select_relationship(rel_id=0)
        self.assertEqual(0, retrieved_relationship.get_id(), 'relationship id is incorrect')

        label = retrieved_relationship.get_label()
        self.assertEqual(2, label.get_id(), 'relationship label id is incorrect')
        self.assertEqual('catches', label.get_name(), 'relationship label name is incorrect')

        # Node creation with the same 'Cat' label
        self.cursor.execute(self.queries[4])
        self.assertEqual(3, self.db.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph_engine.select_node(node_id=2)
        self.assertEqual(2, retrieved_node.get_id(), 'Node id is incorrect')

        label = retrieved_node.get_label()
        self.assertEqual(0, label.get_id(), 'Label id is incorrect')
        self.assertEqual(3, self.db.get_stats()['LabelStorage'], 'Label storage contains extra data')
        self.assertEqual('Cat', label.get_name(), 'Label name is incorrect')

        # Match nodes with 'Cat' label
        self.cursor.execute(self.queries[5])
        result = self.cursor.fetch_all()
        self.assertEqual(2, self.cursor.count(), 'Number of nodes with label is incorrect')
        self.assertEqual(2, len(result), 'Number of nodes with label is incorrect')

        # Match 'Mouse' node
        self.cursor.execute(self.queries[6])
        result = self.cursor.fetch_all()
        self.assertEqual(1, len(result), 'Number of nodes with label is incorrect')

        label = result[0].get_label()
        self.assertEqual('Mouse', label.get_name(), 'Label of matched node is incorrect')

        # Match 'catches' relationship
        self.cursor.execute(self.queries[7])
        result = self.cursor.fetch_all()
        self.assertEqual(1, len(result), 'Number of relationships with label is incorrect')

        label = result[0].get_label()
        self.assertEqual('catches', label.get_name(), 'Label of matched relationship is incorrect')

        # Create nodes with property
        self.cursor.execute(self.queries[8])
        retrieved_node = self.graph_engine.select_node(node_id=3)
        self.assertEqual(3, retrieved_node.get_id(), 'Node id is incorrect')
        prop = retrieved_node.get_first_property()
        self.assertEqual('Animal', prop.get_key(), 'Key of property is incorrect')
        self.assertEqual('Mouse', prop.get_value(), 'Value of property is incorrect')

        self.cursor.execute(self.queries[9])
        retrieved_node = self.graph_engine.select_node(node_id=4)
        self.assertEqual(4, retrieved_node.get_id(), 'Node id is incorrect')
        prop = retrieved_node.get_first_property()
        self.assertEqual('Animal', prop.get_key(), 'Key of property is incorrect')
        self.assertEqual('Cat', prop.get_value(), 'Value of property is incorrect')

        # Create relationships with property
        self.cursor.execute(self.queries[10])
        retrieved_relationship = self.graph_engine.select_relationship(rel_id=1)
        self.assertEqual(1, retrieved_relationship.get_id(), 'relationship id is incorrect')
        prop = retrieved_relationship.get_first_property()
        self.assertEqual('Durability', prop.get_key(), 'Key of property is incorrect')
        self.assertEqual('2', prop.get_value(), 'Value of property is incorrect')

        self.cursor.execute(self.queries[11])
        retrieved_relationship = self.graph_engine.select_relationship(rel_id=2)
        self.assertEqual(2, retrieved_relationship.get_id(), 'relationship id is incorrect')
        prop = retrieved_relationship.get_first_property()
        self.assertEqual('Time', prop.get_key(), 'Key of property is incorrect')
        self.assertEqual('10', prop.get_value(), 'Value of property is incorrect')

        # Create a node with multiple properties
        self.cursor.execute(self.queries[12])
        retrieved_node = self.graph_engine.select_node(node_id=5)
        self.assertEqual(3, len(retrieved_node.get_properties()), 'Number of properties is incorrect')
        self.assertEqual('CPU', retrieved_node.get_properties()[1].get_key(), 'Retrieved key is incorrect')
        self.assertEqual('NVidia', retrieved_node.get_properties()[2].get_value(), 'Retrieved value is incorrect')

        # Create an relationship with multiple properties
        self.cursor.execute(self.queries[13])
        retrieved_relationship = self.graph_engine.select_relationship(rel_id=3)
        self.assertEqual(2, len(retrieved_relationship.get_properties()), 'Number of properties is incorrect')
        self.assertEqual('MadMax', retrieved_relationship.get_properties()[1].get_value(),
                         'Retrieved value is incorrect')
        self.assertEqual(9, self.graph_engine.get_stats()['PropertyStorage'], 'Incorrect number of properties')

        # Graph traverse with match graph: graph
        self.cursor.execute(self.queries[14])
        result = self.cursor.fetch_all()
        self.assertEqual(10, len(result), 'Number of objects in graph is incorrect')

        # Create 2 nodes and 1 relationship with properties to match
        for query in self.queries[15:18]:
            self.cursor.execute(query)

        # Match nodes by property
        self.cursor.execute(self.queries[18])
        result = self.cursor.fetch_all()
        self.assertEqual(1, len(result), 'Incorrect number of matched nodes')
        self.assertEqual(20, int(result[0].get_first_property().get_value()),
                         'Retrieved value of property is incorrect')

        # Node
        self.cursor.execute(self.queries[19])
        result = self.cursor.fetch_all()
        self.assertEqual(1, len(result), 'Incorrect number of matched nodes')

        # Node
        self.cursor.execute(self.queries[20])
        result = self.cursor.fetch_all()
        self.assertEqual(2, len(result), 'Incorrect number of matched nodes')

        # Relationship
        self.cursor.execute(self.queries[21])
        result = self.cursor.fetch_all()
        self.assertEqual(1, len(result), 'Incorrect number of matched relationships')

        # Not existing objects
        self.cursor.execute(self.queries[22])
        result = self.cursor.fetch_all()
        self.assertEqual(0, len(result), 'Incorrect number of matched nodes')

        self.cursor.execute(self.queries[23])
        result = self.cursor.fetch_all()
        self.assertEqual(0, len(result), 'Incorrect number of matched nodes')

        # Delete relationship
        for query in self.queries[24:28]:
            self.cursor.execute(query)

        self.cursor.execute(self.queries[28])
        result = self.cursor.fetch_all()
        self.assertEqual(0, len(result), 'Relationship was not deleted')

        with self.assertRaises(GraphEngineError):
            self.cursor.execute(self.queries[29])

        # Create relationship for invalid nodes
        with self.assertRaises(GraphEngineError):
            self.cursor.execute(self.queries[30])

        self.cursor.execute(self.queries[31])
        retrieved_node = self.graph_engine.select_node(node_id=3)
        prop = retrieved_node.get_first_property()
        self.assertEqual('Animal', prop.get_key(), 'Key of property is incorrect')
        self.assertEqual('Mouse', prop.get_value(), 'Value of property is incorrect')
        prop = retrieved_node.get_last_property()
        self.assertEqual('color', prop.get_key(), 'Key of property is incorrect')
        self.assertEqual('brown', prop.get_value(), 'Value of property is incorrect')

    def test_queries_invalid(self):
        # Graph creation
        for query in self.queries_invalid[0:3]:
            with self.assertRaises(InputError):
                self.cursor.execute(query)

        self.cursor.execute(self.queries[0])

        # Node creation
        for query in self.queries_invalid[3:6]:
            with self.assertRaises(InputError):
                self.cursor.execute(query)

        # Node match
        for query in self.queries_invalid[6:11]:
            with self.assertRaises(InputError):
                self.cursor.execute(query)

        # Create nodes with property
        for query in self.queries_invalid[11:14]:
            with self.assertRaises(InputError):
                self.cursor.execute(query)

        # Create relationships with property
        with self.assertRaises(InputError):
            self.cursor.execute(self.queries_invalid[14])

        # Create a node with multiple properties
        for query in self.queries_invalid[15:18]:
            with self.assertRaises(InputError):
                self.cursor.execute(query)

        # Graph traverse with match graph: graph
        with self.assertRaises(InputError):
            self.cursor.execute(self.queries_invalid[18])

        # Match nodes by property
        for query in self.queries_invalid[19:23]:
            with self.assertRaises(InputError):
                self.cursor.execute(query)
