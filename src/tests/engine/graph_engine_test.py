from unittest import TestCase
import os

from graph_db.engine.graph_engine import GraphEngine
from graph_db.engine.property import Property
from graph_db.fs.io_engine import DYNAMIC_RECORD_PAYLOAD_SIZE


class IOEngineCase(TestCase):
    temp_dir = 'temp_db/'

    def setUp(self):
        self.graph_engine = GraphEngine(base_dir=self.temp_dir)
        self.graph_engine.create_graph('test')

        self.assertDictEqual(dict(), self.graph_engine.get_graph().get_nodes())
        self.assertDictEqual(dict(), self.graph_engine.get_graph().get_relationships())
        self.assertDictEqual(dict(), self.graph_engine.get_graph().get_labels())
        self.assertDictEqual(dict(), self.graph_engine.label_names)

    def tearDown(self):
        self.graph_engine.close()
        self.graph_engine = None

        # deleting created temp stores
        for path in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, path))
        os.removedirs(self.temp_dir)

        with self.assertRaises(FileNotFoundError):
            os.listdir(self.temp_dir)

    def test_nodes_and_labels(self):
        label_name = 'test'

        node = self.graph_engine.create_node(label_name)
        self.assertEqual(1, self.graph_engine.get_stats()['NodeStorage'], 'Storage contains extra data')

        retrieved_node = self.graph_engine.select_node(node.get_id())
        label = retrieved_node.get_label()

        self.assertEqual(node.get_id(), retrieved_node.get_id(), 'Node ids have changed')
        self.assertEqual(0, label.get_id(), 'Label id is incorrect')
        self.assertEqual('test', retrieved_node.get_label().get_name(), 'Label name is incorrect')

        self.assertListEqual([retrieved_node], self.graph_engine.select_nodes())
        self.assertListEqual(list(), self.graph_engine.select_relationships())
        self.assertListEqual([label], self.graph_engine.select_labels())

        # inserting another node with the same label
        node = self.graph_engine.create_node(label_name)

        retrieved_node = self.graph_engine.select_node(node.get_id())
        label = retrieved_node.get_label()

        self.assertEqual(retrieved_node.get_id(), node.get_id(), 'Node ids have changed')
        self.assertEqual(1, retrieved_node.get_id(), 'Node id is incorrect')
        self.assertEqual(0, label.get_id(), 'Label id is incorrect')
        self.assertEqual('test', retrieved_node.get_label().get_name(), 'Label name is incorrect')

        # inserting node with the different label of size > DYNAMIC_RECORD_PAYLOAD_SIZE
        name = 'Tester of the code. ' * 5
        n_dynamic_records = len(name) // DYNAMIC_RECORD_PAYLOAD_SIZE
        if len(name) % DYNAMIC_RECORD_PAYLOAD_SIZE != 0:
            n_dynamic_records += 1

        n_dynamic_records_old = self.graph_engine.get_stats()['DynamicStorage']
        self.assertEqual(1, n_dynamic_records_old, 'Dynamic Storage is not consistent')

        node = self.graph_engine.create_node(name)

        retrieved_node = self.graph_engine.select_node(node.get_id())
        label = retrieved_node.get_label()

        self.assertEqual(retrieved_node.get_id(), node.get_id(), 'Node ids have changed')
        self.assertEqual(2, retrieved_node.get_id(), 'Node id is incorrect')
        self.assertEqual(1, label.get_id(), 'Label id is incorrect')
        self.assertEqual(name, retrieved_node.get_label().get_name(), 'Label name is incorrect')
        self.assertEqual(n_dynamic_records_old + n_dynamic_records,
                         self.graph_engine.get_stats()['DynamicStorage'],
                         'Dynamic Storage is not consistent')

    def test_relationships(self):
        label_one = 'Robin'
        label_two = 'Sara'
        label_rel_one = 'loves :-)'

        first_node = self.graph_engine.create_node(label_one)
        second_node = self.graph_engine.create_node(label_two)
        self.assertEqual(2, self.graph_engine.get_stats()['NodeStorage'], 'Storage has incorrect number of nodes')

        relationship_one = self.graph_engine.create_relationship(label_rel_one, first_node, second_node)
        self.assertEqual(1, self.graph_engine.get_stats()['RelationshipStorage'], 'Storage contains extra data')
        self.assertEqual(3, self.graph_engine.get_stats()['LabelStorage'], 'LabelStorage is not consistent')

        retrieved_relationship_one = self.graph_engine.select_relationship(relationship_one.get_id())
        self.assertEqual(retrieved_relationship_one.get_id(), relationship_one.get_id(), 'Relationship id has changed')
        self.assertEqual(0, retrieved_relationship_one.get_id(), 'Relationship has incorrect id')
        self.assertEqual(first_node.get_id(), retrieved_relationship_one.get_start_node().get_id(), 'Start node id has changed')
        self.assertEqual(second_node.get_id(), retrieved_relationship_one.get_end_node().get_id(), 'End node id has changed')
        self.assertEqual('loves :-)', retrieved_relationship_one.get_label().get_name(), 'Relationship\'s label has changed')
        self.assertEqual(None, retrieved_relationship_one.get_start_prev_rel())
        self.assertEqual(None, retrieved_relationship_one.get_start_next_rel())
        self.assertEqual(None, retrieved_relationship_one.get_end_prev_rel())
        self.assertEqual(None, retrieved_relationship_one.get_end_prev_rel())

        # checking that start and end node records have been updated
        retrieved_first_node = self.graph_engine.select_node(first_node.get_id())
        self.assertEqual(relationship_one.get_id(), retrieved_first_node.get_first_relationship().get_id())
        retrieved_second_node = self.graph_engine.select_node(second_node.get_id())
        self.assertEqual(relationship_one.get_id(), retrieved_second_node.get_first_relationship().get_id())

        # adding third node and relationship from first and second nodes
        label_three = 'Xbox360'
        label_rel_two = 'plays'
        first_node = self.graph_engine.select_node(first_node.get_id())
        self.assertEqual('Robin', first_node.get_label().get_name(), 'Node\'s name has changed')
        self.assertEqual(0, first_node.get_id(), 'Node\'s id has changed')

        third_node = self.graph_engine.create_node(label_three)

        relationship_two = self.graph_engine.create_relationship(label_rel_two, first_node, third_node)
        self.assertEqual(2, self.graph_engine.get_stats()['RelationshipStorage'], 'Storage has incorrect number of data')

        retrieved_relationship_one = self.graph_engine.select_relationship(relationship_one.get_id())
        retrieved_relationship_two = self.graph_engine.select_relationship(relationship_two.get_id())
        self.assertEqual(relationship_two.get_id(), retrieved_relationship_two.get_id(), 'Relationship id have changed')
        self.assertEqual(1, relationship_two.get_id(), 'Relationship has incorrect id')
        self.assertEqual(first_node.get_id(), retrieved_relationship_two.get_start_node().get_id(), 'Start node id has changed')
        self.assertEqual(third_node.get_id(), retrieved_relationship_two.get_end_node().get_id(), 'End node id has changed')
        self.assertEqual('plays', retrieved_relationship_two.get_label().get_name(), 'Relationship\'s label has changed')

        self.assertEqual(relationship_one, retrieved_relationship_two.get_start_prev_rel())
        self.assertEqual(None, retrieved_relationship_two.get_start_next_rel())
        self.assertEqual(None, retrieved_relationship_two.get_end_prev_rel())
        self.assertEqual(None, retrieved_relationship_two.get_end_prev_rel())

        self.assertEqual(None, retrieved_relationship_one.get_start_prev_rel())
        self.assertEqual(relationship_two, retrieved_relationship_one.get_start_next_rel())
        self.assertEqual(None, retrieved_relationship_one.get_end_prev_rel())
        self.assertEqual(None, retrieved_relationship_one.get_end_prev_rel())

        # checking that start and end node records have been updated
        retrieved_first_node = self.graph_engine.select_node(first_node.get_id())
        self.assertNotEqual(relationship_two.get_id(), retrieved_first_node.get_first_relationship().get_id())
        retrieved_third_node = self.graph_engine.select_node(third_node.get_id())
        self.assertEqual(relationship_two.get_id(), retrieved_third_node.get_first_relationship().get_id())

    def test_properties(self):
        label_name = 'test'
        first_property = Property(key='Age', value='18')

        # Create node with one property
        node = self.graph_engine.create_node(label_name=label_name, properties=[first_property])
        self.assertEqual(1, self.graph_engine.get_stats()['NodeStorage'], 'Storage contains extra data')
        self.assertEqual(0, node.get_id(), 'Node ids have changed')
        self.assertEqual(1, self.graph_engine.get_stats()['PropertyStorage'], 'Storage contains extra data')

        retrieved_first_property = node.get_first_property()
        self.assertEqual(0, retrieved_first_property.get_id(), 'Property has incorrect id')
        self.assertEqual('Age', retrieved_first_property.get_key(), 'Property\'s key have changed')
        self.assertEqual('18', retrieved_first_property.get_value(), 'Property\'s value have changed')
        self.assertIsNone(retrieved_first_property.get_next_property(), 'Property object is not consistent')

        # Add second property to node
        second_property = Property(key='Sex', value='Male')
        node = self.graph_engine.add_property(node, second_property)
        self.assertEqual(2, self.graph_engine.get_stats()['PropertyStorage'], 'Storage has incorrect number of properties')

        retrieved_second_property = node.get_last_property()
        self.assertEqual(retrieved_second_property.get_id(), second_property.get_id(), 'Property ids have changed')
        self.assertEqual(1, retrieved_second_property.get_id(), 'Property has incorrect id')
        self.assertEqual('Sex', retrieved_second_property.get_key(), 'Property\'s key have changed')
        self.assertEqual('Male', retrieved_second_property.get_value(), 'Property\'s value have changed')

        self.assertListEqual([first_property, second_property], node.get_properties(), 'List of properties is incorrect')
        self.assertIsNone(retrieved_second_property.get_next_property(), 'Second property should not have next property')
        self.assertEqual(retrieved_second_property, retrieved_first_property.get_next_property(),
                         'Next property pointer is damaged')

    def test_simple_case(self):
        """
        Simulate simple example that uses all features of file system
        :return:
        """
        # Define labels
        node_label_name = 'User'
        edge_label_name = 'loves :-)'

        # First node properties
        first_node_props = [Property('Name', 'Robin'), Property('Age', 18), Property('Male', True)]

        # Define first node
        first_node = self.graph_engine.create_node(label_name=node_label_name, properties=first_node_props)

        # Update node with new properties
        retrieved_node = self.graph_engine.select_node(first_node.get_id())
        self.assertEqual(first_node, retrieved_node, 'Node has changed')
        self.assertEqual(0, retrieved_node.get_id(), 'Node id is incorrect')
        self.assertEqual('Robin', retrieved_node.get_first_property().get_value(), 'Value of first property has changed')
        self.assertEqual(18, retrieved_node.get_properties()[1].get_value(), 'Value of second property has changed')
        self.assertEqual(True, retrieved_node.get_properties()[2].get_value(), 'Value of third property has changed')

        # Define second node
        second_node = self.graph_engine.create_node(label_name=node_label_name)
        self.assertEqual(2, self.graph_engine.get_stats()['NodeStorage'], 'Storage has incorrect number of nodes')
        self.assertEqual(1, second_node.get_id(), 'Node id is incorrect')

        # Add properties to second node
        second_node = self.graph_engine.add_property(second_node, Property('Name', 'Sara'))
        second_node = self.graph_engine.add_property(second_node, Property('Age', 20))
        second_node = self.graph_engine.add_property(second_node, Property('Male', False))

        retrieved_node = self.graph_engine.select_node(second_node.get_id())
        self.assertEqual('Sara', retrieved_node.get_first_property().get_value(), 'Value of first property has changed')
        self.assertEqual(20, retrieved_node.get_properties()[1].get_value(), 'Value of second property has changed')
        self.assertEqual(False, retrieved_node.get_properties()[2].get_value(), 'Value of third property has changed')

        # Create first relationship
        relationship = self.graph_engine.create_relationship(edge_label_name, first_node, second_node)
        self.assertEqual(1, self.graph_engine.get_stats()['RelationshipStorage'], 'Storage contains extra data')

        # Add property to relationship
        self.graph_engine.add_property(relationship, Property('How many years', 1))

        retrieved_relationship = self.graph_engine.select_relationship(relationship.get_id())
        self.assertEqual(retrieved_relationship.get_id(), relationship.get_id(), 'Relationship id have changed')
        self.assertEqual(0, retrieved_relationship.get_id(), 'Relationship has incorrect id')
        self.assertEqual(first_node.get_id(), retrieved_relationship.get_start_node().get_id(), 'Start node has changed')
        self.assertEqual(second_node.get_id(), retrieved_relationship.get_end_node().get_id(), 'End node has changed')
        self.assertEqual('loves :-)', retrieved_relationship.get_label().get_name(),
                         'Relationship\'s label has changed')
        self.assertEqual('How many years', retrieved_relationship.get_first_property().get_key(),
                         'Incorrect property\'s key')
        self.assertEqual(1, retrieved_relationship.get_first_property().get_value(), 'Incorrect property\'s value')
        self.assertEqual(20, retrieved_relationship.get_end_node().get_properties()[1].get_value(),
                         'Incorrect end node property\'s value')

    def test_simple_case_from_disk(self):
        # Define labels
        node_label_name = 'User'
        edge_label_name = 'loves :-)'

        # First node properties
        first_node_props = [Property('Name', 'Robin'), Property('Age', 18), Property('Male', True)]

        # Define first node
        first_node = self.graph_engine.create_node(label_name=node_label_name, properties=first_node_props)

        # Define second node
        second_node = self.graph_engine.create_node(label_name=node_label_name)
        self.assertEqual(2, self.graph_engine.get_stats()['NodeStorage'], 'Storage has incorrect number of nodes')
        self.assertEqual(1, second_node.get_id(), 'Node id is incorrect')

        # Add properties to second node
        second_node = self.graph_engine.add_property(second_node, Property('Name', 'Sara'))
        second_node = self.graph_engine.add_property(second_node, Property('Age', 20))
        second_node = self.graph_engine.add_property(second_node, Property('Male', False))

        # Create first relationship
        relationship = self.graph_engine.create_relationship(edge_label_name, first_node, second_node)

        # Add property to relationship
        self.graph_engine.add_property(relationship, Property('How many years', 1))

        # Close and reopen graph engine, so that in memory objects will be cleared
        self.graph_engine.close()
        self.graph_engine = GraphEngine(base_dir=self.temp_dir)

        # Checking stats
        self.assertEqual(2, self.graph_engine.get_stats()['NodeStorage'], 'Storage has incorrect number of nodes')
        self.assertEqual(1, self.graph_engine.get_stats()['RelationshipStorage'], 'Storage is not in consistent state')
        self.assertEqual(7, self.graph_engine.get_stats()['PropertyStorage'], 'Storage is not in consistent state')

        # Selecting objects (object collector should be invoked)
        retrieved_first_node = self.graph_engine.select_node(first_node.get_id())
        self.assertEqual(0, retrieved_first_node.get_id(), 'Node id is incorrect')
        self.assertEqual('Robin', retrieved_first_node.get_first_property().get_value(),
                         'Value of first property has changed')
        self.assertEqual(18, retrieved_first_node.get_properties()[1].get_value(),
                         'Value of second property has changed')
        self.assertEqual(True, retrieved_first_node.get_properties()[2].get_value(),
                         'Value of third property has changed')

        retrieved_second_node = self.graph_engine.select_node(second_node.get_id())
        self.assertEqual('Sara', retrieved_second_node.get_first_property().get_value(),
                         'Value of first property has changed')
        self.assertEqual(20, retrieved_second_node.get_properties()[1].get_value(),
                         'Value of second property has changed')
        self.assertEqual(False, retrieved_second_node.get_properties()[2].get_value(),
                         'Value of third property has changed')

        retrieved_relationship = self.graph_engine.select_relationship(relationship.get_id())
        self.assertEqual(retrieved_relationship.get_id(), relationship.get_id(), 'Relationship id have changed')
        self.assertEqual(0, retrieved_relationship.get_id(), 'Relationship has incorrect id')
        self.assertEqual(first_node.get_id(), retrieved_relationship.get_start_node().get_id(),
                         'Start node has changed')
        self.assertEqual(second_node.get_id(), retrieved_relationship.get_end_node().get_id(), 'End node has changed')
        self.assertEqual('loves :-)', retrieved_relationship.get_label().get_name(),
                         'Relationship\'s label has changed')
        self.assertEqual('How many years', retrieved_relationship.get_first_property().get_key(),
                         'Incorrect property\'s key')
        self.assertEqual(1, retrieved_relationship.get_first_property().get_value(), 'Incorrect property\'s value')
        self.assertEqual(20, retrieved_relationship.get_end_node().get_properties()[1].get_value(),
                         'Incorrect end node property\'s value')


