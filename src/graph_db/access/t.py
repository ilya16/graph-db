from graph_db.engine.graph import Graph
from graph_db.access.parser import Parser
import networkx as nx
import matplotlib.pyplot as plt
import os

queries = [
    'CREATE node: Warrior Animal:Dog',
    'CREATE node: Node2 Age:2',
    'CREATE edge: rel FROM Warrior TO Node2 Type:Human'
]

temp_dir = 'temp/'

parser = Parser()
graph = Graph('graph', temp_dir)

for query in queries:
    graph = parser.parse_query(graph, query)

node = graph.select_nth_node(1)
print(node.get_first_property().get_key())
print(node._label.get_name())
# print(node.get_property_value('Animal'))

# deleting created temp stores
for path in os.listdir(temp_dir):
    os.remove(os.path.join(temp_dir, path))
os.removedirs(temp_dir)


# матч без условия: match n: id1 Animal:Dog;
# match e: id2 Relation:Enemy;
# матч с условием: match n: id1 Age>2;



# print(graph.select_nth_node(0).get_label().get_name())