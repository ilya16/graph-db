from graph_db.engine.graph import Graph
from graph_db.access.parser import Parser
import networkx as nx
import matplotlib.pyplot as plt
import os

queries = [
    'CREATE node: Tom Animal:Cat',
    'MATCH graph: graph',
    'CREATE node: Cat',
    'MATCH node: Cat'
]

temp_dir = 'temp/'

parser = Parser()
graph = Graph('graph', temp_dir)

for query in queries:
    graph = parser.parse_query(graph, query)

# print(node.get_property_value('Animal'))

# deleting created temp stores
for path in os.listdir(temp_dir):
    os.remove(os.path.join(temp_dir, path))
os.removedirs(temp_dir)


# матч без условия: match n: id1 Animal:Dog;
# match e: id2 Relation:Enemy;
# матч с условием: match n: id1 Age>2;



# print(graph.select_nth_node(0).get_label().get_name())