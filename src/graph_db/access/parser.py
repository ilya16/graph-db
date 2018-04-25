from graph_db.access.graph import Graph

# createedge n: id1 Animal:Dog e: rel1 Relation:Enemy n: id2

queries = []

queries.append('CREATE node: Warrior')
queries.append('CREATE node: Node2')
queries.append('CREATE edge: e FROM Warrior TO Node2')

graph = Graph('graph')

def parse_query(query):
    query_type = query.split()[0]
    if query_type == 'CREATE':
        creation_of = query.split()[1]
        if creation_of == 'node:':
            node_label = query.split()[2]
            graph.create_node(label=node_label)
        if creation_of == 'edge:':
            edge_label = query.split()[2]
            start_node = query.split()[4]
            end_node = query.split()[6]
            graph.create_edge(label=edge_label, start_node_label=start_node, end_node_label=end_node)

for query in queries:
    parse_query(query)

node = graph.select_nth_node(0)
print(node.get_label().get_name())
# edge = graph.select_nth_edge(0)
# print(edge.get_label().get_name())


# print(graph.select_nth_edge(0))
# parse_query(query)
# graph.show_node()