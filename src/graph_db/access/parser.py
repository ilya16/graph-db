from graph_db.access.graph import Graph

query = "CREATE node: Warrior"

g = Graph()

def parse_query(query):
    query_type = query.split()[0]
    if query_type == 'CREATE':
        creation_of = query.split()[1]
        if creation_of == 'node:':
            label = query.split()[2]
            g.create_node(label)


parse_query(query)