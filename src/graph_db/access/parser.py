from graph_db.engine.graph import Graph


class Parser:

    def parse_query(self, graph, query):
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
        return graph
