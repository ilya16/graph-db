
class Parser:

    def parse_query(self, graph, query):
        query_type = query.split()[0]
        if query_type == 'CREATE':
            creation_of = query.split()[1]
            if creation_of == 'node:':
                node_label = query.split()[2]
                if len(query.split()) <= 3:
                    graph.create_node(label=node_label, key=None, value=None)
                else:
                    key, value = query.split()[3].split(':')
                    graph.create_node(label=node_label, key=key, value=value)
            if creation_of == 'edge:':
                edge_label = query.split()[2]
                start_node = query.split()[4]
                end_node = query.split()[6]
                if len(query.split()) <= 7:
                    graph.create_edge(label=edge_label, start_node_label=start_node, end_node_label=end_node, key=None,
                                      value=None)
                else:
                    key, value = query.split()[7].split(':')
                    graph.create_edge(label=edge_label, start_node_label=start_node, end_node_label=end_node, key=key,
                                      value=value)
            return graph
        elif query_type == 'MATCH':
            match_of = query.split()[1]
            if match_of == 'node:':
                return graph.select_node_by_label(query.split()[2])
            if match_of == 'edge:':
                return graph.select_edge_by_label(query.split()[2])
