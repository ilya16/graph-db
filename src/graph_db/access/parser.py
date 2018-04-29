
class Parser:

    def parse_query(self, graph, query):
        query_type = query.split()[0]
        if query_type == 'CREATE':
            creation_of = query.split()[1]
            if creation_of == 'node:':
                node_label = query.split()[2]
                if len(query.split()) <= 3:
                    created_node = graph.create_node(label=node_label, key=None, value=None)
                    print(created_node)
                else:
                    key, value = query.split()[3].split(':')
                    created_node = graph.create_node(label=node_label, key=key, value=value)
                    print(created_node)
            if creation_of == 'edge:':
                edge_label = query.split()[2]
                start_node = query.split()[4]
                end_node = query.split()[6]
                try:
                    if len(query.split()) <= 7:
                        created_edge = graph.create_edge(label=edge_label, start_node_label=start_node, end_node_label=end_node, key=None,
                                          value=None)
                        print(created_edge)
                    else:
                        key, value = query.split()[7].split(':')
                        created_edge = graph.create_edge(label=edge_label, start_node_label=start_node, end_node_label=end_node, key=key,
                                          value=value)
                        print(created_edge)
                except:
                    print('Either you haven\'t created entered label or entered label is incorrect')
                    return None
            return graph
        elif query_type == 'MATCH':
            match_of = query.split()[1]
            try:
                if match_of == 'node:':
                    selected_nodes = graph.select_node_by_label(query.split()[2])
                    for node in selected_nodes:
                        print(node)
                    return selected_nodes
                if match_of == 'edge:':
                    selected_edges = graph.select_edge_by_label(query.split()[2])
                    for edge in selected_edges:
                        print(edge)
                    return selected_edges
            except:
                print('Either you haven\'t created entered label or entered label is incorrect')
                return None
