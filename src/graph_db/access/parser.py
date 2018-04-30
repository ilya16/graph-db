from graph_db.engine.property import Property


class Parser:

    def parse_query(self, graph, query):
        properties = []
        query_type = query.split()[0]
        query_len = len(query.split())
        if query_type == 'CREATE':
            creation_of = query.split()[1]
            if creation_of == 'node:':
                node_label = query.split()[2]
                if query_len > 3:
                    if query_len >= 4:
                        try:
                            key, value = query.split()[3].split(':')
                            properties.append(Property(key, value))
                        except:
                            print('Write properties as follows: key:value')
                            return None
                    if query_len >= 5:
                        try:
                            key, value = query.split()[4].split(':')
                            properties.append(Property(key, value))
                        except:
                            print('Write properties as follows: key:value')
                            return None
                    if query_len >= 6:
                        try:
                            key, value = query.split()[5].split(':')
                            properties.append(Property(key, value))
                        except:
                            print('Write properties as follows: key:value')
                            return None
                created_node = graph.create_node(label=node_label, properties=properties)
                print(created_node)

            if creation_of == 'edge:':
                edge_label = query.split()[2]
                start_node = query.split()[4]
                end_node = query.split()[6]
                try:
                    if query_len > 7:
                        if query_len >= 8:
                            try:
                                key, value = query.split()[7].split(':')
                                properties.append(Property(key, value))
                            except:
                                print('Write properties as follows: key:value')
                                return None
                        if query_len >= 9:
                            try:
                                key, value = query.split()[8].split(':')
                                properties.append(Property(key, value))
                            except:
                                print('Write properties as follows: key:value')
                                return None
                        if query_len >= 10:
                            try:
                                key, value = query.split()[9].split(':')
                                properties.append(Property(key, value))
                            except:
                                print('Write properties as follows: key:value')
                                return None
                    created_edge = graph.create_edge(label=edge_label,
                                                     start_node_label=start_node,
                                                     end_node_label=end_node,
                                                     properties=properties)
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
