from graph_db.engine.property import Property
from graph_db.engine.graph import Graph
import re


class Parser:

    def parse_query(self, graph, query):
        properties = []
        query_type = query.split()[0]
        query_len = len(query.split())
        if query_type == 'CREATE':
            try:
                creation_of = query.split()[1]
            except:
                print("Incorrect query")
                return None
            if creation_of == 'graph:':
                try:
                    graph_label = query.split()[2]
                except:
                    print("Incorrect query")
                    return None
                print("You have created '" + str(graph_label) + "' graph")
                return Graph(graph_label, 'temp_db/')
            if creation_of == 'node:':
                try:
                    node_label = query.split()[2]
                except:
                    print("Incorrect query")
                    return None
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
                created_node = graph.create_node(label_name=node_label, properties=properties)
                print(created_node)

            if creation_of == 'edge:':
                try:
                    edge_label = query.split()[2]
                    start_node_label = query.split()[4]
                    end_node_label = query.split()[6]
                except:
                    print("Incorrect query")
                    return None
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
                    start_node = graph.select_node_by_label(start_node_label)[0]
                    end_node = graph.select_node_by_label(end_node_label)[0]
                    created_edge = graph.create_edge(label_name=edge_label,
                                                     start_node=start_node,
                                                     end_node=end_node,
                                                     properties=properties)
                    print(created_edge)
                except:
                    print('Either you haven\'t created entered label or entered label is incorrect')
                    return None
            return graph
        elif query_type == 'MATCH':
            try:
                match_of = query.split()[1]
                third_term = query.split()[2]
            except:
                print("Incorrect query")
                return None
            try:
                if match_of == 'node:' or match_of == 'edge:':
                    symbols = ['>', '<', '=', '>=', '<=']
                    sign = re.findall('[<=>]+', third_term)
                    if len(sign) == 0:
                        if match_of == 'node:':
                            selected_nodes = graph.select_node_by_label(third_term)
                            for node in selected_nodes:
                                print(node)
                            return selected_nodes
                        elif match_of == 'edge:':
                            selected_edges = graph.select_edge_by_label(third_term)
                            for edge in selected_edges:
                                print(edge)
                            return selected_edges
                    else:
                        if sign[0] in symbols:
                            key, value = third_term.split(sign[0])
                            selected_by_property = []
                            if match_of == 'node:':
                                selected_by_property = graph.select_with_condition(key, value, sign[0], 'node')
                            elif match_of == 'edge:':
                                selected_by_property = graph.select_with_condition(key, value, sign[0], 'edge')
                            for obj in selected_by_property:
                                print(obj)
                            return selected_by_property
                        else:
                            print('Incorrect query')
                            return None
                if match_of == 'graph:':
                    if third_term == graph.name:
                        print("\nGraph: '" + str(graph.name) + "'")
                        objects = graph.traverse_graph()
                        for obj in objects:
                            print(obj)
                        print('\n')
                        return objects
                    else:
                        print("There is no such " + '"' + str(third_term) + '"' + " graph")
                        return None
                if match_of == 'property:':
                    key, value = third_term.split(':')
                    selected_by_property = graph.select_by_property(Property(key, value))
                    for obj in selected_by_property:
                        print(obj)
                    return selected_by_property
            except:
                print('Either you haven\'t created entered label or entered label is incorrect')
                return None
