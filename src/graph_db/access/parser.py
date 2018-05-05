from typing import Callable, Tuple, Dict

from graph_db.engine.graph_engine import GraphEngine
from graph_db.engine.property import Property
import re


class Parser:
    @staticmethod
    def parse_query(query) -> Tuple[Callable, Dict[str, object]]:
        properties = []
        query_type = query.split()[0]
        query_len = len(query.split())
        if query_type.lower() == 'create':
            try:
                creation_of = query.split()[1]
            except Exception:
                raise InputError('Incorrect query')
            if creation_of == 'graph:':
                try:
                    graph_label = query.split()[2]
                except Exception:
                    raise InputError('Incorrect query')
                print("You have created '" + str(graph_label) + "' graph")
                return GraphEngine.create_graph, {'graph_name': graph_label}
            if creation_of == 'node:':
                try:
                    node_label = query.split()[2]
                except Exception:
                    raise InputError('Incorrect query')
                prop_idx = 3
                while query_len > prop_idx:
                    try:
                        key, value = query.split()[prop_idx].split(':')
                        if key == '' or value == '':
                            raise InputError('Write properties as follows: key:value')
                        properties.append(Property(key, value))
                    except Exception:
                        raise InputError('Write properties as follows: key:value')
                    prop_idx += 1
                return GraphEngine.create_node, {'label_name': node_label, 'properties': properties}

            if creation_of == 'relationship:':
                try:
                    relationship_label = query.split()[2]
                    start_node_label = query.split()[4]
                    end_node_label = query.split()[6]
                    start_node_id = -1
                    end_node_id = -1
                    if 'id:' in start_node_label:
                        start_node_id = int(start_node_label[3:])
                    if 'id:' in end_node_label:
                        end_node_id = int(end_node_label[3:])
                except Exception:
                    raise InputError('Incorrect query')
                try:
                    prop_idx = 7
                    while query_len > prop_idx:
                        try:
                            key, value = query.split()[prop_idx].split(':')
                            if key == '' or value == '':
                                raise InputError('Write properties as follows: key:value')
                            properties.append(Property(key, value))
                        except Exception:
                            raise InputError('Write properties as follows: key:value')
                        prop_idx += 1

                    if start_node_id == -1:
                        select_start_node_call = GraphEngine.select_nodes, {'label': start_node_label}
                    else:
                        select_start_node_call = GraphEngine.select_node, {'node_id': start_node_id}
                    if end_node_id == -1:
                        select_end_node_call = GraphEngine.select_nodes, {'label': end_node_label}
                    else:
                        select_end_node_call = GraphEngine.select_node, {'node_id': end_node_id}

                    return GraphEngine.create_relationship, {'label_name': relationship_label,
                                                             'start_node': select_start_node_call,
                                                             'end_node': select_end_node_call,
                                                             'properties': properties}
                except Exception:
                    raise InputError('Either you haven\'t created entered label or entered label is incorrect')
            raise InputError('Incorrect query')
        elif query_type.lower() == 'match':
            try:
                match_of = query.split()[1]
                third_term = query.split()[2]
            except Exception:
                raise InputError('Incorrect query')
            try:
                if match_of == 'node:' or match_of == 'relationship:':
                    symbols = ['>', '<', '=', '>=', '<=']
                    sign = re.findall('[<=>]+', third_term)
                    if len(sign) == 0:
                        if match_of == 'node:':
                            if 'id:' not in third_term:
                                return GraphEngine.select_nodes, {'label': third_term}
                            else:
                                return GraphEngine.select_node, {'node_id': int(third_term[3:])}
                        elif match_of == 'relationship:':
                            if 'id:' not in third_term:
                                return GraphEngine.select_relationships, {'label': third_term}
                            else:
                                return GraphEngine.select_relationship, {'rel_id': int(third_term[3:])}
                    else:
                        if sign[0] in symbols:
                            key, value = third_term.split(sign[0])
                            if key == '' or value == '':
                                raise InputError('Write properties as follows: key:value')
                            if match_of != 'node:' and match_of != 'relationship:':
                                raise InputError('Incorrect query')
                            return GraphEngine.select_with_condition, {'key': key,
                                                                       'value': value,
                                                                       'cond': sign[0],
                                                                       'match_of': match_of[:-1]}
                        else:
                            raise InputError('Incorrect query')
                if match_of == 'graph:':
                    # if third_term != graph.name:
                    #     raise InputError("There is no such " + '"' + str(third_term) + '"' + " graph")
                    return GraphEngine.traverse_graph, {}

                raise InputError('Incorrect query')
            except Exception:
                raise InputError('Either you haven\'t created entered label or entered label is incorrect')
        elif query_type.lower() == 'delete':
            try:
                to_delete = query.split()[1]
                third_term = query.split()[2]
            except Exception:
                raise InputError('Incorrect query')
            try:
                if to_delete == 'node:':
                    node_id = int(third_term[3:])
                    return GraphEngine.delete_node, {'node_id': node_id}
                elif to_delete == 'relationship:':
                    rel_id = int(third_term[3:])
                    return GraphEngine.delete_relationship, {'rel_id': rel_id}
            except Exception:
                raise InputError('Incorrect query')
        elif query_type.lower() == 'update':
            try:
                to_update = query.split()[1]
                third_term = query.split()[2]
                key, value = query.split()[3].split(':')
            except Exception:
                raise InputError('Incorrect query')
            try:
                if to_update == 'node:':
                    node_id = int(third_term[3:])
                    return GraphEngine.add_property, {'obj': (GraphEngine.select_node, {'node_id': node_id}),
                                                      'prop': Property(key, value)}
                elif to_update == 'relationship:':
                    rel_id = int(third_term[3:])
                    return GraphEngine.add_property, {'obj': (GraphEngine.select_relationship(), {'rel_id': rel_id}),
                                                      'prop': Property(key, value)}
            except Exception:
                raise InputError('Incorrect query')


class InputError(Exception):
    pass
