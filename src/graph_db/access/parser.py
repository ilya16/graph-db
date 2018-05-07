from typing import Callable, Tuple, Dict

from graph_db.engine.graph_engine import GraphEngine
from graph_db.engine.property import Property
import re


class Parser:
    @staticmethod
    def parse_query(query) -> Tuple[Callable, Dict[str, object]]:
        properties = []
        tokens = query.split()
        query_type = tokens[0]
        query_len = len(tokens)
        if query_type.lower() == 'create':
            try:
                creation_of = tokens[1]
            except Exception:
                raise SyntaxError('Syntax Error: specify object you want to create '
                                  '(e.g. `graph`, `node`, `relationship`')
            if creation_of == 'graph:':
                try:
                    graph_label = tokens[2]
                except Exception:
                    raise SyntaxError('Syntax Error: graph label is not specified')
                return GraphEngine.create_graph, {'graph_name': graph_label}
            if creation_of == 'node:':
                try:
                    node_label = tokens[2]
                except Exception:
                    raise SyntaxError('Syntax Error: node label is not specified')
                prop_idx = 3
                while query_len > prop_idx:
                    try:
                        key, value = tokens[prop_idx].split(':')
                        if key == '' or value == '':
                            raise SyntaxError('Syntax Error: write properties as follows: key:value')
                        try:
                            value = Parser._cast_value(value)
                        except ValueError:
                            pass
                        properties.append(Property(key, value))
                    except Exception:
                        raise SyntaxError('Syntax Error: write properties as follows: key:value')
                    prop_idx += 1
                return GraphEngine.create_node, {'label_name': node_label, 'properties': properties}
            if creation_of == 'relationship:':
                if len(tokens) < 7:
                    raise SyntaxError('Syntax Error: create relationship statement is incorrect, '
                                      'nodes are not specified')
                try:
                    relationship_label = tokens[2]
                    if tokens[3] != 'from' and tokens[5] != 'to':
                        raise SyntaxError('Syntax Error: create relationship statement is incorrect, '
                                          'missing `from`/`to` clauses')
                    start_node_label = tokens[4]
                    end_node_label = tokens[6]
                    start_node_id = -1
                    end_node_id = -1
                    if 'id:' in start_node_label:
                        start_node_id = int(start_node_label[3:])
                    if 'id:' in end_node_label:
                        end_node_id = int(end_node_label[3:])
                except Exception:
                    raise SyntaxError('Syntax Error: create relationship statement is incorrect')
                try:
                    prop_idx = 7
                    while query_len > prop_idx:
                        try:
                            key, value = tokens[prop_idx].split(':')
                            if key == '' or value == '':
                                raise SyntaxError('Syntax Error: write properties as follows: key:value')
                            try:
                                value = Parser._cast_value(value)
                            except ValueError:
                                pass
                            properties.append(Property(key, value))
                        except Exception:
                            raise SyntaxError('Syntax Error: write properties as follows: key:value')
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
                    raise SyntaxError('Either you haven\'t created entered label or entered label is incorrect')
            raise SyntaxError('Syntax Error: `create` statement and object types are incorrect,'
                              'valid ones are `graph`, `node`, `relationship`')
        elif query_type.lower() == 'match':
            try:
                match_of = tokens[1]
            except Exception:
                raise SyntaxError('Syntax Error: specify object you want to match '
                                  '(e.g. `graph`, `node`, `relationship`')
            try:
                third_term = tokens[2]
            except Exception:
                raise SyntaxError('Syntax Error: object label is not specified')
            try:
                if match_of == 'node:' or match_of == 'relationship:':
                    symbols = ['=', '>', '<', '>=', '<=']
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
                                raise SyntaxError('Syntax Error: write properties as follows: key:value')
                            try:
                                value = Parser._cast_value(value)
                            except ValueError:
                                if sign[0] in symbols[1:]:
                                    raise SyntaxError('Syntax Error: Property value is not a number')
                            if match_of == 'node:':
                                return GraphEngine.select_nodes, {'prop_key': key,
                                                                  'prop_value': value,
                                                                  'query_cond': sign[0]}
                            elif match_of == 'relationship:':
                                return GraphEngine.select_relationships, {'prop_key': key,
                                                                          'prop_value': value,
                                                                          'query_cond': sign[0]}
                        else:
                            raise SyntaxError('Syntax Error: condition type is incorrect')
                if match_of == 'graph:':
                    return GraphEngine.select_graph_objects, {}
                raise SyntaxError('Syntax Error: `match` statement and object types are incorrect,'
                                  'valid ones are `graph`, `node`, `relationship`')
            except Exception:
                raise SyntaxError('Syntax Error: `match` statement is incorrect')
        elif query_type.lower() == 'delete':
            try:
                to_delete = tokens[1]
                third_term = tokens[2]
            except Exception:
                raise SyntaxError('Syntax Error: `delete` statement is incorrect')
            try:
                if to_delete == 'node:':
                    node_id = int(third_term[3:])
                    return GraphEngine.delete_node, {'node_id': node_id}
                elif to_delete == 'relationship:':
                    rel_id = int(third_term[3:])
                    return GraphEngine.delete_relationship, {'rel_id': rel_id}
            except Exception:
                raise SyntaxError('Syntax Error: `delete` statement is incorrect')
        elif query_type.lower() == 'update':
            try:
                to_update = tokens[1]
                third_term = tokens[2]
                key, value = tokens[3].split(':')
                if key == '' or value == '':
                    raise SyntaxError('Syntax Error: write properties as follows: key:value')
                try:
                    value = Parser._cast_value(value)
                except ValueError:
                    pass
            except Exception:
                raise SyntaxError('Syntax Error: `update` statement is incorrect')
            try:
                if to_update == 'node:':
                    node_id = int(third_term[3:])
                    return GraphEngine.add_property, {'obj': (GraphEngine.select_node, {'node_id': node_id}),
                                                      'prop': Property(key, value)}
                elif to_update == 'relationship:':
                    rel_id = int(third_term[3:])
                    return GraphEngine.add_property, {'obj': (GraphEngine.select_relationship, {'rel_id': rel_id}),
                                                      'prop': Property(key, value)}
            except Exception:
                raise SyntaxError('Syntax Error: `update` statement is incorrect')
        else:
            raise SyntaxError('Syntax Error: Query type is incorrect. '
                              'Try either `create`, `match`, `delete` or `update`')

    @staticmethod
    def _cast_value(value):
        try:
            value = int(value)
        except ValueError:
            try:
                value = float(value)
            except ValueError as e:
                raise e
        return value
