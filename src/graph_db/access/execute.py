from typing import Callable

from graph_db.engine.graph_engine import GraphEngine


class QueryExecutor:
    @staticmethod
    def execute(graph_engine: GraphEngine,
                func: Callable,
                **params) -> object:
        if func:
            return QueryExecutor.execute_recursive(graph_engine, func, **params)
        else:
            print('Incorrect execute call')

    @staticmethod
    def execute_recursive(graph_engine: GraphEngine,
                          func: Callable,
                          **params):
        for key in params:
            if isinstance(params[key], tuple):
                result = QueryExecutor.execute_recursive(graph_engine, params[key][0], **params[key][1])
                if isinstance(result, list):
                    # should be executed for each obj in the list, but now only for the first one
                    result = result[0]
                params[key] = result

        return func(graph_engine, **params)
