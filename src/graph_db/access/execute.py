from typing import Callable

from graph_db.engine.api import EngineAPI


class QueryExecutor:
    @staticmethod
    def execute(engine: EngineAPI,
                func: Callable,
                **params) -> object:
        if func:
            return QueryExecutor.execute_recursive(engine, func, **params)
        else:
            print('Incorrect execute call')

    @staticmethod
    def execute_recursive(engine: EngineAPI,
                          func: Callable,
                          **params):
        for key in params:
            if isinstance(params[key], tuple):
                result = QueryExecutor.execute_recursive(engine, params[key][0], **params[key][1])
                if isinstance(result, list):
                    # should be executed for each obj in the list, but now only for the first one
                    result = result[0]
                params[key] = result

        return func(engine, **params)
