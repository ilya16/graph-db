from typing import Callable, Tuple

from graph_db.access.result import ResultSet
from graph_db.engine.api import EngineAPI


class QueryExecutor:
    @staticmethod
    def execute(engine: EngineAPI,
                func: Callable,
                **params) -> ResultSet:
        if func:
            result = QueryExecutor.execute_recursive(engine, func, **params)
            if isinstance(result, list):
                result_set = ResultSet(result)
            else:
                result_set = ResultSet([result])
            result_set.set_message(QueryExecutor._build_message(func, result_set))
            return result_set
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

    @staticmethod
    def _build_message(func: Callable, result_set: ResultSet):
        if func.__name__.startswith('create'):
            return f'created 1 {result_set[0].__class__.__qualname__}.'
        elif func.__name__.startswith('select'):
            return f'found {len(result_set)} object(-s).'
        elif func.__name__.startswith('add'):
            return f'updated {len(result_set)} object(-s).'
        elif func.__name__.startswith('delete'):
            return f'deleted {len(result_set)} object(-s).'
        else:
            return f'affected {len(result_set)} object(-s).'
