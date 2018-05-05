from graph_db.access.execute import QueryExecutor
from graph_db.access.parser import Parser, InputError
from graph_db.access.result import ResultSet
from graph_db.engine.graph_engine import GraphEngine


class Cursor:
    def __init__(self, graph_engine: GraphEngine):
        self.graph_engine = graph_engine
        self.parser = Parser()
        self.query_executor = QueryExecutor()
        self.result_set = None

    def execute(self, query: str):
        try:
            func, params = self.parser.parse_query(query)
            result = self.query_executor.execute(self.graph_engine, func, **params)
            if isinstance(result, list):
                self.result_set = ResultSet(result)
            else:
                self.result_set = ResultSet([result])
        except InputError as e:
            raise e

    def fetch_all(self):
        return self.result_set

    def count(self):
        return len(self.result_set)
