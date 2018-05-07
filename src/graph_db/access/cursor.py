from graph_db.access.execute import QueryExecutor
from graph_db.access.parser import Parser
from graph_db.access.result import ResultSet
from graph_db.engine.api import EngineAPI


class Cursor:
    def __init__(self, graph_engine: EngineAPI):
        self.graph_engine = graph_engine
        self.parser = Parser()
        self.query_executor = QueryExecutor()
        self.result_set = None

    def execute(self, query: str):
        try:
            func, params = self.parser.parse_query(query)
            self.result_set = self.query_executor.execute(self.graph_engine, func, **params)
        except SyntaxError as e:
            raise e

    def fetch_all(self) -> ResultSet:
        return self.result_set

    def fetch_one(self):
        return self.result_set[0]

    def count(self):
        return len(self.result_set)
