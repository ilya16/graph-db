from graph_db.access.cursor import Cursor
from graph_db.engine.api import EngineAPI
from graph_db.engine.graph_engine import GraphEngine


class GraphDB:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.graph_engine: EngineAPI = GraphEngine(base_dir)

    def cursor(self):
        return Cursor(self.graph_engine)

    def close(self):
        self.graph_engine.close()

    def get_graph(self):
        return self.graph_engine.get_graph()

    def get_stats(self):
        return self.graph_engine.get_stats()

    def get_engine(self):
        return self.graph_engine


def connect(base_dir: str):
    db = GraphDB(base_dir)
    return db
