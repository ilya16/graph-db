from graph_db.access.cursor import Cursor
from graph_db.engine.api import EngineAPI
from graph_db.engine.graph_engine import GraphEngine


class GraphDB:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.graph_engine: EngineAPI = GraphEngine(config_path)

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


def connect(config_path: str):
    db = GraphDB(config_path)
    return db
