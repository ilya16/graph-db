from graph_db.engine.graph import Graph
from graph_db.access.parser import Parser
import os


class ConsoleReader:

    def __init__(self):
        self.temp_dir = 'temp_db/'
        self.graph = Graph('test_graph', temp_dir=self.temp_dir)
        self.parser = Parser()

    def __del__(self):
        self.graph.close_engine()
        for path in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, path))
        os.removedirs(self.temp_dir)

    def read_query(self):
        while True:
            user_input = input("\n")
            self.parser.parse_query(self.graph, user_input)
            if 'create node' in user_input:
                print('To create a node:\n' +
                      'CREATE node: label key:value')
            if 'create edge' in user_input:
                print('To create an edge:\n' +
                      'CREATE edge: label FROM node1 TO node2 key:value')
            if 'match node' in user_input:
                print('To match a node:\n' +
                      'MATCH node: label')
            if 'match edge' in user_input:
                print('To match an edge:\n' +
                      'MATCH edge: label')
            if user_input == 'exit' or user_input == 'EXIT':
                break


reader = ConsoleReader()
reader.read_query()
