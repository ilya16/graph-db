from graph_db.access.execute import QueryExecutor
from graph_db.access.parser import Parser, InputError
import os
import sys

from graph_db.engine.graph_engine import GraphEngine

greeting = "Query examples:\n" \
           "create graph: label\n" \
           "create node: label\n" \
           "create node: label key:value\n" \
           "create node: label key:value key:value key:value\n" \
           "create relationship: label from label1 to label2\n" \
           "create relationship: label from label1 to label2 key:value\n" \
           "create relationship: label from id:0 to id:1 key:value\n" \
           "match node: label\n" \
           "match node: id:0\n" \
           "match relationship: label\n" \
           "match relationship: id:1\n" \
           "match node: key=value\n" \
           "match node: key<value\n" \
           "match relationship: key>=value\n" \
           "match graph: label\n" \
           "delete node: id:0\n" \
           "delete relationship: id:0\n" \
           "'exit' to close\n"


class ConsoleReader:

    def __init__(self, base_dir: str = 'temp_db/'):
        self.base_dir = base_dir
        self.graph_engine = GraphEngine(self.base_dir)
        self.parser = Parser()
        self.query_executor = QueryExecutor()

    def __del__(self):
        try:
            self.graph_engine.close()
            for path in os.listdir(self.base_dir):
                os.remove(os.path.join(self.base_dir, path))
            os.removedirs(self.base_dir)
        except:
            print("You didn't enter any query.")

    def read_query(self):
        print("Welcome to Graph DB. (c) Ilya Borovik, Artur Khayaliev, Boris Makaev\n\n"
              "You can enter '/help' to see query examples.\n")
        while True:
            user_input = input("\n")
            if len(user_input) != 0:
                if '/help' in user_input:
                    print(greeting)
                    continue
                if user_input == 'exit':
                    break
                if 'create graph' not in user_input and self.graph_engine.get_graph() is None:
                    print('You have to create a graph firstly using \'create graph: label\'')
                    continue
                else:
                    if not self.graph_engine.get_graph():
                        try:
                            func, params = self.parser.parse_query(user_input)
                            self.query_executor.execute(self.graph_engine, func, **params)
                        except InputError as e:
                            print(e)
                    else:
                        if 'create graph' in user_input:
                            print(f"You have already created a graph called "
                                  f"'{self.graph_engine.get_graph().get_name()}'")
                            continue
                        try:
                            func, params = self.parser.parse_query(user_input)
                            result = self.query_executor.execute(self.graph_engine, func, **params)
                            if isinstance(result, list):
                                print('\n'.join(result))
                            else:
                                print(result)
                        except InputError as e:
                            print(e)


if __name__ == '__main__':
    base_dir = 'temp_db/'
    if sys.argv:
        base_dir = sys.argv[0]

    reader = ConsoleReader()
    reader.read_query()
