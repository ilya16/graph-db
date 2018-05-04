from graph_db.engine.graph import Graph
from graph_db.access.parser import Parser
import os

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

    def __init__(self):
        self.temp_dir = 'temp_db/'
        self.graph = None
        self.parser = Parser()
        self.is_created = False

    def __del__(self):
        try:
            self.graph.close_engine()
            for path in os.listdir(self.temp_dir):
                os.remove(os.path.join(self.temp_dir, path))
            os.removedirs(self.temp_dir)
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
                if 'create graph' not in user_input and self.is_created is False:
                    print('You have to create a graph firstly using \'create graph: label\'')
                    continue
                else:
                    if self.is_created is False:
                        self.graph = self.parser.parse_query(None, user_input)
                        if self.graph is not None:
                            self.is_created = True
                    else:
                        if 'create graph' in user_input:
                            print("You have already created a graph called '" + self.graph.name + "'")
                            continue
                        self.parser.parse_query(self.graph, user_input)


reader = ConsoleReader()
reader.read_query()
