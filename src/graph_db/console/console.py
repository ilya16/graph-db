import os.path

from graph_db.access import db
import sys
import readline

from graph_db.engine.error import GraphEngineError
from graph_db.engine.types import DFS_CONFIG_PATH

GREETING = """
Welcome to Graph DB. (c) Ilya Borovik, Artur Khayaliev, Boris Makaev
Use `help` to see query examples.
Use `exit` to close database connection.
"""

HELP = """
Query examples:
create graph: label
create node: label
create node: label key:value
create node: label key:value key:value key:value
create relationship: label from label1 to label2
create relationship: label from label1 to label2 key:value
create relationship: label from id:0 to id:1 key:value
match node: label
match node: id:0
match relationship: label
match relationship: id:1
match node: key=value
match node: key<value
match relationship: key>=value
match graph: label
delete node: id:0
delete relationship: id:0
"""


class ConsoleReader:
    def __init__(self, base_dir: str = 'temp_db/'):
        print("Application is starting...")
        self.base_dir = base_dir
        print('Setting up connections with worker machines...')
        self.db = db.connect(base_dir)
        print('Connection has been set up!\n')
        self.cursor = self.db.cursor()

    def read_query(self):
        print(GREETING)
        while True:
            user_input = input("/")
            if len(user_input) != 0:
                if 'help' in user_input:
                    print(HELP)
                    continue
                if user_input == 'exit':
                    self.db.close()
                    break
                if 'create graph' not in user_input and self.db.get_graph() is None:
                    print('You have to create a graph firstly using `create graph: label`')
                    continue
                else:
                    if not self.db.get_graph():
                        try:
                            self.cursor.execute(user_input)
                            result = self.cursor.fetch_one()
                            print(f"You have created graph called '{result.get_name()}' ")
                        except (SyntaxError, GraphEngineError) as e:
                            print(e)
                    else:
                        if 'create graph' in user_input:
                            print(f"You have already created a graph called '{self.db.get_graph().get_name()}'")
                            continue
                        try:
                            self.cursor.execute(user_input)
                            result = self.cursor.fetch_all()
                            for r in result:
                                print(r)
                        except (SyntaxError, GraphEngineError) as e:
                            print(e)


def run():
    config_path = DFS_CONFIG_PATH
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    if not os.path.isfile(config_path) and not os.path.isfile('../../../' + config_path):
        print(f'Path `{config_path}` to DFS configuration file is incorrect')
        return
    reader = ConsoleReader(config_path)
    reader.read_query()


if __name__ == '__main__':
    run()
