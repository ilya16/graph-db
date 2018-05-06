from graph_db.access import db
from graph_db.access.parser import InputError
import sys
import readline

HELP = "Query examples:\n" \
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
        self.db = db.connect(base_dir)
        self.cursor = self.db.cursor()

    def read_query(self):
        print("Welcome to Graph DB. (c) Ilya Borovik, Artur Khayaliev, Boris Makaev\n\n"
              "You can enter '/help' to see query examples.\n")
        while True:
            user_input = input("\n")
            if len(user_input) != 0:
                if '/help' in user_input:
                    print(HELP)
                    continue
                if user_input == 'exit':
                    break
                if 'create graph' not in user_input and self.db.get_graph() is None:
                    print('You have to create a graph firstly using \'create graph: label\'')
                    continue
                else:
                    if not self.db.get_graph():
                        try:
                            self.cursor.execute(user_input)
                        except InputError as e:
                            print(e)
                    else:
                        if 'create graph' in user_input:
                            print(f"You have already created a graph called "
                                  f"'{self.db.get_graph().get_name()}'")
                            continue
                        try:
                            self.cursor.execute(user_input)
                            result = self.cursor.fetch_all()
                            for r in result:
                                print(r)
                        except InputError as e:
                            print(e)


def run():
    base_dir = 'temp_db/'
    if len(sys.argv) > 1:
        base_dir = sys.argv[1]
    print(base_dir)
    reader = ConsoleReader()
    reader.read_query()