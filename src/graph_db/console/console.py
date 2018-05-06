from graph_db.access import db
import sys
import readline

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
`/exit` to close
"""


class ConsoleReader:
    def __init__(self, base_dir: str = 'temp_db/'):
        self.base_dir = base_dir
        self.db = db.connect(base_dir)
        self.cursor = self.db.cursor()

    def read_query(self):
        print("Welcome to Graph DB. (c) Ilya Borovik, Artur Khayaliev, Boris Makaev\n\n"
              "You can enter `/help` to see query examples.\n")
        while True:
            user_input = input("\n")
            if len(user_input) != 0:
                if '/help' in user_input:
                    print(HELP)
                    continue
                if user_input == '/exit':
                    self.db.close()
                    break
                if 'create graph' not in user_input and self.db.get_graph() is None:
                    print('You have to create a graph firstly using \'create graph: label\'')
                    continue
                else:
                    if not self.db.get_graph():
                        try:
                            self.cursor.execute(user_input)
                            result = self.cursor.fetch_one()
                            print(f"You have created graph called '{result.get_name()}' ")
                        except SyntaxError as e:
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
                        except SyntaxError as e:
                            print(e)


def run():
    base_dir = 'temp_db/'
    if len(sys.argv) > 1:
        base_dir = sys.argv[1]
    reader = ConsoleReader(base_dir)
    reader.read_query()


if __name__ == '__main__':
    run()
