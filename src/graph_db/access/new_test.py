import networkx as nx
from graph_db.access import grypher


g = nx.MultiDiGraph()
query = 'CREATE (n:SOMECLASS) RETURN n'
test_parser = grypher.CypherToNetworkx()
test_parser.query(g, query)

g = nx.MultiDiGraph()
create_query = 'CREATE (n:SOMECLASS {foo: "bar"}) RETURN n'
match_query = 'MATCH (n) RETURN n.foo'
test_parser = grypher.CypherToNetworkx()
list(test_parser.query(g, create_query))
# out = list(test_parser.query(g, match_query))
# self.assertEqual(out[0], ['bar'])