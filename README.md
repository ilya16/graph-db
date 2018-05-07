# Graph Database

This is a repository with implementation of Graph Database Engine on Python as the project in Advanced Databases course at Innopolis University.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

You have to clone our repository on your local machine

```
git clone https://github.com/ilya16/graph-db.git
```

You should have also **Python 3.6** as your main interpreter

### Installing

To install Graph Database on you local machine

```
cd graph-db/
python setup.py install
```

```
...
Using /anaconda3/lib/python3.6/site-packages
Searching for plumbum==1.6.6
Best match: plumbum 1.6.6
Adding plumbum 1.6.6 to easy-install.pth file

Using /anaconda3/lib/python3.6/site-packages
Finished processing dependencies for Graph-DB==0.1
```

We use [rpyc](https://github.com/tomerfiliba/rpyc) for distributing file system. It must be installed from ***setup.py***,
but in case of any problems, try manual install

```
pip install rpyc
```


## Running the tests

To run tests
```
python setup.py test
```

```
...
test_queries_invalid (src.tests.access.test_parser.ParserCase) ... ok
test_cache_clear (src.tests.engine.test_graph_engine.IOEngineCase) ... ok
test_deletions (src.tests.engine.test_graph_engine.IOEngineCase) ... ok
test_nodes_and_labels (src.tests.engine.test_graph_engine.IOEngineCase) ... ok
test_properties (src.tests.engine.test_graph_engine.IOEngineCase) ... ok
test_relationships (src.tests.engine.test_graph_engine.IOEngineCase) ... ok
test_simple_case (src.tests.engine.test_graph_engine.IOEngineCase) ... ok
test_simple_case_from_disk (src.tests.engine.test_graph_engine.IOEngineCase) ... ok

----------------------------------------------------------------------
Ran 9 tests in 0.064s
```

### Test explanation

There are a lof of tests which tests our system in different aspects:
* Valid/Invalid queries
* Clearing the cache
* Deletions
* Nodes and labels
* Properties
* Relationships
* Reading from disk
* ...

## Deployment

To deploy ***Graph Database*** simply use
```
graphDB
```
```
Welcome to Graph DB. (c) Ilya Borovik, Artur Khayaliev, Boris Makaev

You can enter `/help` to see query examples.


/help

Query examples:
create graph: label
create node: label
create node: label key:value
create node: label key:value key:value key:value
create relationship: label from label1 to label2
create relationship: label from label1 to label2 key:value
create relationship: label from id:0 to id:1 key:value
...
```
## Built With

* [rpyc](https://github.com/tomerfiliba/rpyc) - Distributed File System used

## Authors

* **Ilya Borovik** - [ilya16](https://github.com/ilya16/)
* **Artur Khayaliev** - [zytfo](https://github.com/zytfo)
* **Boris Makaev** - [borisqa](https://github.com/Borisqa)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Implementation of distributed file system was inspired by [PyDFS](https://github.com/iyidgnaw/PyDFS)
* We also were inspired by [Neo4j Graph Platform](https://neo4j.com/)
