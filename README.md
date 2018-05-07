# not4oundGraph

Simple Distributed Graph Database Management System written on `Python` as the project in Advanced Databases course at Innopolis University.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

Clone repository on your local machine using:

```
git clone https://github.com/ilya16/graph-db.git
```

**Python 3.6** interpreter is required and recommended to install and run the project.

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

**Note:** We use [rpyc](https://github.com/tomerfiliba/rpyc) package for building distributing file system and communication between peers. It must be installed from ***setup.py***, but in case of any problems, please, manually run:

```
pip install rpyc
```


## Running the tests

To run tests use:
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

Tests affect various layers of the Graph Database:
* Valid/Invalid queries
* Create/Match/Update/Delete operations on nodes, relationships, labels, properteis
* Reading existing graph from disk
* Main memory cache
* Distributed file system communication

## Deployment

To deploy ***Graph Database*** simply use
```
n4Graph [conf_path]
```
where `conf_path` stands for the path to configuration file that describes peers of distributed file system including managers and workers (slaves). 

### Distributed File System
DFS configuration file has the following JSON format:

```
{
  "manager_config" : {
    "ip": "localhost",
    "port": 2131,
    "db_path": "db/",
    "workers": [
      {
        "ip": "localhost",
        "port": 8888,
        "stores": {
          "NodeStorage": true,
          "RelationshipStorage": true,
          "LabelStorage": true,
          "PropertyStorage": true,
          "DynamicStorage": true
        }
      }
    ],
    "dfs_mode": {
      "Replicate": true,
      "Distribute": false
    },
    "replica_factor": 2
  }
}
```

Without specifying any arguments, the default [configuration file](configs/config.json) with `replica_factor = 2` is used.

***not4oundGraph*** supports two DFS modes:
* **Replication** of data across multiple workers, each worker contains the same portion of data (example configuration file: [config.json](configs/config.json))
* **Distribution** of data across multiple workers using Round-Robin algorithm (example configuration file: [config.json](configs/config_distributed.json))

### Graph Engine API
We provide an [API](src/graph_db/engine/api.py) for managing graph database system directly from Python code. Supported graph operations are listed in [specifications](SPECIFICATIONS.md).

### Data Access API (Query Language)
We provide own simple graph query language ***not4oundQL*** for executing queries and a [wrapper](src/graph_db/access/db.py) around it. 
Language specification can found in ***not4oundGraph*** [specifications](SPECIFICATIONS.md).

Executable file `n4Graph` runs [console](src/graph_db/console/console.py) mode and accepts *not4oundQL* queries:
```
Welcome to not4oundGraph DB. (c) Ilya Borovik, Artur Khayaliev, Boris Makaev

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

**Note:** *not4oundGraph* DB does not support `delete graph` queries. If you want to delete graph, please, delete the `db_path` directory with data that you have specified in configuration file.
## Built With

* Python 3.6
* [rpyc](https://github.com/tomerfiliba/rpyc) (for distibuted file system)

## Authors

* **Ilya Borovik** - [ilya16](https://github.com/ilya16/)
* **Artur Khayaliev** - [zytfo](https://github.com/zytfo)
* **Boris Makaev** - [borisqa](https://github.com/Borisqa)

## License

This project is licensed under the MIT License - see the [LICENSE.md](https://github.com/ilya16/graph-db/blob/readme/LICENSE.md) file for details

## Acknowledgments

* Graph Database implementation is inspired by [Neo4j Graph Platform](https://neo4j.com/)
* Distributed File System implementation is inspired by [PyDFS](https://github.com/iyidgnaw/PyDFS)

