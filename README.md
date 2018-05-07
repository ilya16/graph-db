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


```
## Built With

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

* **Billie Thompson** - *Initial work* - [PurpleBooth](https://github.com/PurpleBooth)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone who's code was used
* Inspiration
* etc
