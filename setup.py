#!/usr/bin/env python

import sys
from os import path
from setuptools import setup, find_packages

sys.path.append(path.join(path.dirname(__file__), 'src'))

from graph_db import __version__ as version

setup(
    name='Graph DB',
    version=version,
    description='Simple Graph Database',
    long_description=open(path.join(path.dirname(__file__), 'README.md')).read(),
    author='ilya16, zyfto & Borisqa',
    url='https://github.com/ilya16/graph-db',
    packages=find_packages('src', include=['graph_db', 'graph_db.*']),
    package_dir={'graph_db': 'src/graph_db'},
    entry_points={
        'console_scripts':
            ['graphDB = graph_db.console.console:run']
    },
    test_suite='src.tests',
)
