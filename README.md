# py-rcsb_exdb

[![Build Status](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_apis/build/status/rcsb.py-rcsb_exdb?branchName=master)](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_build/latest?definitionId=18&branchName=master)

RCSB exchange database extraction and loading workflow tools

## Introduction

This module contains a collection of utility classes for extracting data from
the RCSB exchange database and subsequently reloading processed or integrated data.

### Installation

Download the library source software from the project repository:

```bash

git clone --recurse-submodules https://github.com/rcsb/py-rcsb_exdb.git

```

Optionally, run test suite (Python versions 2.7 and 3.7) using
[setuptools](https://setuptools.readthedocs.io/en/latest/) or
[tox](http://tox.readthedocs.io/en/latest/example/platform.html):

```bash
python setup.py test

or simply run

tox
```

Installation is via the program [pip](https://pypi.python.org/pypi/pip).  To run tests
from the source tree, the package must be installed in editable mode (i.e. -e):

```bash
pip install -r requirements.txt   # OR:   pip install -i https://pypi.anaconda.org/OpenEye/simple OpenEye-toolkits

pip install -e .
```

#### Installing in Ubuntu Linux (tested in 18.04)

You will need a few packages, before `pip install .` can work:

```bash

sudo apt install flex bison

```

### Installing on macOS

To use and develop this package on macOS requires a number of packages that are not
distributed as part of the base macOS operating system.
The following steps provide one approach to creating the development environment for this
package.  First, install the Apple [XCode](https://developer.apple.com/xcode/) package and associate command-line tools.
This will provide essential compilers and supporting tools.  The [HomeBrew](https://brew.sh/) package
manager provides further access to a variety of common open source services and tools.
Follow the instructions provided by at the [HomeBrew](https://brew.sh/) site to
install this system.   Once HomeBrew is installed, you can further install the
[MongoDB](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-os-x/) packages which
are required to support the ExDB tools.  HomeBrew also provides a variety of options for
managing a [Python virtual environments](https://gist.github.com/Geoyi/f55ed54d24cc9ff1c14bd95fac21c042).

### Command Line Interfaces

A convenience CLI `exdb_exec_cli` is provided for performing update and loading operations.

```bash
exdb_exec_cli --help

usage: exdb_exec_cli [-h] [--data_set_id DATA_SET_ID] [--full] [--etl_chemref]
                     [--etl_tree_node_lists] [--config_path CONFIG_PATH]
                     [--config_name CONFIG_NAME] [--db_type DB_TYPE]
                     [--read_back_check] [--num_proc NUM_PROC]
                     [--chunk_size CHUNK_SIZE]
                     [--document_limit DOCUMENT_LIMIT] [--debug] [--mock]
                     [--cache_path CACHE_PATH] [--rebuild_cache]

optional arguments:
  -h, --help            show this help message and exit
  --data_set_id DATA_SET_ID
                        Data set identifier (default= 2019_14 for current
                        week)
  --full                Fresh full load in a new tables/collections (Default)
  --etl_chemref         ETL integrated chemical reference data
  --etl_tree_node_lists
                        ETL tree node lists
  --config_path CONFIG_PATH
                        Path to configuration options file
  --config_name CONFIG_NAME
                        Configuration section name
  --db_type DB_TYPE     Database server type (default=mongo)
  --read_back_check     Perform read back check on all documents
  --num_proc NUM_PROC   Number of processes to execute (default=2)
  --chunk_size CHUNK_SIZE
                        Number of files loaded per process
  --document_limit DOCUMENT_LIMIT
                        Load document limit for testing
  --debug               Turn on verbose logging
  --mock                Use MOCK repository configuration for testing
  --cache_path CACHE_PATH
                        Top cache path for external and local resource files
  --rebuild_cache       Rebuild cached files from remote resources
________________________________________________________________________________

```

For example, to construct and load tree nodes list data collections, the following
command may be used:

```bash
exdb_exec_cli --mock --full --etl_tree_node_lists --rebuild_cache \
              --cache_path ./CACHE  \
              --config_path ./rcsb/mock-data/config/dbload-setup-example.yml \
              --config_name site_info_configuration >& LOGTREE \
```
