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
