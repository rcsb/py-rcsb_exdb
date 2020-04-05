# File: setup.py
# Date: 23-Feb-2019
#
# Updates:
#
#
import re

from setuptools import find_packages
from setuptools import setup

packages = []
thisPackage = "rcsb.exdb"

with open("rcsb/exdb/cli/__init__.py", "r") as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError("Cannot find version information")

setup(
    name=thisPackage,
    version=version,
    description="RCSB Python ExDB extraction and loading workflows",
    long_description="See:  README.md",
    author="John Westbrook",
    author_email="john.westbrook@rcsb.org",
    url="https://github.com/rcsb/py-rcsb_exdb",
    #
    license="Apache 2.0",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    entry_points={"console_scripts": ["exdb_exec_cli=rcsb.exdb.cli.ExDbExec:main"]},
    #
    install_requires=[
        "jsonschema >= 2.6.0",
        "numpy",
        "rcsb.utils.io >= 0.57",
        "rcsb.db >= 0.973",
        "rcsb.utils.chemref >= 0.33",
        "rcsb.utils.citation >= 0.15",
        "rcsb.utils.config >= 0.33",
        "rcsb.utils.seq >= 0.41",
        "rcsb.utils.ec >= 0.21",
        "rcsb.utils.go >= 0.17",
        "rcsb.utils.struct >= 0.24",
        "rcsb.utils.taxonomy >= 0.27",
        'statistics; python_version < "3.0"',
    ],
    packages=find_packages(exclude=["rcsb.mock-data", "rcsb.exdb.tests-anal", "rcsb.exdb.tests-*", "tests.*"]),
    package_data={
        # If any package contains *.md or *.rst ...  files, include them:
        "": ["*.md", "*.rst", "*.txt", "*.cfg"]
    },
    #
    test_suite="rcsb.exdb.tests",
    tests_require=["tox"],
    #
    # Not configured ...
    extras_require={"dev": ["check-manifest"], "test": ["coverage"]},
    # Added for
    command_options={"build_sphinx": {"project": ("setup.py", thisPackage), "version": ("setup.py", version), "release": ("setup.py", version)}},
    # This setting for namespace package support -
    zip_safe=False,
)
