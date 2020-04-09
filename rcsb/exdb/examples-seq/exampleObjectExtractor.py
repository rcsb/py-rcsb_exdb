##
# File:    ObjectExtractorTests.py
# Author:  J. Westbrook
# Date:    25-Apr-2019
#
# Updates:
#
##
"""
Tests for extractor selected values from collections (limited tests from mock-data repos)
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os

import time
import unittest

from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ObjectExtractorTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(ObjectExtractorTests, self).__init__(methodName)
        self.__verbose = False

    def setUp(self):
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        #
        configName = "site_info_remote_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        self.__workPath = "."
        self.__mU = MarshalUtil(workPath=self.__workPath)
        self.__entityTaxonPath = os.path.join(self.__workPath, "entity_taxon.tdd")
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testExtractEntityTaxonomyContent(self):
        """ Test case - extract unique entity source and host taxonomies
        """
        tL = []
        try:
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName="pdbx_core",
                collectionName="pdbx_core_polymer_entity",
                useCache=False,
                keyAttribute="entity",
                uniqueAttributes=["rcsb_id"],
                selectionQuery=None,
                selectionList=["rcsb_id", "rcsb_entity_source_organism.ncbi_taxonomy_id", "rcsb_entity_host_organism.ncbi_taxonomy_id"],
            )
            eCount = obEx.getCount()
            logger.info("Polymer entity count is %d", eCount)
            objD = obEx.getObjects()
            sD = {}
            hD = {}
            for rId, eD in objD.items():
                try:
                    for tD in eD["rcsb_entity_source_organism"]:
                        sD.setdefault(rId, []).append(str(tD["ncbi_taxonomy_id"]))

                except Exception:
                    pass
                try:
                    for tD in eD["rcsb_entity_host_organism"]:
                        hD.setdefault(rId, []).append(str(tD["ncbi_taxonomy_id"]))
                except Exception:
                    pass
            for rId, taxIdL in sD.items():
                tS = "|".join(sorted(set(taxIdL)))
                if tS:
                    lS = "%s\t%s" % (rId, "|".join(sorted(set(taxIdL))))
                tL.append(lS)
            self.__mU.doExport(self.__entityTaxonPath, tL, fmt="list")
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def objectExtractorSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ObjectExtractorTests("testExtractEntityTaxonomyContent"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = objectExtractorSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
