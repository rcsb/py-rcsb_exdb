##
# File:    LigandNeighborMappingProviderTests.py
# Author:  J. Westbrook
# Date:    28-Jun-2021
#
# Updates:
#
##
"""
Tests for

"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import platform
import resource
import time
import unittest

from rcsb.exdb.seq.LigandNeighborMappingProvider import LigandNeighborMappingProvider

from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class LigandNeighborMappingProviderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(LigandNeighborMappingProviderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testLigandNeighborMapping(self):
        """Test case - load and access ligand neighbor mapping cache"""
        try:
            crmP = LigandNeighborMappingProvider(self.__cachePath, useCache=True)
            ok = crmP.testCache()
            self.assertTrue(ok)
            #
            ok = crmP.fetchLigandNeighborMapping(self.__cfgOb)
            self.assertTrue(ok)
            crmP = LigandNeighborMappingProvider(self.__cachePath, useCache=True)
            ok = crmP.testCache(minCount=2)
            self.assertTrue(ok)
            nL = crmP.getLigandNeighbors("3VFJ_2")
            self.assertGreaterEqual(len(nL), 4)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def ligandNeighborMappingSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(LigandNeighborMappingProviderTests("testLigandNeighborMapping"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = ligandNeighborMappingSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
