##
# File:    ReferenceSequenceAssignmentProviderTests.py
# Author:  J. Westbrook
# Date:    17-Oct-2019
#
# Updates:
#
##
"""
Tests for reference sequence assignment update operations
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

from rcsb.exdb.seq.ReferenceSequenceAssignmentProvider import ReferenceSequenceAssignmentProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ReferenceSequenceAssignmentProviderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(ReferenceSequenceAssignmentProviderTests, self).__init__(methodName)
        self.__verbose = True
        self.__traceMemory = False

    def setUp(self):
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        self.__resourceName = "MONGO_DB"
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        # self.__testEntityCacheKwargs = {"fmt": "json", "indent": 3}
        self.__testEntityCacheKwargs = {"fmt": "pickle"}
        self.__fetchLimitTest = None
        self.__useCache = False
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testAssignmentProvider(self):
        """Test case - create and read cache reference sequences assignments and related data.

        Some profiling statistics -
         Current memory usage is 0.711864MB; Peak was 4646.476926MB (full cache no limit)
         Current memory usage is 1.080839MB; Peak was 1258.231275MB (163)
         Current memory usage is 0.874476MB; Peak was 1920.689116MB (918)
         Current memory usage is 0.937091MB; Peak was 2086.910197MB (2739)
         Current memory usage is 1.3539 MB; Peak was 2300.5170 MB (10000)
         Current memory usage is 1.3517 MB; Peak was 2714.5467 MB (20K entries)
        """
        try:
            #  -- create cache ---
            rsaP = ReferenceSequenceAssignmentProvider(
                self.__cfgOb,
                databaseName="pdbx_core",
                collectionName="pdbx_core_polymer_entity",
                polymerType="Protein",
                referenceDatabaseName="UniProt",
                provSource="PDB",
                useCache=self.__useCache,
                cachePath=self.__cachePath,
                cacheKwargs=self.__testEntityCacheKwargs,
                fetchLimit=self.__fetchLimitTest,
                siftsAbbreviated="TEST",
            )
            ok = rsaP.testCache()
            self.assertTrue(ok)
            numRef = rsaP.getRefDataCount()
            self.assertGreaterEqual(numRef, 49)
            #
            # ---  Reload from cache ---
            rsaP = ReferenceSequenceAssignmentProvider(
                self.__cfgOb, referenceDatabaseName="UniProt", useCache=True, cachePath=self.__cachePath, cacheKwargs=self.__testEntityCacheKwargs
            )
            ok = rsaP.testCache()
            self.assertTrue(ok)
            numRef = rsaP.getRefDataCount()
            self.assertGreaterEqual(numRef, 49)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def referenceSequenceAssignmentProviderSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ReferenceSequenceAssignmentProviderTests("testAssignmentProvider"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = referenceSequenceAssignmentProviderSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
