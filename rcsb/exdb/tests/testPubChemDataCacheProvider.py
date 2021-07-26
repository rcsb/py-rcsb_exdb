##
# File:    PubChemDataCacheProviderTests.py
# Author:  J. Westbrook
# Date:    17-Jul-2020
#
# Updates:
#
##
"""
Tests for reference data cache maintenance operations
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

from rcsb.exdb.chemref.PubChemDataCacheProvider import PubChemDataCacheProvider

from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class PubChemDataCacheProviderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(PubChemDataCacheProviderTests, self).__init__(methodName)
        self._verbose = True

    def setUp(self):
        #
        self.__cidList = ["49866376", "66835630", "71664579", "11915", "12072107"]
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        #
        # Site configuration used for database resource access -
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testALoadAndUpdate(self):
        """Test case - load and reload/update data store."""
        try:
            #  -- Update/create cache ---
            exportPath = os.path.join(self.__cachePath, "PubChem")
            pcdcP = PubChemDataCacheProvider(self.__cfgOb, self.__cachePath)
            ok, failList = pcdcP.load(self.__cidList, exportPath=exportPath)
            self.assertTrue(ok)
            self.assertEqual(len(failList), 0)
            logger.info("Status %r failList %r", ok, failList)
            #
            idL = pcdcP.getRefIdCodes()
            logger.info("idL %r", idL)
            self.assertGreaterEqual(len(idL), len(self.__cidList))
            #
            ok, failList = pcdcP.updateMissing(self.__cidList, exportPath=exportPath)
            self.assertTrue(ok)
            self.assertEqual(len(failList), 0)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testBackupAndRestore(self):
        """Test case - load and dump operations."""
        try:
            #  -- Backup/Restore cache ---
            pcdcP = PubChemDataCacheProvider(self.__cfgOb, self.__cachePath)
            ok, failList = pcdcP.load(self.__cidList, exportPath=None)
            self.assertEqual(len(failList), 0)
            self.assertTrue(ok)
            ok = pcdcP.dump(fmt="json")
            self.assertTrue(ok)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testGetRelatedIdentifiers(self):
        """Test case - get PubChem xrefs."""
        try:
            #  --- Get related identifiers ---
            pcdcP = PubChemDataCacheProvider(self.__cfgOb, self.__cachePath)
            rD = pcdcP.getRelatedMapping(self.__cidList)
            logger.info("rD %r", rD)
            self.assertGreaterEqual(len(rD), len(self.__cidList))
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def pubChemDataCacheProviderSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PubChemDataCacheProviderTests("testALoadAndUpdate"))
    suiteSelect.addTest(PubChemDataCacheProviderTests("testBackupAndRestore"))
    suiteSelect.addTest(PubChemDataCacheProviderTests("testGetRelatedIdentifiers"))

    return suiteSelect


if __name__ == "__main__":
    mySuite = pubChemDataCacheProviderSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
