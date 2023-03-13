##
# File:    PubChemIndexCacheProviderTests.py
# Author:  J. Westbrook
# Date:    16-Jul-2020
#
# Updates:
#  13-Mar-2023 aae Fix tests after removing obsolete entries from test data
##
"""
Tests for PubChem index cache maintenance operations
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

from rcsb.exdb.chemref.PubChemIndexCacheProvider import PubChemIndexCacheProvider

from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class PubChemIndexCacheProviderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(PubChemIndexCacheProviderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        self.__dataPath = os.path.join(HERE, "test-data")
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        #
        # Site configuration used for database resource access -
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        self.__configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=self.__configName, mockTopPath=self.__mockTopPath)
        #
        # These are test source files for chemical component/BIRD indices
        self.__ccUrlTarget = os.path.join(self.__dataPath, "components-abbrev.cif")
        self.__birdUrlTarget = os.path.join(self.__dataPath, "prdcc-abbrev.cif")
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testAPubChemIndexCacheProvider(self):
        """Test case - search, backup, restore and select PubChem correspondences for reference chemical definitions."""
        try:
            #  -- Update/create mapping index cache ---
            numObj = 25
            pcicP = PubChemIndexCacheProvider(self.__cfgOb, self.__cachePath)
            pcicP.updateMissing(
                expireDays=0,
                cachePath=self.__cachePath,
                ccUrlTarget=self.__ccUrlTarget,
                birdUrlTarget=self.__birdUrlTarget,
                ccFileNamePrefix="cc-abbrev",
                exportPath=os.path.join(self.__cachePath, "PubChem"),
                rebuildChemIndices=False,
                fetchLimit=None,
            )
            matchD = pcicP.getMatchData(expireDays=0)
            logger.info("matchD (%d)", len(matchD))
            self.assertGreaterEqual(len(matchD), numObj)
            ok = pcicP.testCache()
            self.assertTrue(ok)
            #
            ok = pcicP.dump()
            self.assertTrue(ok)
            #
            mapD, extraMapD = pcicP.getSelectedMatches(exportPath=os.path.join(self.__cachePath, "mapping"))
            self.assertGreaterEqual(len(mapD), 20)
            logger.info("mapD (%d) extraMapD (%d) %r", len(mapD), len(extraMapD), extraMapD)
            self.assertGreaterEqual(len(extraMapD), 2)
            cidList = pcicP.getMatches()
            logger.info("cidList (%d)", len(cidList))
            self.assertGreaterEqual(len(cidList), 49)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testBPubChemIndexCacheProvider(self):
        """Test case -  verify the PubChem index cache"""
        try:
            #  -- check cache
            pcicP = PubChemIndexCacheProvider(self.__cfgOb, self.__cachePath)
            ok = pcicP.testCache()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def pubChemIndexCacheProviderSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PubChemIndexCacheProviderTests("testAPubChemIndexCacheProvider"))
    suiteSelect.addTest(PubChemIndexCacheProviderTests("testBPubChemIndexCacheProviderCache"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = pubChemIndexCacheProviderSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
