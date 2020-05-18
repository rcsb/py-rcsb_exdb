##
# File:    PubChemCacheProviderTests.py
# Author:  J. Westbrook
# Date:    10-Feb-2020
#
# Updates:
#
##
"""
Tests for reference data cache maintenance operations
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import platform
import resource
import time
import unittest

from rcsb.exdb.chemref.PubChemCacheProvider import PubChemCacheProvider

from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class PubChemCacheProviderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(PubChemCacheProviderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        self.__workPath = os.path.join(HERE, "test-output")
        self.__dataPath = os.path.join(HERE, "test-data")
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        self.__mU = MarshalUtil(workPath=self.__cachePath)
        #
        # Site configuration used for database resource access -
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
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

    def testPubChemCacheProvider(self):
        """ Test case - create and read cached reference chemical definitions.
        """
        try:
            #  -- Update/create cache ---
            rsaP = PubChemCacheProvider(
                self.__cfgOb,
                maxChunkSize=20,
                numProc=1,
                expireDays=0,
                cachePath=self.__cachePath,
                useCache=True,
                ccUrlTarget=self.__ccUrlTarget,
                birdUrlTarget=self.__birdUrlTarget,
                ccFileNamePrefix="cc-abbrev",
                exportPath=os.path.join(self.__cachePath, "PubChem"),
                rebuildChemIndices=True,
                fetchLimit=2,
            )
            ok = rsaP.testCache()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def pubChemCacheProviderSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PubChemCacheProviderTests("testPubChemCacheProvider"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = pubChemCacheProviderSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
