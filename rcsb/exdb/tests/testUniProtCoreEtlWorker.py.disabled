##
# File:    UniProtCoreEtlWorkerTests.py
# Author:  J. Westbrook
# Date:    9-Dec-2018
#
# Updates:
#
##
"""
Tests for loading UniProt core collection

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

from rcsb.exdb.seq.UniProtCoreEtlWorker import UniProtCoreEtlWorker
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class UniProtCoreEtlWorkerTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(UniProtCoreEtlWorkerTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        #
        # sample data set
        self.__updateId = "2018_23"
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    @unittest.skip("Disable test - deprecated")
    def testLoadUniProtCore(self):
        """Test case - load UniProt core collection reference data -"""
        try:
            uw = UniProtCoreEtlWorker(self.__cfgOb, self.__cachePath)
            ok = uw.load(self.__updateId, extResource="UniProt", loadType="full")
            #
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skip("Disable test - deprecated")
    def testValidateUniProtCore(self):
        """Test case - validate UniProt core collection reference data -"""
        try:
            uw = UniProtCoreEtlWorker(self.__cfgOb, self.__cachePath, doValidate=True)
            ok = uw.load(self.__updateId, extResource="UniProt", loadType="full")
            #
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def uniProtCoreEtlWorkerSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(UniProtCoreEtlWorkerTests("testLoadUniProtCore"))
    suiteSelect.addTest(UniProtCoreEtlWorkerTests("testValidateUniProtCore"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = uniProtCoreEtlWorkerSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
