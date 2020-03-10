##
# File:    UniProtEtlWorkerTests.py
# Author:  J. Westbrook
# Date:    9-Dec-2018
#
# Updates:
#
##
"""
Tests for loading repository holdings information.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import resource
import time
import tracemalloc
import unittest

from rcsb.exdb.seq.UniProtEtlWorker import UniProtEtlWorker
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class UniProtEtlWorkerTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(UniProtEtlWorkerTests, self).__init__(methodName)
        self.__verbose = True
        self.__traceMemory = False

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
        #
        if self.__traceMemory:
            tracemalloc.start()
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        if self.__traceMemory:
            rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            current, peak = tracemalloc.get_traced_memory()
            logger.info("Current memory usage is %.2f MB; Peak was %.2f MB Resident size %.2f MB", current / 10 ** 6, peak / 10 ** 6, rusageMax / 10 ** 6)
            tracemalloc.stop()
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    @unittest.skip("Disable test - deprecated")
    def testLoadUniProt(self):
        """ Test case - load UniProt reference data -
        """
        try:
            uw = UniProtEtlWorker(self.__cfgOb, self.__cachePath)
            ok = uw.load(self.__updateId, extResource="UniProt", loadType="full")
            #
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    @unittest.skip("Disable test - deprecated")
    def testValidateUniProt(self):
        """ Test case - validate UniProt reference data -
        """
        try:
            uw = UniProtEtlWorker(self.__cfgOb, self.__cachePath, doValidate=True)
            ok = uw.load(self.__updateId, extResource="UniProt", loadType="full")
            #
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def uniProtEtlWorkerSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(UniProtEtlWorkerTests("testLoadUniProt"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = uniProtEtlWorkerSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
