##
# File:    ReferenceSequenceCacheProviderTests.py
# Author:  J. Westbrook
# Date:    10-Feb-2020
#
# Updates:
#
##
"""
Tests for reference sequence cache maintenance operations
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

from rcsb.exdb.seq.ReferenceSequenceCacheProvider import ReferenceSequenceCacheProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ReferenceSequenceCacheProviderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(ReferenceSequenceCacheProviderTests, self).__init__(methodName)
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
        self.__fetchLimitTest = None
        self.__useCache = False
        #
        if self.__traceMemory:
            tracemalloc.start()
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        if self.__traceMemory:
            rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            current, peak = tracemalloc.get_traced_memory()
            logger.info("Current memory usage is %.4f MB; Peak was %.4f MB Resident size %.4f MB", current / 10 ** 6, peak / 10 ** 6, rusageMax / 10 ** 6)
            tracemalloc.stop()
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testCacheProvider(self):
        """ Test case - create and read cache reference sequences.
        """
        try:
            #  -- create cache ---
            rsaP = ReferenceSequenceCacheProvider(self.__cfgOb, useCache=self.__useCache, cachePath=self.__cachePath, fetchLimit=self.__fetchLimitTest,)
            ok = rsaP.testCache()
            self.assertTrue(ok)
            reLoad = False
            if reLoad:
                numRef = rsaP.getRefDataCount()
                self.assertGreaterEqual(numRef, 90)
                #
                # ---  Reload from cache ---
                rsaP = ReferenceSequenceCacheProvider(self.__cfgOb, useCache=True, cachePath=self.__cachePath)
                ok = rsaP.testCache()
                self.assertTrue(ok)
                numRef = rsaP.getRefDataCount()
                self.assertGreaterEqual(numRef, 90)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def referenceSequenceCacheProviderSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ReferenceSequenceCacheProviderTests("testCacheProvider"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = referenceSequenceCacheProviderSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
