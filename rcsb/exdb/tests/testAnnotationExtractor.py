##
# File:    AnnotationExtractorTests.py
# Author:  J. Westbrook
# Date:    26-Jan-2020
#
# Updates:
#
##
"""
Tests for extraction of annotation identifiers from the polymer entity collection.
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

from rcsb.exdb.seq.AnnotationExtractor import AnnotationExtractor
from rcsb.utils.config.ConfigUtil import ConfigUtil


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class AnnotationExtractorTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(AnnotationExtractorTests, self).__init__(methodName)
        self.__verbose = True
        self.__traceMemory = True

    def setUp(self):
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
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
        logger.info("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testGetGoIds(self):
        """ Test case - get reference sequences and update candidates
        """
        try:
            urs = AnnotationExtractor(self.__cfgOb)
            goIdL = urs.getUniqueIdentifiers("GO")
            logger.debug("goIdL %r", goIdL)
            logger.info("Unique GO ID count %d", len(goIdL))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def extractorSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(AnnotationExtractorTests("testGetGoIds"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = extractorSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
