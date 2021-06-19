##
# File:    UniProtExtractorTests.py
# Author:  J. Westbrook
# Date:    5-Dec-2020
#
# Updates:
#
##
"""
Tests for extraction of UniProt reference sequence details from the ExDB UniProt collection.
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

from rcsb.exdb.seq.UniProtExtractor import UniProtExtractor
from rcsb.utils.config.ConfigUtil import ConfigUtil


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class UniProtExtractorTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(UniProtExtractorTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testGetUniProtDetails(self):
        """Test case - get UniProt reference sequences and essential details"""
        try:
            uEx = UniProtExtractor(self.__cfgOb)
            unpD = uEx.getReferenceSequenceDetails()
            logger.info("UniProt count %d", len(unpD))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def extractorSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(UniProtExtractorTests("testGetGoIds"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = extractorSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
