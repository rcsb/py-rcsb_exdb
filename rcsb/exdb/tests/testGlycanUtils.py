##
# File:    GlycanUtilsTests.py
# Author:  J. Westbrook
# Date:    25-May-2021
#
# Update:
##
"""
Tests for creating glycan accession mapping details.
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import unittest

from rcsb.exdb.branch.GlycanUtils import GlycanUtils
from rcsb.utils.config.ConfigUtil import ConfigUtil

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(HERE))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


class GlycanUtilsTests(unittest.TestCase):
    def setUp(self):
        self.__dirPath = os.path.join(HERE, "test-output", "CACHE", "glycan")
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #

    def tearDown(self):
        pass

    def testGlycanMapping(self):
        gU = GlycanUtils(self.__cfgOb, self.__dirPath)
        beD = gU.getBranchedEntityDetails()
        self.assertGreaterEqual(len(beD), 1)
        logger.info("branched entity descriptor details (%d)", len(beD))
        eaD = gU.updateEntityAccessionMap()
        logger.info("updated entity accession map length (%d)", len(eaD))
        self.assertGreaterEqual(len(eaD), 12)


def glycanMappingSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(GlycanUtilsTests("testGlycanMapping"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = glycanMappingSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
