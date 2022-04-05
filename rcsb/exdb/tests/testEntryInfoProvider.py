##
# File:    EntryInfoProviderTests.py
# Author:  J. Westbrook
# Date:    22-Sep-2021
#
# Update:
#
#
##
"""
Tests for accessors entry-level annotations.

"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import unittest

from rcsb.exdb.entry.EntryInfoProvider import EntryInfoProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


class EntryInfoProviderTests(unittest.TestCase):
    doInternal = False

    def setUp(self):
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=self.__configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #

    def tearDown(self):
        pass

    def testGetEntryInfo(self):
        minCount = 12
        eiP = EntryInfoProvider(cachePath=self.__cachePath, useCache=False)
        ok = eiP.testCache(minCount=0)
        self.assertTrue(ok)
        ok = eiP.update(self.__cfgOb, fmt="json", indent=3)
        self.assertTrue(ok)
        riD = eiP.getEntryInfo("4en8")
        logger.info("riD (%d) %r", len(riD), riD)
        rL = eiP.getEntriesByPolymerEntityCount(count=2)
        self.assertGreaterEqual(len(rL), 5)
        ok = eiP.testCache(minCount=minCount)
        self.assertTrue(ok)
        eiP = EntryInfoProvider(cachePath=self.__cachePath, useCache=True)
        ok = eiP.testCache(minCount=minCount)
        self.assertTrue(ok)

    @unittest.skipUnless(doInternal, "Internal full test")
    def testEntryInfoRemote(self):
        minCount = 182000
        configName = "site_info_remote_configuration"
        cfgOb = ConfigUtil(configPath=self.__configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        eiP = EntryInfoProvider(cachePath=self.__cachePath, useCache=True)
        ok = eiP.testCache(minCount=minCount)
        ok = eiP.update(cfgOb, fmt="json", indent=3)
        self.assertTrue(ok)
        riD = eiP.getEntryInfo("1kip")
        logger.info("riD (%d)", len(riD))
        self.assertGreaterEqual(len(riD), 1)
        configName = "site_info_configuration"
        cfgOb = ConfigUtil(configPath=self.__configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        ok = eiP.backup(cfgOb, configName, useStash=False, useGit=True)
        self.assertTrue(ok)
        ok = eiP.restore(cfgOb, configName, useStash=False, useGit=True)
        self.assertTrue(ok)
        eiP = EntryInfoProvider(cachePath=self.__cachePath, useCache=True)
        ok = eiP.testCache(minCount=minCount)


def entryInfoSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(EntryInfoProviderTests("testGetEntryInfo"))
    return suiteSelect


if __name__ == "__main__":

    mySuite = entryInfoSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
