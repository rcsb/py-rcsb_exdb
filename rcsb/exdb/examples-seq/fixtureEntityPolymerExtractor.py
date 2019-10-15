##
# File:    EntityPolymerExtractorFixture.py
# Author:  J. Westbrook
# Date:    25-Mar-2019
#
# Updates:
#  21-Apr-2019 jdw Separate tests against the  mock-data repo in this module
#   4-Sep-201  jdw make this a fixture
#
##
"""
Fixture extractor to preserve entity polymer data.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.exdb.seq.EntityPolymerExtractor import EntityPolymerExtractor
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class EntityPolymerExtractorFixture(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(EntityPolymerExtractorFixture, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        #
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        #
        self.__cacheKwargs = {"fmt": "pickle"}
        self.__exdbCacheDirPath = os.path.join(self.__cachePath, self.__cfgOb.get("EXDB_CACHE_DIR", sectionName=configName))
        #
        self.__entryLimitTest = None
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testExtractEntityPolymers(self):
        """ Fixture - extract and save entity polymer info

        """
        try:
            epe = EntityPolymerExtractor(self.__cfgOb, exdbDirPath=self.__exdbCacheDirPath, useCache=False, cacheKwargs=self.__cacheKwargs, entryLimit=self.__entryLimitTest)
            eCount = epe.getEntryCount()
            self.assertGreaterEqual(eCount, 10)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def entityPolymerExtractSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(EntityPolymerExtractorFixture("testExtractEntityPolymers"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = entityPolymerExtractSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
