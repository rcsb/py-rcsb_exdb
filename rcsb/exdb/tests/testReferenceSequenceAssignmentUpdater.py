##
# File:    UpdateReferenceSequencesTests.py
# Author:  J. Westbrook
# Date:    12-Oct-2019
#
# Updates:
#
##
"""
Tests for reference sequence assignment update operations
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from rcsb.exdb.seq.ReferenceSequenceAssignmentUpdater import ReferenceSequenceAssignmentUpdater
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ReferenceSequenceAssignmentUpdaterTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(ReferenceSequenceAssignmentUpdaterTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        self.__resourceName = "MONGO_DB"
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__testEntityCacheKwargs = {"fmt": "json", "indent": 3}
        self.__fetchLimitTest = None
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testUpdateAssignments(self):
        """ Test case - get reference sequences and update candidates
        """
        try:
            rsau = ReferenceSequenceAssignmentUpdater(self.__cfgOb, useCache=False, cachePath=self.__cachePath, fetchLimit=self.__fetchLimitTest, siftsAbbreviated="TEST")
            updateLimit = None
            updateId = "2019_01"
            lenUpd, numUpd = rsau.doUpdate(updateId, updateLimit=updateLimit)
            logger.info("Update length %d numUpd %d", lenUpd, numUpd)
            # self.assertEqual(numUpd, lenUpd)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def referenceUpdaterSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ReferenceSequenceAssignmentUpdaterTests("testUpdateAssignments"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = referenceUpdaterSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
