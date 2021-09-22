##
# File:    EntryInfoEtlWorkflowTests.py
# Author:  J. Westbrook
# Date:    22-Sep-2021
#
# Updates:
#
##
"""
Tests for entry information extraction ETL workflow methods
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

from rcsb.exdb.wf.EntryInfoEtlWorkflow import EntryInfoEtlWorkflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class EntryInfoEtlWorkflowTests(unittest.TestCase):
    def setUp(self):
        #
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        self.__configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        self.__configName = "site_info_configuration"
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testUpdateEntryInfo(self):
        """Test case - update extracted entry-level annotations."""
        try:
            eiP = EntryInfoEtlWorkflow(configPath=self.__configPath, configName=self.__configName, cachePath=self.__cachePath)
            ok = eiP.update(backup=False)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def entryInfoEtlWorkflowSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(EntryInfoEtlWorkflowTests("testUpdateEntryInfo"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = entryInfoEtlWorkflowSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
