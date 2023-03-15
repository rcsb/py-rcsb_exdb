##
# File:    PubChemEtlWorkflowTests.py
# Author:  J. Westbrook
# Date:    29-Jul-2020
#
# Updates:
#  13-Mar-2023 aae Disable git stash testing
##
"""
Tests for PubChem ETL workflow methods
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

from rcsb.exdb.wf.PubChemEtlWorkflow import PubChemEtlWorkflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class PubChemEtlWorkflowTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(PubChemEtlWorkflowTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        self.__dataPath = os.path.join(HERE, "test-data")
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        #
        # Site configuration used for database resource access -
        # self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        self.__configName = "site_info_configuration"
        #
        # These are test source files for chemical component/BIRD indices
        self.__ccUrlTarget = os.path.join(self.__dataPath, "components-abbrev.cif")
        self.__birdUrlTarget = os.path.join(self.__dataPath, "prdcc-abbrev.cif")
        self.__ccFileNamePrefix = "cc-abbrev"
        #
        # This tests pushing files to the stash
        self.__testStashServer = True
        self.__testStashGit = False
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testAUpdateIndex(self):
        """Test case - build CCD/BIRD search indices and search for PubChem matches."""
        try:
            #  -- Update local chemical indices and  create PubChem mapping index ---

            pcewP = PubChemEtlWorkflow(configPath=self.__configPath, configName=self.__configName, cachePath=self.__cachePath)
            ok = pcewP.updateMatchedIndex(
                ccUrlTarget=self.__ccUrlTarget,
                birdUrlTarget=self.__birdUrlTarget,
                ccFileNamePrefix=self.__ccFileNamePrefix,
                numProcChemComp=4,
                rebuildChemIndices=True,
                useStash=self.__testStashServer,
                useGit=self.__testStashGit
            )
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testBDump(self):
        """Test case - dump current stored state"""
        try:
            #  --
            pcewP = PubChemEtlWorkflow(configPath=self.__configPath, configName=self.__configName, cachePath=self.__cachePath)
            ok = pcewP.dump(useStash=self.__testStashServer, useGit=self.__testStashGit)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testCRestore(self):
        """Test case - restore object store from the prior dump"""
        try:
            #  --
            pcewP = PubChemEtlWorkflow(configPath=self.__configPath, configName=self.__configName, cachePath=self.__cachePath)
            ok = pcewP.restore()
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testDUpdateData(self):
        """Test case - update corresponding data and generate corresponding identifiers."""
        try:
            #  --
            pcewP = PubChemEtlWorkflow(configPath=self.__configPath, configName=self.__configName, cachePath=self.__cachePath)
            ok = pcewP.updateMatchedData(useStash=self.__testStashServer, useGit=self.__testStashGit)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def pubChemEtlWorkflowSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PubChemEtlWorkflowTests("testAUpdateIndex"))
    suiteSelect.addTest(PubChemEtlWorkflowTests("testBDump"))
    suiteSelect.addTest(PubChemEtlWorkflowTests("testCRestore"))
    suiteSelect.addTest(PubChemEtlWorkflowTests("testDUpdateData"))

    return suiteSelect


if __name__ == "__main__":
    mySuite = pubChemEtlWorkflowSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
