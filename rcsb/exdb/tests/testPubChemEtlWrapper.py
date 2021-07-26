##
# File:    PubChemEtlWrapperTests.py
# Author:  J. Westbrook
# Date:    20-Jul-2020
#
# Updates:
#
##
"""
Tests for PubChem ETL wrapper methods
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

from rcsb.exdb.chemref.PubChemEtlWrapper import PubChemEtlWrapper

from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class PubChemEtlWrapperTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(PubChemEtlWrapperTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        self.__workPath = os.path.join(HERE, "test-output")
        self.__dataPath = os.path.join(HERE, "test-data")
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        self.__dirPath = os.path.join(self.__cachePath, "PubChem")
        self.__mU = MarshalUtil(workPath=self.__cachePath)
        #
        # Site configuration used for database resource access -
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        # These are test source files for chemical component/BIRD indices
        self.__ccUrlTarget = os.path.join(self.__dataPath, "components-abbrev.cif")
        self.__birdUrlTarget = os.path.join(self.__dataPath, "prdcc-abbrev.cif")
        self.__numComponents = 30
        self.__numSelectMatches = 23
        self.__numAltMatches = 2
        self.__numTotalMatches = 50
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testAFromBootstrap(self):
        """Test case - build CCD/BIRD search indices and search for PubChem matches."""
        try:
            #  -- Update local chemical indices and  create PubChem mapping index ---

            pcewP = PubChemEtlWrapper(self.__cfgOb, self.__cachePath)
            ok = pcewP.updateIndex(
                ccUrlTarget=self.__ccUrlTarget,
                birdUrlTarget=self.__birdUrlTarget,
                ccFileNamePrefix="cc-abbrev",
                exportPath=self.__dirPath,
                rebuildChemIndices=True,
                numProc=4,
            )
            self.assertTrue(ok)
            #
            mL = pcewP.getMatches()
            self.assertGreaterEqual(len(mL), self.__numTotalMatches)
            selectMatchD, altMatchD = pcewP.getSelectedMatches()
            #
            logger.info("matchD (%d)", len(selectMatchD))
            self.assertGreaterEqual(len(selectMatchD), self.__numSelectMatches)
            self.assertGreaterEqual(len(altMatchD), self.__numAltMatches)
            #
            ok = pcewP.dump(contentType="index")
            self.assertTrue(ok)
            ok = pcewP.toStash(contentType="index")
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testBFromRestore(self):
        """Test case - operations from a restored starting point"""
        try:
            #  --
            pcewP = PubChemEtlWrapper(self.__cfgOb, self.__cachePath)
            ok = pcewP.fromStash(contentType="index")
            self.assertTrue(ok)
            #
            numObjects = pcewP.reloadDump(contentType="index")
            logger.info("Restored %d correspondence records", numObjects)
            self.assertGreaterEqual(numObjects, self.__numComponents)
            mapD, extraMapD = pcewP.getSelectedMatches(exportPath=os.path.join(self.__cachePath, "mapping"))
            self.assertGreaterEqual(len(mapD), self.__numSelectMatches)
            logger.info("mapD (%d) extraMapD (%d) %r", len(mapD), len(extraMapD), extraMapD)
            self.assertGreaterEqual(len(extraMapD), self.__numAltMatches)
            cidList = pcewP.getMatches()
            logger.info("cidList (%d)", len(cidList))
            self.assertGreaterEqual(len(cidList), self.__numTotalMatches - 2)
            ok = pcewP.updateMatchedData()
            self.assertTrue(ok)
            ok = pcewP.dump(contentType="data")
            self.assertTrue(ok)
            ok = pcewP.toStash(contentType="data")
            self.assertTrue(ok)
            ok = pcewP.updateIdentifiers()
            self.assertTrue(ok)
            ok = pcewP.dump(contentType="identifiers")
            self.assertTrue(ok)
            ok = pcewP.toStash(contentType="identifiers")
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def pubChemEtlWrapperSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PubChemEtlWrapperTests("testAFromBootstrap"))
    suiteSelect.addTest(PubChemEtlWrapperTests("testBFromRestore"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = pubChemEtlWrapperSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
