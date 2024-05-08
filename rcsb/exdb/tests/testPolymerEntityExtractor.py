##
# File:    PolymerEntityExtractorTests.py
# Author:  J. Westbrook
# Date:    5-Dec-2020
#
# Updates:
#
##
"""
Tests for extraction of polymer entity sequence details from the ExDB core collections.
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

from rcsb.exdb.seq.PolymerEntityExtractor import PolymerEntityExtractor
from rcsb.utils.config.ConfigUtil import ConfigUtil


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class PolymerEntityExtractorTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(PolymerEntityExtractorTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        self.__fastaPath = os.path.join(HERE, "test-output", "CACHE", "pdb-protein-entity.fa")
        self.__taxonPath = os.path.join(HERE, "test-output", "CACHE", "pdb-protein-entity-taxon.tdd")
        self.__detailsPath = os.path.join(HERE, "test-output", "CACHE", "pdb-protein-entity-details.json")
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testGetProteinEntityDetails(self):
        """Test case - get protein entity sequences and essential details"""
        try:
            pEx = PolymerEntityExtractor(self.__cfgOb)
            pD, _ = pEx.getProteinSequenceDetails()
            #
            self.assertGreaterEqual(len(pD), 70)
            logger.info("Polymer entity count %d", len(pD))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testExportProteinEntityFasta(self):
        """Test case - export protein entity sequence Fasta"""
        try:
            pEx = PolymerEntityExtractor(self.__cfgOb)
            ok = pEx.exportProteinEntityFasta(self.__fastaPath, self.__taxonPath, self.__detailsPath)
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def extractorSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PolymerEntityExtractorTests("testGetProteinEntityDetails"))
    suiteSelect.addTest(PolymerEntityExtractorTests("testExportProteinEntityFasta"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = extractorSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
