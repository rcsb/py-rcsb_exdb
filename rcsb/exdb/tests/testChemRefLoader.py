##
# File:    ChemRefLoaderTests.py
# Author:  J. Westbrook
# Date:    18-Jun-2021
#
# Updates:
#
##
"""
Tests for loading chemical reference data and identifer mapping information.

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

from rcsb.exdb.chemref.ChemRefExtractor import ChemRefExtractor
from rcsb.exdb.chemref.ChemRefEtlWorker import ChemRefEtlWorker
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ChemRefLoaderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(ChemRefLoaderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        #
        # sample data set
        self.__updateId = "2021_10"
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testLoadIntegratedDrugBankData(self):
        """Test case - load integrated DrugBank chemical reference data -"""
        try:
            crw = ChemRefEtlWorker(self.__cfgOb, self.__cachePath)
            crExt = ChemRefExtractor(self.__cfgOb)

            idD = crExt.getChemCompAccessionMapping(referenceResourceName="DrugBank")
            logger.info("Mapping dictionary %r", len(idD))
            #
            ok = crw.load(self.__updateId, extResource="DrugBank", loadType="full")
            #
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testDrugBankDataMapping(self):
        """Test case - get DrugBank mapping -"""
        try:
            crExt = ChemRefExtractor(self.__cfgOb)
            idD = crExt.getChemCompAccessionMapping(referenceResourceName="DrugBank")
            logger.info("Mapping dictionary %r", len(idD))
            #
            mU = MarshalUtil()
            fp = os.path.join(HERE, "test-output", "drugbank-mapping.json")
            mU.doExport(fp, idD, fmt="json", indent=3)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def chemRefLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ChemRefLoaderTests("testLoadIntegratedDrugBankData"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = chemRefLoadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
