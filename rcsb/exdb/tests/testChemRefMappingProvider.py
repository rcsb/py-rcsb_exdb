##
# File:    ChemRefMappingProviderTests.py
# Author:  J. Westbrook
# Date:    18-Jun-2021
#
# Updates:
#
##
"""
Tests for

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

from rcsb.exdb.chemref.ChemRefMappingProvider import ChemRefMappingProvider

from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ChemRefMappingProviderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(ChemRefMappingProviderTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testChemRefMapping(self):
        """Test case - load and access mapping cache"""
        try:
            crmP = ChemRefMappingProvider(self.__cachePath, useCache=True)
            ok = crmP.testCache()
            self.assertTrue(ok)
            #
            ok = crmP.fetchChemRefMapping(self.__cfgOb, referenceResourceNameList=None)
            self.assertTrue(ok)
            crmP = ChemRefMappingProvider(self.__cachePath, useCache=True)
            ok = crmP.testCache(minCount=2)
            self.assertTrue(ok)
            # tD = {"CHEMBL": ("CHEMBL14249", "ATP"), "DRUGBANK": ("DB00171", "ATP")}
            tD = {"DRUGBANK": ("DB00171", "ATP")}
            for refName, refTup in tD.items():
                tL = crmP.getReferenceIds(refName, refTup[1])
                logger.info("tL %r", tL)
                self.assertTrue(refTup[0] in tL)
                tL = crmP.getLocalIds(refName, refTup[0])
                logger.info("tL %r", tL)
                self.assertTrue(refTup[1] in tL)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def chemRefMappingSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ChemRefMappingProviderTests("testChemRefMapping"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = chemRefMappingSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
