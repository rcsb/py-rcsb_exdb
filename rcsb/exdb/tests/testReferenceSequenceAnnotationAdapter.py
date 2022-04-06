##
# File:    ReferenceSequenceAnnotationAdapterTests.py
# Author:  J. Westbrook
# Date:    14-Feb-2020
#
# Updates:
#
##
"""
Tests of reference seequence annotation adapter.
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

from rcsb.exdb.seq.ReferenceSequenceAnnotationAdapter import ReferenceSequenceAnnotationAdapter
from rcsb.exdb.seq.ReferenceSequenceAnnotationProvider import ReferenceSequenceAnnotationProvider
from rcsb.exdb.utils.ObjectTransformer import ObjectTransformer
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ReferenceSequenceAnnotationAdapterTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(ReferenceSequenceAnnotationAdapterTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__useCache = True
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        self.__resourceName = "MONGO_DB"
        self.__fetchLimit = None
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testAnnotationAdapter(self):
        """Test case - create and read cache reference sequences assignments and related data."""
        try:
            databaseName = "pdbx_core"
            collectionName = "pdbx_core_polymer_entity"
            polymerType = "Protein"
            #  -- create cache ---
            rsaP = ReferenceSequenceAnnotationProvider(
                self.__cfgOb, databaseName, collectionName, polymerType, fetchLimit=self.__fetchLimit, siftsAbbreviated="TEST", cachePath=self.__cachePath, useCache=True
            )
            ok = rsaP.testCache()
            self.assertTrue(ok)
            numRef1 = rsaP.getRefDataCount()
            #
            # ---  Reload from cache ---
            rsaP = ReferenceSequenceAnnotationProvider(self.__cfgOb, databaseName, collectionName, polymerType, cachePath=self.__cachePath, useCache=True)
            ok = rsaP.testCache()
            self.assertTrue(ok)
            numRef2 = rsaP.getRefDataCount()
            self.assertEqual(numRef1, numRef2)
            #
            rsa = ReferenceSequenceAnnotationAdapter(rsaP)
            obTr = ObjectTransformer(self.__cfgOb, objectAdapter=rsa)
            ok = obTr.doTransform(
                databaseName=databaseName, collectionName=collectionName, fetchLimit=self.__fetchLimit, selectionQuery={"entity_poly.rcsb_entity_polymer_type": polymerType}
            )
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def referenceSequenceAnnotationAdapterSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ReferenceSequenceAnnotationAdapterTests("testAnnotationAdapter"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = referenceSequenceAnnotationAdapterSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
