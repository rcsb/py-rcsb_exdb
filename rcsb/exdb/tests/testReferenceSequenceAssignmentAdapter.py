##
# File:    ReferenceSequenceAssignmentAdapterTests.py
# Author:  J. Westbrook
# Date:    25-Apr-2019
#
# Updates:
#  2GKI_1,3ZXR_1,2I6F_1,1NQP_1,1NQP_2,1BL4_1,1F9E_1,1SFI_2,1EBO_1,1R6Z_1,1MH3_1,1MH4_1,1MOW_3,1BWM_1 --2I6F_1
##
"""
Tests of reference seequence assignment adapter.
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

from rcsb.exdb.seq.ReferenceSequenceAssignmentAdapter import ReferenceSequenceAssignmentAdapter
from rcsb.exdb.seq.ReferenceSequenceAssignmentProvider import ReferenceSequenceAssignmentProvider
from rcsb.exdb.utils.ObjectTransformer import ObjectTransformer
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ReferenceSequenceAssignmentAdapterTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(ReferenceSequenceAssignmentAdapterTests, self).__init__(methodName)
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
        # self.__testEntityCacheKwargs = {"fmt": "json", "indent": 3}
        self.__testEntityCacheKwargs = {"fmt": "pickle"}
        self.__fetchLimit = None
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testAssignmentAdapter(self):
        """Test case - create and read cache reference sequences assignments and related data."""
        try:
            databaseName = "pdbx_core"
            collectionName = "pdbx_core_polymer_entity"
            polymerType = "Protein"
            referenceDatabaseName = "UniProt"
            provSource = "PDB"
            #
            #  -- create cache ---
            rsaP = ReferenceSequenceAssignmentProvider(
                self.__cfgOb,
                databaseName=databaseName,
                collectionName=collectionName,
                polymerType=polymerType,
                referenceDatabaseName=referenceDatabaseName,
                provSource=provSource,
                useCache=True,
                cachePath=self.__cachePath,
                cacheKwargs=self.__testEntityCacheKwargs,
                fetchLimit=self.__fetchLimit,
                siftsAbbreviated="TEST",
            )
            ok = rsaP.testCache()
            self.assertTrue(ok)
            numRef1 = rsaP.getRefDataCount()
            #
            # ---  Reload from cache ---
            rsaP = ReferenceSequenceAssignmentProvider(
                self.__cfgOb, referenceDatabaseName=referenceDatabaseName, useCache=True, cachePath=self.__cachePath, cacheKwargs=self.__testEntityCacheKwargs
            )
            ok = rsaP.testCache()
            self.assertTrue(ok)
            numRef2 = rsaP.getRefDataCount()
            self.assertEqual(numRef1, numRef2)
            #
            rsa = ReferenceSequenceAssignmentAdapter(refSeqAssignProvider=rsaP)
            obTr = ObjectTransformer(self.__cfgOb, objectAdapter=rsa)
            ok = obTr.doTransform(
                databaseName=databaseName, collectionName=collectionName, fetchLimit=self.__fetchLimit, selectionQuery={"entity_poly.rcsb_entity_polymer_type": polymerType}
            )
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def referenceSequenceAssignmentAdapterSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ReferenceSequenceAssignmentAdapterTests("testAssignmentAdapter"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = referenceSequenceAssignmentAdapterSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
