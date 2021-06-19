##
# File:    ObjectUpdaterTests.py
# Author:  J. Westbrook
# Date:    25-Apr-2019
#
# Updates:
#
##
"""
Tests for extractor and updater or selected values from collections (limited tests from mock-data repos)
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

from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor
from rcsb.exdb.utils.ObjectUpdater import ObjectUpdater
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ObjectUpdaterTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(ObjectUpdaterTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        #
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        self.__workPath = os.path.join(TOPDIR, "CACHE", "exdb")
        self.__testEntryCacheKwargs = {"fmt": "json", "indent": 3}
        self.__objectLimitTest = 5
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testUpdateSelectedEntityContent(self):
        """Test case - update of selected entity reference sequence content"""
        try:
            databaseName = "pdbx_core"
            collectionName = "pdbx_core_polymer_entity"
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName=databaseName,
                collectionName=collectionName,
                cacheFilePath=os.path.join(self.__workPath, "entity-selected-content-test-cache.json"),
                useCache=False,
                keyAttribute="entity",
                uniqueAttributes=["rcsb_id"],
                cacheKwargs=self.__testEntryCacheKwargs,
                objectLimit=self.__objectLimitTest,
                # objectLimit=None,
                selectionQuery={"entity_poly.rcsb_entity_polymer_type": "Protein"},
                selectionList=["rcsb_id", "rcsb_polymer_entity_container_identifiers.reference_sequence_identifiers"],
            )
            eCount = obEx.getCount()
            logger.info("Entity count is %d", eCount)
            objD = obEx.getObjects()
            updateDL = []
            for entityKey, eD in objD.items():
                try:
                    selectD = {"rcsb_id": entityKey}
                    tL = (
                        eD["rcsb_polymer_entity_container_identifiers"]["reference_sequence_identifiers"]
                        if "reference_sequence_identifiers" in eD["rcsb_polymer_entity_container_identifiers"]
                        else []
                    )
                    tL.append({"database_accession": "1111111", "database_name": "PDB", "provenance_source": "RCSB"})
                    #
                    updateD = {"rcsb_polymer_entity_container_identifiers.reference_sequence_identifiers": tL}
                    updateDL.append({"selectD": selectD, "updateD": updateD})
                except Exception as e:
                    logger.exception("Failing with %s", str(e))
            for ii, uD in enumerate(updateDL):
                logger.debug(" >>>> (%d) selectD %r updateD %r", ii, uD["selectD"], uD["updateD"])
            #
            obUpd = ObjectUpdater(self.__cfgOb)
            numUpd = obUpd.update(databaseName, collectionName, updateDL)
            self.assertGreaterEqual(numUpd, len(updateDL))
            logger.info("Update count is %d", numUpd)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def objectUpdaterSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ObjectUpdaterTests("testUpdateSelectedEntityContent"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = objectUpdaterSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
