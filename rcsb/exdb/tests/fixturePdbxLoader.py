##
# File:    PdbxLoaderFixture.py
# Author:  J. Westbrook
# Date:    4-Sep-2019
# Version: 0.001
#
# Updates:
#
##
"""
Fixture for loading the chemical reference and pdbx_core collections in a loca mongo instance.

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

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.mongo.PdbxLoader import PdbxLoader
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class PdbxLoaderFixture(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(PdbxLoaderFixture, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        self.__isMac = platform.system() == "Darwin"
        self.__excludeType = None if self.__isMac else "optional"
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
        #
        self.__resourceName = "MONGO_DB"
        self.__failedFilePath = os.path.join(HERE, "test-output", "failed-list.txt")
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__readBackCheck = True
        self.__numProc = 2
        self.__chunkSize = 10
        self.__fileLimit = None
        self.__documentStyle = "rowwise_by_name_with_cardinality"
        self.__ldList = [
            {"databaseName": "bird_chem_comp_core", "collectionNameList": None, "loadType": "full", "mergeContentTypes": None, "validationLevel": "min"},
            {"databaseName": "pdbx_core", "collectionNameList": None, "loadType": "full", "mergeContentTypes": ["vrpt"], "validationLevel": "min"},
        ]
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        unitS = "MB" if platform.system() == "Darwin" else "GB"
        rusageMax = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.info("Maximum resident memory size %.4f %s", rusageMax / 10 ** 6, unitS)
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testPdbxLoader(self):
        #
        for ld in self.__ldList:
            self.__pdbxLoaderWrapper(**ld)

    def __pdbxLoaderWrapper(self, **kwargs):
        """Wrapper for the PDBx loader module"""
        try:
            logger.info("Loading %s", kwargs["databaseName"])
            mw = PdbxLoader(
                self.__cfgOb,
                cachePath=self.__cachePath,
                resourceName=self.__resourceName,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                fileLimit=None,
                verbose=self.__verbose,
                readBackCheck=self.__readBackCheck,
                maxStepLength=2000,
                useSchemaCache=True,
                rebuildSchemaFlag=False,
            )
            ok = mw.load(
                kwargs["databaseName"],
                collectionLoadList=kwargs["collectionNameList"],
                loadType=kwargs["loadType"],
                inputPathList=None,
                styleType=self.__documentStyle,
                dataSelectors=["PUBLIC_RELEASE"],
                failedFilePath=self.__failedFilePath,
                saveInputFileListPath=None,
                pruneDocumentSize=None,
                logSize=False,
                validationLevel=kwargs["validationLevel"],
                mergeContentTypes=kwargs["mergeContentTypes"],
                useNameFlag=False,
                providerTypeExclude=self.__excludeType,
                restoreUseGit=True,
                restoreUseStash=False,
            )
            self.assertTrue(ok)
            ok = self.__loadStatus(mw.getLoadStatus())
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def __loadStatus(self, statusList):
        sectionName = "data_exchange_configuration"
        dl = DocumentLoader(
            self.__cfgOb,
            self.__cachePath,
            resourceName=self.__resourceName,
            numProc=self.__numProc,
            chunkSize=self.__chunkSize,
            documentLimit=None,
            verbose=self.__verbose,
            readBackCheck=self.__readBackCheck,
        )
        #
        databaseName = self.__cfgOb.get("DATABASE_NAME", sectionName=sectionName)
        collectionName = self.__cfgOb.get("COLLECTION_UPDATE_STATUS", sectionName=sectionName)
        ok = dl.load(databaseName, collectionName, loadType="append", documentList=statusList, indexAttributeList=["update_id", "database_name", "object_name"], keyNames=None)
        return ok


def mongoLoadPdbxSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PdbxLoaderFixture("testPdbxLoader"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = mongoLoadPdbxSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
