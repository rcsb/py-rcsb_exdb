##
# File:    EntityInstanceExtractorTests.py
# Author:  J. Westbrook
# Date:    25-Mar-2019
#
# Updates:
# 21-Apr-2019 jdw Tests for full cache construction and processiong
#
##
"""
Tests for extractor of selected values from entity polymer collections (full cache)

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest


from rcsb.exdb.seq.EntityPolymerExtractor import EntityPolymerExtractor
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.taxonomy.TaxonomyUtils import TaxonomyUtils


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class EntityPolymerExtractorFullTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(EntityPolymerExtractorFullTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        #
        # Caution: this is very site specific setting
        #
        configName = "site_info_remote"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        if configName != "site_info_configuration":
            self.__cfgOb.replaceSectionName("site_info_configuration", configName)
        #
        #
        self.__workPath = os.path.join(HERE, "test-cache-preserve")
        #
        self.__fullCacheKwargs = {"fmt": "pickle"}
        self.__fullEntitySaveCachePath = os.path.join(self.__workPath, "entity-polymer-data-cache.pic")
        #
        self.__mU = MarshalUtil()
        self.__entryLimitFull = 50
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    @unittest.skip("rebuild cache")
    def testRebuildCache(self):
        """ Test case - extract entity polymer info - rebuild full cache of extracted entity polymer data -

        """
        try:
            epe = EntityPolymerExtractor(
                self.__cfgOb, saveCachePath=self.__fullEntitySaveCachePath, useCache=False, saveCacheKwargs=self.__fullCacheKwargs, entryLimit=self.__entryLimitFull
            )
            eCount = epe.getEntryCount()
            if self.__entryLimitFull is not None:
                self.assertGreaterEqual(eCount, self.__entryLimitFull)
            else:
                self.assertGreaterEqual(eCount, 10)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testAccessEntityPolymerFeatures(self):
        """ Test case - access cached entity polymer info from full cache

        """
        try:
            epe = EntityPolymerExtractor(self.__cfgOb, saveCachePath=self.__fullEntitySaveCachePath, useCache=True, saveCacheKwargs=self.__fullCacheKwargs)
            eCount = epe.getEntryCount()
            logger.info("Entry count %d", eCount)
            self.assertGreaterEqual(eCount, self.__entryLimitFull)
            #
            unpL = epe.getRefSeqAccessions("UNP")
            logger.info("Ref seq count %d", len(unpL))
            self.assertGreaterEqual(len(unpL), 1)
            #
            testOp = False
            if testOp:
                for entryId in ["1CP9"]:
                    for entityId in ["1", "2"]:
                        uL = epe.getEntityRefSeqAccessions("UNP", entryId, entityId)
                        logger.debug("UNP for %s %s %r", entryId, entityId, uL)
                #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testAccessEntityPolymerReadCache(self):
        """ Test case - access cached entity polymer info from full cache

        """
        try:
            epe = EntityPolymerExtractor(self.__cfgOb, saveCachePath=self.__fullEntitySaveCachePath, useCache=True, saveCacheKwargs=self.__fullCacheKwargs)
            logger.info("Cache entry count %d", epe.getEntryCount())
            cD = epe.countRefSeqAccessions("UNP")
            self.assertGreaterEqual(len(cD), 2)
            #
            logger.info("UNP reference sequences per entity %r", dict(sorted(cD.items())))
            logger.info("Reference sequences per entity %r", dict(sorted(epe.countRefSeqAccessionAny().items())))
            logger.info("Reference sequences per ref db %r", dict(sorted(epe.countRefSeqAccessionDbType().items())))
            #
            ok = epe.checkRefSeqAlignRange("UNP")
            self.assertTrue(ok)
            unpL = epe.getRefSeqAccessions("UNP")
            logger.info("Unique UNP reference sequences %d", len(unpL))
            self.assertTrue(ok)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testTaxonomyEntityPolymerReadCache(self):
        """ Test case - evaluate taxonomy - from full cache

        """
        try:
            taxIdList = [562, 9606, 3701]
            for taxId in taxIdList:
                tU = TaxonomyUtils(taxDirPath=self.__workPath)
                tL = tU.getLineage(taxId)
                logger.info("Taxonomy lineage for %d %r", taxId, tL)
                #
                #
                epe = EntityPolymerExtractor(self.__cfgOb, saveCachePath=self.__fullEntitySaveCachePath, useCache=True, saveCacheKwargs=self.__fullCacheKwargs)
                logger.info("Cache entry count %d", epe.getEntryCount())
                logger.info("Reference sequences per ref db %r", dict(sorted(epe.countRefSeqAccessionDbType().items())))
                rD = epe.countRefSeqAccessionByTaxon(dbNameList=["UNP"])
                logger.info("Unique taxons %d", len(list(rD.keys())))
                #
                numT = 0
                for tId, aL in rD.items():
                    tL = tU.getLineage(tId)
                    if taxId in tL:
                        tc = len(set(aL))
                        logger.info("Matched %5d %s (%r)", tc, tU.getScientificName(tId), tId)
                        numT += tc
                logger.info("Total matched accessions %d ", numT)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def entityPolymerExtractFullSuite():
    suiteSelect = unittest.TestSuite()
    # suiteSelect.addTest(EntityPolymerExtractorFullTests("testRebuildCache"))
    suiteSelect.addTest(EntityPolymerExtractorFullTests("testAccessEntityPolymerFeatures"))
    suiteSelect.addTest(EntityPolymerExtractorFullTests("testAccessEntityPolymerReadCache"))
    suiteSelect.addTest(EntityPolymerExtractorFullTests("testTaxonomyEntityPolymerReadCache"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = entityPolymerExtractFullSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
