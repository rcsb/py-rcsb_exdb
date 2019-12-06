##
# File:    CitationAdapterTests.py
# Author:  J. Westbrook
# Date:    22-Nov-2019
#
# Updates:
#
##
"""
Tests of reference seequence assignment adapter.
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from rcsb.exdb.citation.CitationAdapter import CitationAdapter
from rcsb.exdb.utils.ObjectTransformer import ObjectTransformer
from rcsb.utils.citation.CitationReferenceProvider import CitationReferenceProvider
from rcsb.utils.citation.JournalTitleAbbreviationProvider import JournalTitleAbbreviationProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class CitationAdapterTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(CitationAdapterTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        self.__resourceName = "MONGO_DB"
        self.__cachePath = os.path.join(TOPDIR, "CACHE", "cit_ref")
        self.__testEntityCacheKwargs = {"fmt": "json", "indent": 3}
        self.__fetchLimit = None
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testCitationAdapter(self):
        """ Test case - create and read cache reference sequences assignments and related data.
        """
        try:
            databaseName = "pdbx_core"
            collectionName = "pdbx_core_entry"
            useCache = False
            #
            crP = CitationReferenceProvider(cachePath=self.__cachePath, useCache=useCache)
            ok = crP.testCache()
            self.assertTrue(ok)
            jtaP = JournalTitleAbbreviationProvider(cachePath=self.__cachePath, useCache=useCache)
            ok = jtaP.testCache()
            self.assertTrue(ok)
            #
            ca = CitationAdapter(crP, jtaP)
            obTr = ObjectTransformer(self.__cfgOb, objectAdapter=ca)
            ok = obTr.doTransform(databaseName=databaseName, collectionName=collectionName, fetchLimit=self.__fetchLimit)
            self.assertTrue(ok)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def citationAdapterSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(CitationAdapterTests("testCitationAdapter"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = citationAdapterSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
