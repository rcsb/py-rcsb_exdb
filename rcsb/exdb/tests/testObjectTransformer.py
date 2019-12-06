##
# File:    ObjectTransformerTests.py
# Author:  J. Westbrook
# Date:    25-Apr-2019
#
# Updates:
#
##
"""
Tests for extractor and updater or selected values from collections (limited tests from mock-data repos)
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time
import unittest

from rcsb.exdb.utils.ObjectTransformer import ObjectTransformer
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ObjectTransformerTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(ObjectTransformerTests, self).__init__(methodName)
        self.__verbose = True

    def setUp(self):
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        #
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        self.__fetchLimit = 5
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.info("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testTranformEntityProteinContent(self):
        """ Test case - transform selected entity protein documents
        """
        try:
            databaseName = "pdbx_core"
            collectionName = "pdbx_core_polymer_entity"
            obTr = ObjectTransformer(self.__cfgOb)
            ok = obTr.doTransform(
                databaseName=databaseName, collectionName=collectionName, fetchLimit=self.__fetchLimit, selectionQuery={"entity_poly.rcsb_entity_polymer_type": "Protein"}
            )
            self.assertTrue(ok)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def objectTransformerSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ObjectTransformerTests("testTransformEntityProteinContent"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = objectTransformerSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
