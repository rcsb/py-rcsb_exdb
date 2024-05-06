##
# File:    ObjectExtractorTests.py
# Author:  J. Westbrook
# Date:    25-Apr-2019
#
# Updates:
#
##
"""
Tests for extractor selected values from collections (limited tests from mock-data repos)
"""

__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import platform
import resource
import pprint
import time
import unittest
from collections import defaultdict

from rcsb.db.mongo.Connection import Connection
from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.TimeUtil import TimeUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ObjectExtractorTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(ObjectExtractorTests, self).__init__(methodName)
        self.__verbose = False

    def setUp(self):
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(TOPDIR, "rcsb", "mock-data", "config", "dbload-setup-example.yml")
        #
        configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=self.__mockTopPath)
        #
        self.__resourceName = "MONGO_DB"
        self.__workPath = os.path.join(TOPDIR, "CACHE", "exdb")
        #
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

    def testCreateMultipleConnections(self):
        """Test case -  multiple connection creation"""
        try:
            for _ in range(5):
                with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                    self.assertNotEqual(client, None)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testExtractDrugbankMapping(self):
        """Test case - extract Drugbank mapping"""
        try:
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName="bird_chem_comp_core",
                collectionName="bird_chem_comp_core",
                cacheFilePath=os.path.join(self.__workPath, "drugbank-mapping-cache.json"),
                useCache=False,
                cacheKwargs=self.__testEntryCacheKwargs,
                keyAttribute="chem_comp",
                uniqueAttributes=["rcsb_id"],
                selectionQuery={"rcsb_chem_comp_container_identifiers.drugbank_id": {"$exists": True}},
                selectionList=["rcsb_id", "rcsb_chem_comp_container_identifiers", "rcsb_chem_comp_related"],
            )
            eCount = obEx.getCount()
            logger.info("Component count ifs %d", eCount)
            self.assertGreaterEqual(eCount, 3)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testExtractEntriesBefore(self):
        """Test case - extract entries subject to date restriction"""
        try:
            tU = TimeUtil()
            tS = tU.getTimestamp(useUtc=True, before={"days": 365 * 5})
            tD = tU.getDateTimeObj(tS)
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName="pdbx_core",
                collectionName="pdbx_core_entry",
                useCache=False,
                keyAttribute="entry",
                uniqueAttributes=["rcsb_id"],
                selectionQuery={"rcsb_accession_info.initial_release_date": {"$gt": tD}},
                selectionList=["rcsb_id", "rcsb_accession_info"],
            )
            eD = obEx.getObjects()
            eCount = obEx.getCount()
            logger.info("Entry count is %d", eCount)
            logger.info("Entries are %r", list(eD.keys()))
            self.assertGreaterEqual(eCount, 5)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testExtractEntries(self):
        """Test case - extract entries"""
        try:
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName="pdbx_core",
                collectionName="pdbx_core_entry",
                cacheFilePath=os.path.join(self.__workPath, "entry-data-test-cache.json"),
                useCache=False,
                keyAttribute="entry",
                uniqueAttributes=["rcsb_id"],
                cacheKwargs=self.__testEntryCacheKwargs,
                objectLimit=self.__objectLimitTest,
            )
            eCount = obEx.getCount()
            logger.info("Entry count is %d", eCount)
            self.assertGreaterEqual(eCount, self.__objectLimitTest)

            objD = obEx.getObjects()
            for _, obj in objD.items():
                # obEx.genPathList(obj["software"], path=["software"])
                obEx.genPathList(obj, path=None)

            #
            pL = obEx.getPathList(filterList=True)
            obEx.setPathList(pL)
            if self.__verbose:
                for ky, obj in objD.items():
                    obEx.genValueList(obj, path=None)
                    tD = obEx.getValues()
                    logger.debug("Index object %r %s", ky, pprint.pformat(tD, indent=3, width=120))

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testExtractEntities(self):
        """Test case - extract entities"""
        try:
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName="pdbx_core",
                collectionName="pdbx_core_polymer_entity",
                cacheFilePath=os.path.join(self.__workPath, "entity-data-test-cache.json"),
                useCache=False,
                keyAttribute="entity",
                uniqueAttributes=["rcsb_id"],
                cacheKwargs=self.__testEntryCacheKwargs,
                objectLimit=self.__objectLimitTest,
            )
            eCount = obEx.getCount()
            logger.info("Entity count is %d", eCount)
            self.assertGreaterEqual(eCount, self.__objectLimitTest)

            objD = obEx.getObjects()
            for _, obj in objD.items():
                obEx.genPathList(obj, path=None)
            #
            pL = obEx.getPathList(filterList=False)
            logger.debug("Path list (unfiltered) %r", pL)
            #
            pL = obEx.getPathList()
            logger.debug("Path list %r", pL)
            obEx.setPathList(pL)
            if self.__verbose:
                for ky, obj in objD.items():
                    obEx.genValueList(obj, path=None)
                    tD = obEx.getValues()
                    logger.info("Index object %r %s", ky, pprint.pformat(tD, indent=3, width=120))

        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testExtractSelectedEntityContent(self):
        """Test case - extract selected entity content

        "reference_sequence_identifiers": [
                    {
                        "database_name": "UniProt",
                        "database_accession": "Q5SHN1",
                        "provenance_source": "SIFTS"
                    },
                    {
                        "database_name": "UniProt",
                        "database_accession": "Q5SHN1",
                        "provenance_source": "PDB"
                    }
                    ]
        """
        try:
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName="pdbx_core",
                collectionName="pdbx_core_polymer_entity",
                cacheFilePath=os.path.join(self.__workPath, "entity-selected-content-test-cache.json"),
                useCache=False,
                keyAttribute="entity",
                uniqueAttributes=["rcsb_id"],
                cacheKwargs=self.__testEntryCacheKwargs,
                # objectLimit=self.__objectLimitTest,
                objectLimit=None,
                selectionQuery={"entity_poly.rcsb_entity_polymer_type": "Protein"},
                selectionList=["rcsb_id", "rcsb_polymer_entity_container_identifiers.reference_sequence_identifiers"],
            )
            eCount = obEx.getCount()
            logger.info("Entity count is %d", eCount)
            #
            #
            if self.__objectLimitTest is not None:
                self.assertGreaterEqual(eCount, self.__objectLimitTest)
                objD = obEx.getObjects()
                for _, obj in objD.items():
                    obEx.genPathList(obj, path=None)
                #
                pL = obEx.getPathList(filterList=False)
                logger.debug("Path list (unfiltered) %r", pL)
                #
                pL = obEx.getPathList()
                logger.debug("Path list %r", pL)
                obEx.setPathList(pL)
                if self.__verbose:
                    for ky, obj in objD.items():
                        obEx.genValueList(obj, path=None)
                        tD = obEx.getValues()
                        logger.info("Index object %r %s", ky, pprint.pformat(tD, indent=3, width=120))

            objD = obEx.getObjects()
            # logger.info("objD.keys() %r", list(objD.keys()))
            totCount = 0
            difCount = 0
            pdbUnpIdD = defaultdict(int)
            siftsUnpIdD = defaultdict(int)
            pdbDifUnpIdD = defaultdict(int)
            for entityKey, eD in objD.items():
                try:
                    siftsS = set()
                    pdbS = set()
                    for tD in eD["rcsb_polymer_entity_container_identifiers"]["reference_sequence_identifiers"]:
                        if tD["database_name"] == "UniProt":
                            if tD["provenance_source"] == "SIFTS":
                                siftsS.add(tD["database_accession"])
                                siftsUnpIdD[tD["database_accession"]] += 1
                            elif tD["provenance_source"] == "PDB":
                                pdbS.add(tD["database_accession"])
                                pdbUnpIdD[tD["database_accession"]] += 1
                        else:
                            logger.debug("No UniProt for %r", eD["rcsb_polymer_entity_container_identifiers"])
                    logger.debug("PDB assigned sequence length %d", len(pdbS))
                    logger.debug("SIFTS assigned sequence length %d", len(siftsS))

                    if pdbS and siftsS:
                        totCount += 1
                        if pdbS != siftsS:
                            difCount += 1
                            for idV in pdbS:
                                pdbDifUnpIdD[idV] += 1

                except Exception as e:
                    logger.warning("No identifiers for %s with %s", entityKey, str(e))
            logger.info("Total %d differences %d", totCount, difCount)
            logger.info("Unique UniProt ids  PDB %d  SIFTS %d", len(pdbUnpIdD), len(siftsUnpIdD))
            logger.info("Unique UniProt differences %d ", len(pdbDifUnpIdD))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()

    def testExtractEntityTaxonomyContent(self):
        """Test case - extract unique entity source and host taxonomies"""
        try:
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName="pdbx_core",
                collectionName="pdbx_core_polymer_entity",
                cacheFilePath=os.path.join(self.__workPath, "entity-taxonomy-test-cache.json"),
                useCache=False,
                keyAttribute="entity",
                uniqueAttributes=["rcsb_id"],
                cacheKwargs=self.__testEntryCacheKwargs,
                # objectLimit=self.__objectLimitTest,
                objectLimit=None,
                selectionQuery=None,
                selectionList=["rcsb_id", "rcsb_entity_source_organism.ncbi_taxonomy_id", "rcsb_entity_host_organism.ncbi_taxonomy_id"],
            )
            eCount = obEx.getCount()
            logger.info("Polymer entity count is %d", eCount)
            taxIdS = set()
            objD = obEx.getObjects()
            for _, eD in objD.items():
                try:
                    for tD in eD["rcsb_entity_source_organism"]:
                        taxIdS.add(tD["ncbi_taxonomy_id"])
                except Exception:
                    pass
                try:
                    for tD in eD["rcsb_entity_host_organism"]:
                        taxIdS.add(tD["ncbi_taxonomy_id"])
                except Exception:
                    pass

            logger.info("Unique taxons %d", len(taxIdS))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def objectExtractorSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ObjectExtractorTests("testExtractEntries"))
    suiteSelect.addTest(ObjectExtractorTests("testExtractEntities"))
    suiteSelect.addTest(ObjectExtractorTests("testExtractSelectedEntityContent"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = objectExtractorSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
