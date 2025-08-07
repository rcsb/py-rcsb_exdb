##
# File: TreeNodeListWorker.py
# Date: 9-Apr-2019  jdw
#
# Loading worker for tree node list data.
#
# Updates:
#  9-Sep-2019 jdw add AtcProvider() and ChemrefExtractor() for ATC tree.
# 12-Apr-2023 dwp add CARD ontology tree
#  8-Aug-2023 dwp Load full (unfiltered) taxonomy tree node list, and stop loading GO tree (will be loaded in DW instead)
# 27-Aug-2024 dwp Update CARD ontology tree loading
# 23-Jan-2025 dwp Change indexed field from 'update_id' to 'id'
#  7-Aug-2025 dwp Change target DB and collection names to "dw" and "tree_*" (via configuration file);
#                 Make use of configuration file for loading tree node lists and setting indexed fields
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os.path

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.exdb.chemref.ChemRefExtractor import ChemRefExtractor
from rcsb.utils.chemref.AtcProvider import AtcProvider
from rcsb.utils.ec.EnzymeDatabaseProvider import EnzymeDatabaseProvider
from rcsb.utils.targets.CARDTargetOntologyProvider import CARDTargetOntologyProvider
from rcsb.utils.struct.CathClassificationProvider import CathClassificationProvider
from rcsb.utils.struct.EcodClassificationProvider import EcodClassificationProvider
from rcsb.utils.struct.ScopClassificationProvider import ScopClassificationProvider
from rcsb.utils.struct.Scop2ClassificationProvider import Scop2ClassificationProvider
from rcsb.utils.taxonomy.TaxonomyProvider import TaxonomyProvider
from rcsb.exdb.seq.TaxonomyExtractor import TaxonomyExtractor

logger = logging.getLogger(__name__)


class TreeNodeListWorker(object):
    """Prepare and load repository holdings and repository update data."""

    def __init__(self, cfgOb, cachePath, numProc=1, chunkSize=10, maxStepLength=4000, readBackCheck=False, documentLimit=None, verbose=False, useCache=False, useFilteredLists=False):
        self.__cfgOb = cfgOb
        self.__cachePath = os.path.abspath(cachePath)
        self.__readBackCheck = readBackCheck
        self.__numProc = numProc
        self.__chunkSize = chunkSize
        self.__maxStepLength = maxStepLength
        self.__documentLimit = documentLimit
        self.__resourceName = "MONGO_DB"
        self.__filterType = "assign-dates"
        self.__verbose = verbose
        self.__statusList = []
        self.__useCache = useCache
        self.__useFilteredLists = useFilteredLists

    def __updateStatus(self, updateId, databaseName, collectionName, status, startTimestamp):
        try:
            sFlag = "Y" if status else "N"
            desp = DataExchangeStatus()
            desp.setStartTime(tS=startTimestamp)
            desp.setObject(databaseName, collectionName)
            desp.setStatus(updateId=updateId, successFlag=sFlag)
            desp.setEndTime()
            self.__statusList.append(desp.getStatus())
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def load(self, updateId, loadType="full", doLoad=True):
        """Load tree node lists and status data -

        Relevant configuration options:

        tree_node_lists_configuration:
            DATABASE_NAME: dw
            COLLECTION_VERSION_STRING: 2.1.0
            COLLECTION_NAME_LIST:
                - tree_taxonomy
                - tree_ec
                - tree_scop
                - tree_scop2
                - tree_cath
                - tree_atc
                - tree_card
                - tree_ecod
            COLLECTION_INDICES:
                - INDEX_NAME: primary
                ATTRIBUTE_NAMES:
                    - id
                - INDEX_NAME: index_2
                ATTRIBUTE_NAMES:
                    - parents
        """
        try:
            useCache = self.__useCache
            #
            logger.info("Starting with cache path %r (useCache=%r)", self.__cachePath, useCache)
            #
            self.__statusList = []
            desp = DataExchangeStatus()
            statusStartTimestamp = desp.setStartTime()
            dl = DocumentLoader(
                self.__cfgOb,
                self.__cachePath,
                self.__resourceName,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                maxStepLength=self.__maxStepLength,
                documentLimit=self.__documentLimit,
                verbose=self.__verbose,
                readBackCheck=self.__readBackCheck,
            )
            #
            sectionName = "tree_node_lists_configuration"
            databaseNameMongo = self.__cfgOb.get("DATABASE_NAME", sectionName=sectionName)
            collectionNameList = self.__cfgOb.get("COLLECTION_NAME_LIST", sectionName=sectionName)
            collectionIndexList = self.__cfgOb.get("COLLECTION_INDICES", sectionName=sectionName)
            # databaseNameMongo = 'dw'
            # collectionNameList = ['tree_taxonomy', 'tree_ec', 'tree_scop', 'tree_scop2', 'tree_cath', 'tree_atc', 'tree_card', 'tree_ecod', 'tree_go']
            # collectionIndexList = [{'INDEX_NAME': 'primary', 'ATTRIBUTE_NAMES': ['id']}, {'INDEX_NAME': 'index_2', 'ATTRIBUTE_NAMES': ['parents']}]

            # collectionVersion = self.__cfgOb.get("COLLECTION_VERSION_STRING", sectionName=sectionName)
            # addValues = {"_schema_version": collectionVersion}
            addValues = None

            ok = True
            for collectionName in collectionNameList:
                nL = self.__getTreeDocList(collectionName, useCache)
                if nL and doLoad:
                    ok = dl.load(
                        databaseNameMongo,
                        collectionName,
                        loadType=loadType,
                        documentList=nL,
                        keyNames=None,
                        addValues=addValues,
                        schemaLevel=None,
                        indexDL=collectionIndexList
                    ) and ok
                    self.__updateStatus(updateId, databaseNameMongo, collectionName, ok, statusStartTimestamp)
                logger.info(
                    "Completed load of tree node list for database %r, collection %r, len(nL) %r (status %r)",
                    databaseNameMongo, collectionName, len(nL), ok
                )
            # ---
            logger.info("Completed tree node list loading operations with loadType %r (status %r)", loadType, ok)
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def __checkTaxonNodeList(self, nL):
        eCount = 0
        tD = {dD["id"]: True for dD in nL}
        for dD in nL:
            if "parents" in dD:
                pId = dD["parents"][0]
                if pId not in tD:
                    logger.info("Missing parent for taxon %d", pId)
                    eCount += 1
            else:
                logger.info("No parents for node %r", dD["id"])

    def getLoadStatus(self):
        return self.__statusList

    def __getTreeDocList(self, collectionName, useCache):
        nL = []
        if collectionName.lower() == "tree_cath":
            ccu = CathClassificationProvider(cachePath=self.__cachePath, useCache=useCache)
            nL = ccu.getTreeNodeList()
        elif collectionName.lower() == "tree_scop2":
            scu2 = Scop2ClassificationProvider(cachePath=self.__cachePath, useCache=useCache)
            nL = scu2.getTreeNodeList()
        elif collectionName.lower() == "tree_scop":
            scu = ScopClassificationProvider(cachePath=self.__cachePath, useCache=useCache)
            nL = scu.getTreeNodeList()
        elif collectionName.lower() == "tree_ecod":
            ecu = EcodClassificationProvider(cachePath=self.__cachePath, useCache=useCache)
            nL = ecu.getTreeNodeList()
        elif collectionName.lower() == "tree_ec":
            edbu = EnzymeDatabaseProvider(cachePath=self.__cachePath, useCache=useCache)
            nL = edbu.getTreeNodeList()
        elif collectionName.lower() == "tree_card":
            okCou = True
            cou = CARDTargetOntologyProvider(cachePath=self.__cachePath, useCache=useCache)
            if not cou.testCache():
                ok = cou.buildOntologyData()
                cou.reload()
                if not (ok and cou.testCache()):
                    logger.error("Skipping load of CARD Target Ontology tree data because it is missing.")
                    okCou = False
            if okCou:
                nL = cou.getTreeNodeList()
        elif collectionName.lower() == "tree_taxonomy":
            tU = TaxonomyProvider(cachePath=self.__cachePath, useCache=useCache)
            if self.__useFilteredLists:
                # Get the taxon coverage in the current data set -
                epe = TaxonomyExtractor(self.__cfgOb)
                tL = epe.getUniqueTaxons()
                logger.info("Taxon coverage length %d", len(tL))
                #
                fD = {1}
                for taxId in tL:
                    fD.update({k: True for k in tU.getLineage(taxId)})
                logger.info("Taxon filter dictionary length %d", len(fD))
                logger.debug("fD %r", sorted(fD))
                #
                nL = tU.exportNodeList(filterD=fD)
            else:
                # Get the full taxon node list without filtering
                nL = tU.exportNodeList()
            self.__checkTaxonNodeList(nL)
        elif collectionName.lower() == "tree_atc":
            crEx = ChemRefExtractor(self.__cfgOb)
            atcFilterD = crEx.getChemCompAccessionMapping("ATC")
            logger.info("Length of ATC filter %d", len(atcFilterD))
            atcP = AtcProvider(cachePath=self.__cachePath, useCache=useCache)
            nL = atcP.getTreeNodeList(filterD=atcFilterD)
        else:
            logger.error("Unsupported tree node collection %r", collectionName)
        #
        logger.info("Gathered tree nodes for loading collection %s (length %d)", collectionName, len(nL))
        return nL
