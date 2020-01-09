##
# File: ExDbWorkflow.py
# Date: 17-Dec-2019  jdw
#
#  Workflow wrapper  --  exchange database loading utilities --
#
#  Updates:
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from rcsb.db.helpers.DictMethodResourceProvider import DictMethodResourceProvider
from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.utils.TimeUtil import TimeUtil
from rcsb.exdb.chemref.ChemRefEtlWorker import ChemRefEtlWorker
from rcsb.exdb.seq.ReferenceSequenceAssignmentAdapter import ReferenceSequenceAssignmentAdapter
from rcsb.exdb.seq.ReferenceSequenceAssignmentProvider import ReferenceSequenceAssignmentProvider
from rcsb.exdb.seq.UniProtEtlWorker import UniProtEtlWorker
from rcsb.exdb.tree.TreeNodeListWorker import TreeNodeListWorker
from rcsb.exdb.utils.ObjectTransformer import ObjectTransformer
from rcsb.utils.config.ConfigUtil import ConfigUtil

logger = logging.getLogger(__name__)


class ExDbWorkflow(object):
    def __init__(self, **kwargs):
        #  Configuration Details
        configPath = kwargs.get("configPath", "exdb-config-example.yml")
        self.__configName = kwargs.get("configName", "site_info_configuration")
        mockTopPath = kwargs.get("mockTopPath", None)
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=self.__configName, mockTopPath=mockTopPath)
        #
        self.__cachePath = kwargs.get("cachePath", ".")
        self.__cachePath = os.path.abspath(self.__cachePath)
        self.__debugFlag = kwargs.get("debugFlag", False)
        if self.__debugFlag:
            logger.setLevel(logging.DEBUG)
        #
        #  Rebuild or check resource cache
        rebuildCache = kwargs.get("rebuildCache", False)
        self.__useCache = not rebuildCache
        self.__cacheStatus = self.buildResourceCache(rebuildCache=rebuildCache)
        logger.info("Cache status if %r", self.__cacheStatus)
        #

    def load(self, op, **kwargs):
        if not self.__cacheStatus:
            logger.error("Cache update failed")
            return False
        # argument processing
        if op not in ["etl_tree_node_lists", "etl_chemref", "etl_uniprot", "upd_ref_seq"]:
            return False
        try:
            # test mode and UniProt accession primary match minimum count for doReferenceSequenceUpdate()
            testMode = kwargs.get("testMode", False)
            minMatchPrimary = kwargs.get("minMatchPrimary", None)
            #
            readBackCheck = kwargs.get("readBackCheck", False)
            numProc = int(kwargs.get("numPro", 1))
            chunkSize = int(kwargs.get("chunkSize", 10))
            documentLimit = int(kwargs.get("documentLimit")) if "documentLimit" in kwargs else None
            loadType = kwargs.get("loadType", "full")  # or replace
            dbType = kwargs.get("dbType", "mongo")
            tU = TimeUtil()
            dataSetId = kwargs.get("dataSetId") if "dataSetId" in kwargs else tU.getCurrentWeekSignature()
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            return False
        #
        if dbType == "mongo":
            if op == "etl_tree_node_lists":
                rhw = TreeNodeListWorker(
                    self.__cfgOb,
                    self.__cachePath,
                    numProc=numProc,
                    chunkSize=chunkSize,
                    documentLimit=documentLimit,
                    verbose=self.__debugFlag,
                    readBackCheck=readBackCheck,
                    useCache=self.__useCache,
                )
                ok = rhw.load(dataSetId, loadType=loadType)
                okS = self.loadStatus(rhw.getLoadStatus(), readBackCheck=readBackCheck)

            elif op == "etl_chemref":
                crw = ChemRefEtlWorker(
                    self.__cfgOb,
                    self.__cachePath,
                    numProc=numProc,
                    chunkSize=chunkSize,
                    documentLimit=documentLimit,
                    verbose=self.__debugFlag,
                    readBackCheck=readBackCheck,
                    useCache=self.__useCache,
                )
                ok = crw.load(dataSetId, extResource="DrugBank", loadType=loadType)
                okS = self.loadStatus(crw.getLoadStatus(), readBackCheck=readBackCheck)

            elif op == "etl_uniprot":
                crw = UniProtEtlWorker(
                    self.__cfgOb,
                    self.__cachePath,
                    numProc=numProc,
                    chunkSize=chunkSize,
                    documentLimit=documentLimit,
                    verbose=self.__debugFlag,
                    readBackCheck=readBackCheck,
                    useCache=self.__useCache,
                )
                ok = crw.load(dataSetId, extResource="UniProt", loadType=loadType)
                okS = self.loadStatus(crw.getLoadStatus(), readBackCheck=readBackCheck)

            elif op == "upd_ref_seq":
                ok = self.doReferenceSequenceUpdate(fetchLimit=documentLimit, testMode=testMode, minMatchPrimary=minMatchPrimary)
                okS = ok
        #
        logger.info("Operation completed with status %r " % ok and okS)
        return ok and okS

    def loadStatus(self, statusList, readBackCheck=True):
        ret = False
        try:
            dl = DocumentLoader(self.__cfgOb, self.__cachePath, "MONGO_DB", numProc=1, chunkSize=2, documentLimit=None, verbose=False, readBackCheck=readBackCheck)
            #
            sectionName = "data_exchange_configuration"
            databaseName = self.__cfgOb.get("DATABASE_NAME", sectionName=sectionName)
            collectionName = self.__cfgOb.get("COLLECTION_UPDATE_STATUS", sectionName=sectionName)
            ret = dl.load(databaseName, collectionName, loadType="append", documentList=statusList, indexAttributeList=["update_id", "database_name", "object_name"], keyNames=None)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ret

    def buildResourceCache(self, rebuildCache=False):
        """Generate and cache resource dependencies.
        """
        ret = False
        try:
            rp = DictMethodResourceProvider(self.__cfgOb, configName=self.__configName, cachePath=self.__cachePath)
            ret = rp.cacheResources(useCache=not rebuildCache)
            logger.info("Cache status return is %r", ret)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ret

    def doReferenceSequenceUpdate(self, fetchLimit=None, testMode=False, minMatchPrimary=None):
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
                useCache=self.__useCache,
                cachePath=self.__cachePath,
                fetchLimit=fetchLimit,
                siftsAbbreviated="TEST",
            )
            ok = rsaP.testCache(minMatchPrimary=minMatchPrimary)
            if not ok:
                logger.error("Cache construction fails %s", ok)
                return False
            logger.info("Cached reference data count is %d", rsaP.getRefDataCount())
            #
            if testMode:
                return ok
            rsa = ReferenceSequenceAssignmentAdapter(refSeqAssignProvider=rsaP)
            obTr = ObjectTransformer(self.__cfgOb, objectAdapter=rsa)
            ok = obTr.doTransform(
                databaseName=databaseName, collectionName=collectionName, fetchLimit=fetchLimit, selectionQuery={"entity_poly.rcsb_entity_polymer_type": polymerType}
            )
            return ok
        except Exception as e:
            logger.exception("Failing with %s", str(e))
