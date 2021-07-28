##
# File: ExDbWorkflow.py
# Date: 17-Dec-2019  jdw
#
#  Workflow wrapper  --  exchange database loading utilities --
#
#  Updates:
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.utils.TimeUtil import TimeUtil
from rcsb.exdb.chemref.ChemRefEtlWorker import ChemRefEtlWorker
from rcsb.exdb.seq.ReferenceSequenceAnnotationAdapter import ReferenceSequenceAnnotationAdapter
from rcsb.exdb.seq.ReferenceSequenceAnnotationProvider import ReferenceSequenceAnnotationProvider
from rcsb.exdb.seq.UniProtCoreEtlWorker import UniProtCoreEtlWorker
from rcsb.exdb.tree.TreeNodeListWorker import TreeNodeListWorker
from rcsb.exdb.utils.ObjectTransformer import ObjectTransformer
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.dictionary.DictMethodResourceProvider import DictMethodResourceProvider

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
        restoreUseGit = kwargs.get("restoreUseGit", True)
        restoreUseStash = kwargs.get("restoreUseStash", True)
        providerTypeExclude = kwargs.get("providerTypeExclude", None)
        #
        self.__cacheStatus = self.buildResourceCache(
            rebuildCache=rebuildCache,
            providerTypeExclude=providerTypeExclude,
            restoreUseGit=restoreUseGit,
            restoreUseStash=restoreUseStash,
        )
        logger.debug("Cache status if %r", self.__cacheStatus)
        #

    def load(self, op, **kwargs):
        logger.info("Starting operation %r\n", op)
        if not self.__cacheStatus:
            logger.error("Resource cache test or rebuild has failed - exiting")
            return False
        # argument processing
        if op not in ["etl_tree_node_lists", "etl_chemref", "etl_uniprot_core", "upd_ref_seq", "refresh_pubchem"]:
            logger.error("Unsupported operation %r - exiting", op)
            return False
        try:
            # test mode and UniProt accession primary match minimum count for doReferenceSequenceUpdate()
            testMode = kwargs.get("testMode", False)
            minMatchPrimaryPercent = kwargs.get("minMatchPrimaryPercent", None)
            minMissing = kwargs.get("minMissing", 0)
            #
            readBackCheck = kwargs.get("readBackCheck", False)
            numProc = int(kwargs.get("numProc", 1))
            chunkSize = int(kwargs.get("chunkSize", 10))
            refChunkSize = int(kwargs.get("refChunkSize", 100))
            documentLimit = int(kwargs.get("documentLimit")) if "documentLimit" in kwargs else None
            loadType = kwargs.get("loadType", "full")  # or replace
            dbType = kwargs.get("dbType", "mongo")
            tU = TimeUtil()
            dataSetId = kwargs.get("dataSetId") if "dataSetId" in kwargs else tU.getCurrentWeekSignature()
            #  Rebuild or reuse reference sequence cache
            rebuildSequenceCache = kwargs.get("rebuildSequenceCache", False)
            useSequenceCache = not rebuildSequenceCache
            #
        except Exception as e:
            logger.exception("Argument or configuration processing failing with %s", str(e))
            return False
        #
        okS = ok = False
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

            elif op == "etl_uniprot_core":
                crw = UniProtCoreEtlWorker(
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
                ok = self.doReferenceSequenceUpdate(
                    fetchLimit=documentLimit,
                    useSequenceCache=useSequenceCache,
                    testMode=testMode,
                    minMatchPrimaryPercent=minMatchPrimaryPercent,
                    minMissing=minMissing,
                    refChunkSize=refChunkSize,
                )
                okS = ok

        #
        logger.info("Completed operation %r with status %r\n", op, ok and okS)
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

    def buildResourceCache(self, rebuildCache=False, providerTypeExclude=None, restoreUseGit=True, restoreUseStash=True):
        """Generate and cache resource dependencies."""
        ret = False
        try:
            rp = DictMethodResourceProvider(
                self.__cfgOb,
                configName=self.__configName,
                cachePath=self.__cachePath,
                providerTypeExclude=providerTypeExclude,
                restoreUseGit=restoreUseGit,
                restoreUseStash=restoreUseStash,
            )
            ret = rp.cacheResources(useCache=not rebuildCache, doBackup=False)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ret

    def doReferenceSequenceUpdate(self, fetchLimit=None, useSequenceCache=False, testMode=False, minMatchPrimaryPercent=None, minMissing=0, refChunkSize=50, **kwargs):
        try:
            _ = kwargs
            databaseName = "pdbx_core"
            collectionName = "pdbx_core_polymer_entity"
            polymerType = "Protein"
            _ = testMode
            # -------
            rsaP = ReferenceSequenceAnnotationProvider(self.__cfgOb, useCache=useSequenceCache, cachePath=self.__cachePath, maxChunkSize=refChunkSize)
            ok = rsaP.testCache(minMatchPrimaryPercent=minMatchPrimaryPercent, minMissing=minMissing)
            if ok:
                rsa = ReferenceSequenceAnnotationAdapter(rsaP)
                obTr = ObjectTransformer(self.__cfgOb, objectAdapter=rsa)
                ok = obTr.doTransform(
                    databaseName=databaseName, collectionName=collectionName, fetchLimit=fetchLimit, selectionQuery={"entity_poly.rcsb_entity_polymer_type": polymerType}
                )
            else:
                logger.error("Reference sequence data cache build failing")
            return ok
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False
