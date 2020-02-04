##
# File: TreeNodeListWorker.py
# Date: 9-Apr-2019  jdw
#
# Loading worker for tree node list data.
#
# Updates:
#  9-Sep-2019 jdw add AtcProvider() and ChemrefExtractor() for ATC tree.
# JDW TODO TEST
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import glob
import logging
import os.path

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.exdb.chemref.ChemRefExtractor import ChemRefExtractor
from rcsb.exdb.seq.AnnotationExtractor import AnnotationExtractor
from rcsb.exdb.seq.TaxonomyExtractor import TaxonomyExtractor
from rcsb.utils.chemref.AtcProvider import AtcProvider
from rcsb.utils.ec.EnzymeDatabaseProvider import EnzymeDatabaseProvider
from rcsb.utils.go.GeneOntologyProvider import GeneOntologyProvider
from rcsb.utils.struct.CathClassificationProvider import CathClassificationProvider
from rcsb.utils.struct.ScopClassificationProvider import ScopClassificationProvider
from rcsb.utils.taxonomy.TaxonomyProvider import TaxonomyProvider

logger = logging.getLogger(__name__)


class TreeNodeListWorker(object):
    """ Prepare and load repository holdings and repository update data.
    """

    def __init__(self, cfgOb, cachePath, numProc=1, chunkSize=10, readBackCheck=False, documentLimit=None, verbose=False, useCache=False):
        self.__cfgOb = cfgOb
        self.__cachePath = os.path.abspath(cachePath)
        self.__readBackCheck = readBackCheck
        self.__numProc = numProc
        self.__chunkSize = chunkSize
        self.__documentLimit = documentLimit
        self.__resourceName = "MONGO_DB"
        self.__filterType = "assign-dates"
        self.__verbose = verbose
        self.__statusList = []
        self.__useCache = useCache

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

    def load(self, updateId, loadType="full"):
        """ Load tree node lists and status data -

        Relevant configuration options:

        tree_node_lists_configuration:
            DATABASE_NAME: tree_node_lists
            DATABASE_VERSION_STRING: v5
            COLLECTION_VERSION_STRING: 1.0.0
            COLLECTION_TAXONOMY: tree_taxonomy_node_list
            COLLECTION_ENZYME: tree_ec_node_list
            COLLECTION_SCOP: tree_scop_node_list
            COLLECTION_CATH: tree_cath_node_list

        """
        try:
            useCache = self.__useCache
            #
            if not useCache:
                cDL = ["domains_struct", "NCBI", "ec", "go", "atc"]
                for cD in cDL:
                    try:
                        cfp = os.path.join(self.__cachePath, cD)
                        os.makedirs(cfp, 0o755)
                    except Exception:
                        pass
                    #
                    try:
                        cfp = os.path.join(self.__cachePath, cD)
                        fpL = glob.glob(os.path.join(cfp, "*"))
                        if fpL:
                            for fp in fpL:
                                os.remove(fp)
                    except Exception:
                        pass

            #
            logger.info("Starting with cache path %r (useCache=%r)", self.__cachePath, useCache)
            #
            sectionName = "tree_node_lists_configuration"
            self.__statusList = []
            desp = DataExchangeStatus()
            statusStartTimestamp = desp.setStartTime()
            dl = DocumentLoader(
                self.__cfgOb,
                self.__cachePath,
                self.__resourceName,
                numProc=self.__numProc,
                chunkSize=self.__chunkSize,
                documentLimit=self.__documentLimit,
                verbose=self.__verbose,
                readBackCheck=self.__readBackCheck,
            )
            #
            databaseName = self.__cfgOb.get("DATABASE_NAME", sectionName=sectionName)
            # collectionVersion = self.__cfgOb.get("COLLECTION_VERSION_STRING", sectionName=sectionName)
            # addValues = {"_schema_version": collectionVersion}
            addValues = None
            # ---
            goP = GeneOntologyProvider(goDirPath=os.path.join(self.__cachePath, "go"), useCache=useCache)
            ok = goP.testCache()
            anEx = AnnotationExtractor(self.__cfgOb)
            goIdL = anEx.getUniqueIdentifiers("GO")
            logger.info("Unique GO assignments %d", len(goIdL))
            nL = goP.exportTreeNodeList(goIdL)
            logger.info("GO tree node list length %d", len(nL))
            collectionName = self.__cfgOb.get("COLLECTION_GO", sectionName=sectionName)
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=nL, indexAttributeList=["update_id"], keyNames=None, addValues=addValues, schemaLevel=None)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)
            # ----
            ccu = CathClassificationProvider(cathDirPath=os.path.join(self.__cachePath, "domains_struct"), useCache=useCache)
            nL = ccu.getTreeNodeList()
            logger.info("Starting load SCOP node tree length %d", len(nL))
            collectionName = self.__cfgOb.get("COLLECTION_CATH", sectionName=sectionName)
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=nL, indexAttributeList=["update_id"], keyNames=None, addValues=addValues, schemaLevel=None)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)

            scu = ScopClassificationProvider(scopDirPath=os.path.join(self.__cachePath, "domains_struct"), useCache=useCache)
            nL = scu.getTreeNodeList()
            logger.info("Starting load SCOP node tree length %d", len(nL))
            collectionName = self.__cfgOb.get("COLLECTION_SCOP", sectionName=sectionName)
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=nL, indexAttributeList=["update_id"], keyNames=None, addValues=addValues, schemaLevel=None)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)

            edbu = EnzymeDatabaseProvider(enzymeDirPath=os.path.join(self.__cachePath, "ec"), useCache=useCache)
            nL = edbu.getTreeNodeList()
            logger.info("Starting load of EC node tree length %d", len(nL))
            collectionName = self.__cfgOb.get("COLLECTION_ENZYME", sectionName=sectionName)
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=nL, indexAttributeList=["update_id"], keyNames=None, addValues=addValues, schemaLevel=None)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)
            # ----
            # Get the taxon coverage in the current data set -
            epe = TaxonomyExtractor(self.__cfgOb)
            tL = epe.getUniqueTaxons()
            logger.info("Taxon coverage length %d", len(tL))
            #
            tU = TaxonomyProvider(taxDirPath=os.path.join(self.__cachePath, "NCBI"), useCache=useCache)
            fD = {1}
            for taxId in tL:
                fD.update({k: True for k in tU.getLineage(taxId)})
            logger.info("Taxon filter dictionary length %d", len(fD))
            # logger.info("fD %r" % sorted(fD))
            #
            nL = tU.exportNodeList(filterD=fD)
            self.__checkTaxonNodeList(nL)
            logger.info("Starting load of taxonomy node tree length %d", len(nL))
            collectionName = self.__cfgOb.get("COLLECTION_TAXONOMY", sectionName=sectionName)
            logger.debug("Taxonomy nodes (%d) %r", len(nL), nL[:5])
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=nL, indexAttributeList=["update_id"], keyNames=None, addValues=addValues, schemaLevel=None)
            logger.info("Tree loading operations completed.")
            #
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)
            #
            # ---
            crEx = ChemRefExtractor(self.__cfgOb)
            atcFilterD = crEx.getChemCompAccessionMapping("ATC")
            logger.info("Length of ATC filter %d", len(atcFilterD))
            atcCacheDirPath = os.path.join(self.__cachePath, self.__cfgOb.get("ATC_CACHE_DIR", sectionName=self.__cfgOb.getDefaultSectionName()))
            atcP = AtcProvider(dirPath=atcCacheDirPath, useCache=useCache)
            nL = atcP.getTreeNodeList(filterD=atcFilterD)
            collectionName = self.__cfgOb.get("COLLECTION_ATC", sectionName=sectionName)
            logger.debug("ATC node list length %d %r", len(nL), nL[:5])
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=nL, indexAttributeList=["update_id"], keyNames=None, addValues=addValues, schemaLevel=None)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)
            #
            # ---
            logger.info("Completed tree node list loading operations.\n")
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
