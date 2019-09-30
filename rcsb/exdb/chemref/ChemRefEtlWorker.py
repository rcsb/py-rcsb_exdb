##
# File: ChemRefEtlWorker.py
# Date: 2-Jul-2018  jdw
#
# ETL utilities for processing chemical reference data and related data integration.
#
# Updates:
#  9-Dec-2018  jdw add validation methods
#  3-Sep-2019  jdw move to rcsb.exdb.chemref
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.db.utils.SchemaProvider import SchemaProvider
from rcsb.exdb.chemref.ChemRefExtractor import ChemRefExtractor
from rcsb.utils.chemref.DrugBankProvider import DrugBankProvider

logger = logging.getLogger(__name__)


class ChemRefEtlWorker(object):
    """ Prepare and load chemical reference data collections.
    """

    def __init__(self, cfgOb, cachePath, useCache=True, numProc=2, chunkSize=10, readBackCheck=False, documentLimit=None, verbose=False):
        self.__cfgOb = cfgOb
        self.__cachePath = cachePath
        self.__useCache = useCache
        self.__readBackCheck = readBackCheck
        self.__numProc = numProc
        self.__chunkSize = chunkSize
        self.__documentLimit = documentLimit
        #
        self.__resourceName = "MONGO_DB"
        self.__verbose = verbose
        self.__statusList = []
        self.__schP = SchemaProvider(self.__cfgOb, self.__cachePath, useCache=self.__useCache)
        #

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

    def load(self, updateId, extResource, loadType="full"):
        """ Load chemical reference integrated data for the input external resource-

        """
        try:
            self.__statusList = []
            desp = DataExchangeStatus()
            statusStartTimestamp = desp.setStartTime()
            #
            if extResource == "DrugBank":
                databaseName = "drugbank_core"
                configName = self.__cfgOb.getDefaultSectionName()
                user = self.__cfgOb.get("_DRUGBANK_AUTH_USERNAME", sectionName=configName)
                pw = self.__cfgOb.get("_DRUGBANK_AUTH_PASSWORD", sectionName=configName)
                dirPath = os.path.join(self.__cachePath, self.__cfgOb.get("DRUGBANK_CACHE_DIR", self.__cfgOb.getDefaultSectionName()))
                dbP = DrugBankProvider(dirPath=dirPath, useCache=self.__useCache, username=user, password=pw)
                #
                crExt = ChemRefExtractor(self.__cfgOb)
                idD = crExt.getChemCompAccessionMapping(extResource)
                dList = dbP.getDocuments(mapD=idD)
                #
                logger.info("Resource %r extracted mapped document length %d", extResource, len(dList))
                logger.debug("Objects %r", dList[:2])
                sD, _, collectionList, _ = self.__schP.getSchemaInfo(databaseName)
                collectionName = collectionList[0] if collectionList else "unassigned"
                indexL = sD.getDocumentIndex(collectionName, "primary")
                logger.info("Database %r collection %r index attributes %r", databaseName, collectionName, indexL)
                #
                collectionVersion = sD.getCollectionVersion(collectionName)
                addValues = {"_schema_version": collectionVersion}
                #
                addValues = {}
            #
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
            ok = dl.load(databaseName, collectionName, loadType=loadType, documentList=dList, indexAttributeList=indexL, keyNames=None, addValues=addValues)
            self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)

            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def getLoadStatus(self):
        return self.__statusList
