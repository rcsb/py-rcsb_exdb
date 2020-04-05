##
# File: UniProtEtlWorker.py
# Date: 9-Dec-2019  jdw
#
# ETL utilities for processing and loading UniProt reference data.
#
# Updates:
#
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from jsonschema import Draft4Validator
from jsonschema import FormatChecker

from rcsb.db.helpers.DocumentDefinitionHelper import DocumentDefinitionHelper
from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.db.utils.SchemaProvider import SchemaProvider
from rcsb.exdb.seq.ReferenceSequenceAssignmentProvider import ReferenceSequenceAssignmentProvider

#

logger = logging.getLogger(__name__)


class UniProtEtlWorker(object):
    """ Prepare and load UniProt 'core' sequence reference data collections.
    """

    def __init__(self, cfgOb, cachePath, useCache=True, numProc=2, chunkSize=10, readBackCheck=False, documentLimit=None, doValidate=False, verbose=False):
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
        self.__docHelper = DocumentDefinitionHelper(cfgOb=self.__cfgOb)
        self.__valInst = None
        self.__doValidate = doValidate
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

    def __getReferenceSequenceProvider(self):
        """
        """
        try:
            rsaP = ReferenceSequenceAssignmentProvider(
                self.__cfgOb,
                databaseName="pdbx_core",
                collectionName="pdbx_core_polymer_entity",
                polymerType="Protein",
                referenceDatabaseName="UniProt",
                provSource="PDB",
                useCache=self.__useCache,
                cachePath=self.__cachePath,
                fetchLimit=self.__documentLimit,
                siftsAbbreviated="TEST",
            )
            ok = rsaP.testCache()
            return ok, rsaP
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return None

    def load(self, updateId, extResource, loadType="full"):
        """ Load sequence reference data

        """
        try:
            self.__statusList = []
            desp = DataExchangeStatus()
            statusStartTimestamp = desp.setStartTime()
            #
            dList = indexL = []
            databaseName = collectionName = collectionVersion = None
            #
            if extResource == "UniProt":
                databaseName = "uniprot_core"
                # configName = self.__cfgOb.getDefaultSectionName()
                # dirPath = os.path.join(self.__cachePath, self.__cfgOb.get("EXDB_CACHE_DIR", self.__cfgOb.getDefaultSectionName()))
                #
                ok, rsP = self.__getReferenceSequenceProvider()
                if not ok:
                    return False
                #
                dList = rsP.getDocuments()
                logger.info("Resource %r extracted mapped document length %d", extResource, len(dList))
                logger.debug("Objects %r", dList[:2])
                #
                cDL = self.__docHelper.getCollectionInfo(databaseName)
                collectionName = cDL[0]["NAME"]
                collectionVersion = cDL[0]["VERSION"]
                indexL = self.__docHelper.getDocumentIndexAttributes(collectionName, "primary")
                logger.info("Database %r collection %r version %r index attributes %r", databaseName, collectionName, collectionVersion, indexL)
                addValues = {}
            else:
                logger.error("Unsupported external resource %r", extResource)
            #
            if self.__doValidate:
                self.__valInst = self.__getValidator(databaseName, collectionName, schemaLevel="full")
                for dObj in dList:
                    self.__validateObj(databaseName, collectionName, dObj, label="Original")
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
            okS = self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)

            return ok and okS
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def getLoadStatus(self):
        return self.__statusList

    def __getValidator(self, databaseName, collectionName, schemaLevel="full"):
        # _ = self.__schP.makeSchemaDef(databaseName, dataTyping="ANY", saveSchema=True)
        # cD = self.__schP.makeSchema(databaseName, collectionName, encodingType="JSON", level=schemaLevel, saveSchema=True)
        logger.info("Fetch schema for %r %r validation level %r", databaseName, collectionName, schemaLevel)
        cD = self.__schP.getJsonSchema(databaseName, collectionName, encodingType="JSON", level=schemaLevel)
        # Raises exceptions for schema compliance.
        Draft4Validator.check_schema(cD)
        valInst = Draft4Validator(cD, format_checker=FormatChecker())
        return valInst

    def __validateObj(self, databaseName, collectionName, rObj, label=""):
        try:
            eCount = 0
            tId = rObj["rcsb_id"] if rObj and "rcsb_id" in rObj else "anonymous"
            for error in sorted(self.__valInst.iter_errors(rObj), key=str):
                logger.info("Database %s collection %s (%s %r) path %s error: %s", databaseName, collectionName, label, tId, error.path, error.message)
                logger.debug(">>> Failing object is %r", rObj)
                if "rcsb_uniprot_feature" in rObj:
                    for dd in rObj["rcsb_uniprot_feature"]:
                        if "feature_id" in dd:
                            logger.info("feature_id %r", dd["feature_id"])
                        else:
                            logger.info("no feature_id keys %r", sorted(dd.keys()))
                            logger.info("description %r", dd["description"])
                eCount += 1
        except Exception as e:
            logger.exception("Validation failing %s", str(e))

        return eCount
