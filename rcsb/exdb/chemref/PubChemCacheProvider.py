##
# File: PubChemCacheProvider.py
# Date: 2-Apr-2020  jdw
#
# Utilities to cache chemical referencence data and mappings for PubChem
#
# Updates:
# 9-May-2020 jdw separate cache behavior with separate option rebuildChemIndices=True/False
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor
from rcsb.exdb.utils.ObjectUpdater import ObjectUpdater
from rcsb.utils.chem.ChemCompIndexProvider import ChemCompIndexProvider
from rcsb.utils.chem.ChemCompSearchIndexProvider import ChemCompSearchIndexProvider
from rcsb.utils.chemref.PubChemUtils import PubChemUtils, ChemicalIdentifier
from rcsb.utils.io.IoUtil import getObjSize
from rcsb.utils.io.TimeUtil import TimeUtil
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil


logger = logging.getLogger(__name__)


class ReferenceUpdateWorker(object):
    """  A skeleton worker class that implements the interface expected by the multiprocessing
         for fetching chemical reference data --
    """

    def __init__(self, cfgOb, searchIdxD, **kwargs):
        self.__cfgOb = cfgOb
        self.__searchIdxD = searchIdxD
        #
        _ = kwargs
        self.__lookupD = {}
        for sId, sD in self.__searchIdxD.items():
            ccId = sId.split("|")[0]
            self.__lookupD.setdefault(ccId, []).append(sD)
        self.__databaseName = "pubchem_exdb"
        self.__refDataCollectionName = "reference_entry"
        self.__matchDataCollectionName = "reference_match"
        self.__createCollections(self.__databaseName, self.__refDataCollectionName, indexAttributeNames=["rcsb_id", "rcsb_last_update"])
        self.__createCollections(self.__databaseName, self.__matchDataCollectionName, indexAttributeNames=["rcsb_id", "rcsb_last_update"])

    def __genChemIdList(self, ccId):
        chemIdList = []
        if ccId in self.__lookupD:
            for sD in self.__lookupD[ccId]:
                if "inchi-key" in sD:
                    idType = "inchikey"
                    descr = sD["inchi-key"]
                elif "smiles" in sD:
                    idType = "smiles"
                    descr = sD["smiles"]
                chemIdList.append(ChemicalIdentifier(idCode=ccId, identifierSource=sD["build-type"], identifierType=idType, identifier=descr))
        return chemIdList

    def updateList(self, dataList, procName, optionsD, workingDir):
        """  Update the input list of reference data identifiers (ChemicalIdentifier()) and return
             matching diagnostics and reference feature data.
             {
                    "_id" : ObjectId("5e8dfb49eab967a0483a0472"),
                    "rcsb_id" : "local reference ID (ccid|bird)", << LOCAL CANNONICAL ID (e.g. ATP, PRD_000100)
                    "rcsb_last_update" : ISODate("2020-04-08T16:26:47.993+0000"),
                    "matched_ids" : [
                        {"matched_id":  "<external reference ID code>", "search_id_type" : "oe-smiles", "search_id_source": "model-xyz"},
                        {"matched_id":  "<external reference ID code>", "search_id_type": ... , "search_id_source": ... }
                        ]                          ]
                    },
                }
                // Failed matches are recorded with NO matchedIds:
                {
                    "_id" : ObjectId("5e8dfb49eab967a0483a04a3"),
                    "rcsb_id" : "local reference ID (ccid|bird)", << LOCAL ID
                    "rcsb_last_update" : ISODate("2020-04-08T16:26:48.025+0000"),
                }
                #
                ChemicalIdentifier(idCode=uId, identifierSource=sD["build-type"], identifierType=idType, identifier=descr))
                #
        """
        _ = workingDir
        chunkSize = optionsD.get("chunkSize", 50)
        exportPath = optionsD.get("exportPath", None)
        #
        successList = []
        retList1 = []
        retList2 = []
        diagList = []
        emptyList = []
        #
        pcU = PubChemUtils()
        try:
            tU = TimeUtil()
            ccIdList = dataList
            logger.info("%s starting update for %d (%d) reference definitions", procName, len(ccIdList), chunkSize)
            for ccIdChunk in self.__chunker(ccIdList, chunkSize):
                tDL = []
                tIdxDL = []
                timeS = tU.getDateTimeObj(tU.getTimestamp())
                for ccId in ccIdChunk:
                    # Get various forms from the search index -
                    chemIdList = self.__genChemIdList(ccId)
                    tIdxD = {"rcsb_id": ccId, "rcsb_last_update": timeS}
                    #
                    mL = []
                    for chemId in chemIdList:
                        ok, refDL = pcU.assemble(chemId, exportPath=exportPath)
                        #
                        if ok and refDL:
                            for tD in refDL:
                                pcId = tD["cid"]
                                mL.append({"matched_id": pcId, "search_id_type": chemId.identifierType, "search_id_source": chemId.identifierSource})
                                tD.update({"rcsb_id": pcId, "rcsb_last_update": timeS})
                                tDL.append(tD)
                    #
                    if mL:
                        tIdxD["matched_ids"] = mL
                        successList.append(ccId)
                    else:
                        logger.info("No match result %s", ccId)
                    #
                    tIdxDL.append(tIdxD)
                # --
                self.__updateReferenceData(self.__databaseName, self.__matchDataCollectionName, tIdxDL)
                self.__updateReferenceData(self.__databaseName, self.__refDataCollectionName, tDL)
        except Exception as e:
            logger.exception("Failing %s for %d data items %s", procName, len(dataList), str(e))
        logger.info("%s dataList length %d success length %d rst1 %d rst2 %d", procName, len(dataList), len(successList), len(retList1), len(retList2))
        #
        return successList, emptyList, emptyList, diagList

    def __updateReferenceData(self, databaseName, collectionName, objDL):
        updateDL = []
        for objD in objDL:
            try:
                selectD = {"rcsb_id": objD["rcsb_id"]}
                updateDL.append({"selectD": selectD, "updateD": objD})
            except Exception as e:
                logger.exception("Failing with %s", str(e))
        obUpd = ObjectUpdater(self.__cfgOb)
        numUpd = obUpd.update(databaseName, collectionName, updateDL)
        logger.info("Updated reference count is %d", numUpd)

    def __createCollections(self, databaseName, collectionName, indexAttributeNames=None):
        obUpd = ObjectUpdater(self.__cfgOb)
        ok = obUpd.createCollection(databaseName, collectionName, indexAttributeNames=indexAttributeNames, checkExists=True, bsonSchema=None)
        return ok

    def __chunker(self, iList, chunkSize):
        chunkSize = max(1, chunkSize)
        return (iList[i : i + chunkSize] for i in range(0, len(iList), chunkSize))


class PubChemCacheProvider(object):
    """  Utilities to cache chemical referencence data and identifier mappings for PubChem compound data

    """

    def __init__(self, cfgOb, chunkSize=100, expireDays=0, numProc=1, fetchLimit=None, **kwargs):
        self.__cfgOb = cfgOb
        #
        self.__chunkSize = chunkSize
        self.__numProc = numProc
        self.__useCache = kwargs.get("useCache", True)
        #
        self.__databaseName = "pubchem_exdb"
        self.__refDataCollectionName = "reference_entry"
        self.__matchDataCollectionName = "reference_match"
        #
        self.__matchD, self.__refD = self.__reload(expireDays, fetchLimit, **kwargs)

    def getMatchInfo(self):
        return self.__matchD

    def getRefData(self):
        return self.__refD

    def getRefDataCount(self):
        return len(self.__refD)

    def testCache(self, minMatch=None, logSizes=False):
        okC = bool(self.__matchD and self.__refD)
        if not okC:
            return okC
        logger.info("Reference data cache lengths: matchD %d refD %d", len(self.__matchD), len(self.__refD))
        if minMatch and len(self.__matchD) < minMatch:
            return False
        #
        if logSizes:
            logger.info(
                "RefMatchD %.2f RefD %.2f", getObjSize(self.__matchD) / 1000000.0, getObjSize(self.__refD) / 1000000.0,
            )
        return True

    # -- Extract current data from object store --
    def __getReferenceDataIds(self, expireDays=0, filterMatched=True):
        """Get reference data identifiers subject to an expiration interval (i.e. not updated in/older than deltaDays)

        Args:
            expireDays (int, optional): expiration interval in days. Defaults to 0.

        Returns:
            (list): reference identifier list
        """
        selectD = {}
        if expireDays > 0:
            tU = TimeUtil()
            tS = tU.getTimestamp(useUtc=True, before={"days": expireDays})
            selectD.update({"rcsb_latest_update": {"$lt": tU.getDateTimeObj(tS)}})
        if filterMatched:
            tU = TimeUtil()
            tS = tU.getTimestamp(useUtc=True, before={"days": expireDays})
            selectD.update({"matched_ids": {"$exists": True}})
        matchD = self.__getReferenceData(self.__databaseName, self.__matchDataCollectionName, selectD=selectD if selectD else None)
        return sorted(matchD.keys())

    # --
    def __getReferenceData(self, databaseName, collectionName, selectD=None):
        logger.info("Searching %s %s with selection query %r", databaseName, collectionName, selectD)
        obEx = ObjectExtractor(
            self.__cfgOb, databaseName=databaseName, collectionName=collectionName, keyAttribute="rcsb_id", uniqueAttributes=["rcsb_id"], selectionQuery=selectD,
        )
        docCount = obEx.getCount()
        logger.debug("Reference data match count %d", docCount)
        objD = obEx.getObjects()
        return objD

    def __reload(self, expireDays, fetchLimit, **kwargs):
        """[summary]

        Args:
            expireDays ([type]): [description]

        Returns:
            [type]: [description]
            Match Object -


        ChemicalIdentifierFields = ("idCode", "identifierSource", "identifierType", "identifier")
        ChemicalIdentifier = collections.namedtuple("ChemicalIdentifier", ChemicalIdentifierFields, defaults=(None,) * len(ChemicalIdentifierFields))

        # --  Source Search index - entry ---- distinct descriptors for each local chemical definition ----
            "PRD_000009|oe-iso-smiles": {
                    "name": "PRD_000009|oe-iso-smiles",
                    "build-type": "oe-iso-smiles",
                    "smiles": "Cc1ccc(c2c1OC3=C(C(=O)C(=C(C3=N2)C(=O)N[C@H]4[C@H](OC(=O)[C@@H](N(C(=O)CN(C(=O)[C@@H]5CC(=O)[C@@H]...",
                    "inchi-key": "PLQQUUFBVCDPMY-VFSNQXKDSA-N",
                    "formula": "C62H83ClN12O18",
                    "fcharge": 0,
                    "elementCounts": {
                    "C": 62,
                    "O": 18,
                    "N": 12,
                    "H": 83,
                    "CL": 1
                    }
                //  --- Match index object example
                {
                    "_id" : ObjectId("5e8dfb49eab967a0483a0472"),
                    "rcsb_id" : "local reference ID (ccid|bird)", << LOCAL CANNONICAL ID (e.g. ATP, PRD_000100)
                    "rcsb_last_update" : ISODate("2020-04-08T16:26:47.993+0000"),
                    "matched_ids" : [
                        {"matched_id":  "<external reference ID code>", "search_id_type" : "oe-smiles", "search_id_source": "model-xyz"},
                        {"matched_id":  "<external reference ID code>", "search_id_type": ... , "search_id_source": ... }
                        ]                          ]
                    },
                }
                // Failed matches are recorded with NO matchedIds:
                {
                    "_id" : ObjectId("5e8dfb49eab967a0483a04a3"),
                    "rcsb_id" : "local reference ID (ccid|bird)", << LOCAL ID
                    "rcsb_last_update" : ISODate("2020-04-08T16:26:48.025+0000"),
                }
        """
        #
        matchD = {}
        refD = {}
        matchedIdList = []
        try:
            # --
            # Get current the indices of source chemical reference data -
            ok, ccidxP, ccsidxP = self.__rebuildChemCompSourceIndices(**kwargs)
            if not ok:
                return matchD, refD
            ccIdxD = ccidxP.getIndex()
            # Target list of local chemical component and BIRD identifiers
            sourceIdList = sorted(ccIdxD.keys())
            if self.__useCache:
                logger.info("Reloading chemical reference data (expireDays %r)", expireDays)
                matchedIdList = self.__getReferenceDataIds(expireDays=expireDays)
            # --
            logger.info("With useCache %r matched reference identifier count (%d) ", self.__useCache, len(matchedIdList))
            updateIdList = sorted(set(sourceIdList) - set(matchedIdList))
            logger.info("Missing chemical definition correspondences %d fetchLimit %r", len(updateIdList), fetchLimit)
            #
            updateIdList = updateIdList[:fetchLimit] if fetchLimit else updateIdList
            #
            if updateIdList:
                searchIdxD = ccsidxP.getIndex()
                logger.info("Update reference data cache for %d chemical identifers", len(updateIdList))
                ok, failList = self.__updateReferenceData(updateIdList, searchIdxD, **kwargs)
                logger.info("Update reference data status is %r missing count %d", ok, len(failList))
            else:
                logger.info("No reference data updates required")
            #
            matchD = self.__getReferenceData(self.__databaseName, self.__matchDataCollectionName)
            refD = self.__getReferenceData(self.__databaseName, self.__refDataCollectionName)
            logger.info("Completed - returning match length %d and reference data length %d", len(matchD), len(refD))
            return matchD, refD
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return None, None

    def __updateReferenceData(self, idList, searchIdxD, **kwargs):
        """Launch worker methods to update chemical reference data correspondences.

        Args:
            idList (list): list of local chemical identifiers (ChemIndentifier())

        Returns:
            (bool, list): status flag, list of unmatched identifiers
        """
        numProc = self.__numProc
        chunkSize = self.__chunkSize
        exportPath = kwargs.get("exportPath", None)
        logger.info("Length starting list is %d", len(idList))
        optD = {"chunkSize": chunkSize, "exportPath": exportPath}
        rWorker = ReferenceUpdateWorker(self.__cfgOb, searchIdxD)
        mpu = MultiProcUtil(verbose=True)
        mpu.setOptions(optD)
        mpu.set(workerObj=rWorker, workerMethod="updateList")
        ok, failList, resultList, _ = mpu.runMulti(dataList=idList, numProc=numProc, numResults=2, chunkSize=chunkSize)
        logger.info("Multi-proc %r failures %r result lengths %r %r", ok, len(failList), len(resultList[0]), len(resultList[1]))
        return ok, failList

    # -- --- ---
    # -- Load or rebuild source chemical reference data indices --
    def __rebuildChemCompSourceIndices(self, **kwargs):
        """ Rebuild source indices of chemical component definitions.
        """

        ok1, ccidxP = self.__buildChemCompIndex(**kwargs)
        ok2, ccsidxP = self.__buildChemCompSearchIndex(**kwargs)
        return ok1 & ok2, ccidxP, ccsidxP

    def __buildChemCompIndex(self, **kwargs):
        """ Build chemical component cache files from the input component dictionaries
        """
        try:
            molLimit = kwargs.get("molLimit", None)
            useCache = not kwargs.get("rebuildChemIndices", False)
            logSizes = kwargs.get("logSizes", False)
            ccFileNamePrefix = kwargs.get("ccFileNamePrefix", "cc-full")
            ccUrlTarget = kwargs.get("ccUrlTarget", None)
            birdUrlTarget = kwargs.get("birdUrlTarget", None)
            cachePath = kwargs.get("cachePath", ".")
            #
            ccidxP = ChemCompIndexProvider(
                ccUrlTarget=ccUrlTarget, birdUrlTarget=birdUrlTarget, cachePath=cachePath, useCache=useCache, molLimit=molLimit, ccFileNamePrefix=ccFileNamePrefix
            )
            ok = ccidxP.testCache(minCount=molLimit, logSizes=logSizes)
            return ok, ccidxP if ok else None
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return False, None

    def __buildChemCompSearchIndex(self, **kwargs):
        """ Test build search index chemical component cache files from the input component dictionaries
        """
        try:
            cachePath = kwargs.get("cachePath", ".")
            molLimit = kwargs.get("molLimit", None)
            useCache = not kwargs.get("rebuildChemIndices", False)
            logSizes = kwargs.get("logSizes", False)
            limitPerceptions = kwargs.get("limitPerceptions", False)
            numProc = kwargs.get("numProc", 1)
            chunkSize = kwargs.get("chunkSize", 5)
            molLimit = kwargs.get("molLimit", None)
            ccFileNamePrefix = kwargs.get("ccFileNamePrefix", "cc-full")
            quietFlag = kwargs.get("quietFlag", True)
            ccUrlTarget = kwargs.get("ccUrlTarget", None)
            birdUrlTarget = kwargs.get("birdUrlTarget", None)
            #
            ccsiP = ChemCompSearchIndexProvider(
                ccUrlTarget=ccUrlTarget,
                birdUrlTarget=birdUrlTarget,
                cachePath=cachePath,
                useCache=useCache,
                molLimit=molLimit,
                ccFileNamePrefix=ccFileNamePrefix,
                limitPerceptions=limitPerceptions,
                numProc=numProc,
                maxChunkSize=chunkSize,
                quietFlag=quietFlag,
            )
            ok = ccsiP.testCache(minCount=molLimit, logSizes=logSizes)
            return ok, ccsiP if ok else None
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False, None
