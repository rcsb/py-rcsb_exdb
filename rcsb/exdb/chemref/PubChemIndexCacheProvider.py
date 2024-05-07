##
# File: PubChemIndexCacheProvider.py
# Date: 2-Apr-2020  jdw
#
# Utilities to manage chemical component/BIRD to PubChem compound identifier mapping data.
#
# Updates:
#  9-May-2020 jdw separate cache behavior with separate option rebuildChemIndices=True/False
# 16-Jul-2020 jdw separate index and reference data management.
# 23-Jul-2021 jdw Make PubChemIndexCacheProvider a subclass of StashableBase()
#  2-Mar-2023 aae Return correct status from Single proc
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time

from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor
from rcsb.exdb.utils.ObjectUpdater import ObjectUpdater
from rcsb.utils.chem.ChemCompIndexProvider import ChemCompIndexProvider
from rcsb.utils.chem.ChemCompSearchIndexProvider import ChemCompSearchIndexProvider
from rcsb.utils.chemref.PubChemUtils import PubChemUtils, ChemicalIdentifier
from rcsb.utils.io.IoUtil import getObjSize
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.io.StashableBase import StashableBase
from rcsb.utils.io.TimeUtil import TimeUtil
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil


logger = logging.getLogger(__name__)


class PubChemUpdateWorker(object):
    """A skeleton worker class that implements the interface expected by the multiprocessing module
    for fetching CCD/BIRD to PubChem chemical compound identifier correspondences --
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
        self.__matchIndexCollectionName = "reference_match_index"
        self.__createCollections(self.__databaseName, self.__matchIndexCollectionName, indexAttributeNames=["rcsb_id", "rcsb_last_update"])
        self.__pcU = PubChemUtils()

    def __genChemIdList(self, ccId):
        """Return a list of ChemicalIdentifier() objects for the input chemical component identifier.

        Args:
            ccId (str): chemical component identifiers

        Returns:
            (list): list of ChemicalIdentifier() objects corresponding to the input chemical component.
        """
        chemIdList = []
        if ccId in self.__lookupD:
            for sD in self.__lookupD[ccId]:
                if "inchi-key" in sD:
                    idType = "inchikey"
                    descr = sD["inchi-key"]
                elif "smiles" in sD:
                    idType = "smiles"
                    descr = sD["smiles"]
                chemIdList.append(ChemicalIdentifier(idCode=ccId, identifierSource=sD["build-type"], identifierType=idType, identifier=descr, indexName=sD["name"]))
        return chemIdList

    def updateList(self, dataList, procName, optionsD, workingDir):
        """Update the input list of reference data identifiers (ChemicalIdentifier()) and return
        matching diagnostics and reference feature data.
        {
               "_id" : ObjectId("5e8dfb49eab967a0483a0472"),
               "rcsb_id" : "local reference ID (ccid|bird)", << LOCAL CANONICAL ID (e.g. ATP, PRD_000100)
               "rcsb_last_update" : ISODate("2020-04-08T16:26:47.993+0000"),
               "matched_ids" : [
                   {"matched_id":  "<external reference ID code>", "search_id_type" : "oe-smiles", "search_id_source": "model-xyz",
                                   'source_index_name': <>, 'source_inchikey': <>, 'source_smiles': <>},
                   {"matched_id":  "<external reference ID code>", "search_id_type": ... , "search_id_source": ... , ...}
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
        """
        _ = workingDir
        chunkSize = optionsD.get("chunkSize", 50)
        matchIdOnly = optionsD.get("matchIdOnly", True)
        # Path to store raw request data -
        exportPath = optionsD.get("exportPath", None)
        #
        successList = []
        retList1 = []
        retList2 = []
        diagList = []
        emptyList = []
        #
        try:
            tU = TimeUtil()
            ccIdList = dataList
            numChunks = len(list(self.__chunker(ccIdList, chunkSize)))
            logger.info("%s search starting for %d reference definitions (in chunks of length %d)", procName, len(ccIdList), chunkSize)
            for ii, ccIdChunk in enumerate(self.__chunker(ccIdList, chunkSize), 1):
                logger.info("%s starting chunk for %d of %d", procName, ii, numChunks)
                # tDL = []
                tIdxDL = []
                timeS = tU.getDateTimeObj(tU.getTimestamp())
                for ccId in ccIdChunk:
                    # Get various forms from the search index -
                    chemIdList = self.__genChemIdList(ccId)
                    tIdxD = {"rcsb_id": ccId, "rcsb_last_update": timeS}
                    #
                    mL = []
                    for chemId in chemIdList:
                        stA = time.time()
                        ok, refDL = self.__pcU.assemble(chemId, exportPath=exportPath, matchIdOnly=matchIdOnly)
                        #
                        if not ok:
                            etA = time.time()
                            logger.debug("Failing %s search source %s for %s (%.4f secs)", chemId.identifierType, chemId.identifierSource, chemId.idCode, etA - stA)

                        #
                        if ok and refDL:
                            for tD in refDL:
                                pcId = tD["cid"]
                                inchiKey = (
                                    self.__searchIdxD[chemId.indexName]["inchi-key"]
                                    if chemId.indexName in self.__searchIdxD and "inchi-key" in self.__searchIdxD[chemId.indexName]
                                    else None
                                )
                                smiles = (
                                    self.__searchIdxD[chemId.indexName]["smiles"] if chemId.indexName in self.__searchIdxD and "smiles" in self.__searchIdxD[chemId.indexName] else None
                                )
                                mL.append(
                                    {
                                        "matched_id": pcId,
                                        "search_id_type": chemId.identifierType,
                                        "search_id_source": chemId.identifierSource,
                                        "source_index_name": chemId.indexName,
                                        "source_smiles": smiles,
                                        "source_inchikey": inchiKey,
                                    }
                                )
                                # tD.update({"rcsb_id": pcId, "rcsb_last_update": timeS})
                                # tDL.append(tD)
                    #
                    if mL:
                        tIdxD["matched_ids"] = mL
                        successList.append(ccId)
                    else:
                        logger.info("No match result for any form of %s", ccId)
                    #
                    tIdxDL.append(tIdxD)
                # --
                startTimeL = time.time()
                logger.info("Saving chunk %d (len=%d)", ii, len(ccIdChunk))
                self.__updateObjectStore(self.__databaseName, self.__matchIndexCollectionName, tIdxDL)
                endTimeL = time.time()
                logger.info("Saved chunk %d (len=%d) in %.3f secs", ii, len(ccIdChunk), endTimeL - startTimeL)
        except Exception as e:
            logger.exception("Failing %s for %d data items %s", procName, len(dataList), str(e))
        logger.info("%s dataList length %d success length %d rst1 %d rst2 %d", procName, len(dataList), len(successList), len(retList1), len(retList2))
        #
        return successList, emptyList, emptyList, diagList

    def __updateObjectStore(self, databaseName, collectionName, objDL):
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
        return (iList[i: i + chunkSize] for i in range(0, len(iList), chunkSize))


class PubChemIndexCacheProvider(StashableBase):
    """Utilities to manage chemical component/BIRD to PubChem compound identifier mapping data."""

    def __init__(self, cfgOb, cachePath):
        dirName = "PubChem-index"
        super(PubChemIndexCacheProvider, self).__init__(cachePath, [dirName])
        self.__cfgOb = cfgOb
        self.__cachePath = cachePath
        self.__dirPath = os.path.join(self.__cachePath, dirName)
        #
        self.__databaseName = "pubchem_exdb"
        self.__matchIndexCollectionName = "reference_match_index"
        #

        self.__matchD = None

    def getMatchData(self, expireDays=0):
        if not self.__matchD:
            selectD = {}
            if expireDays > 0:
                tU = TimeUtil()
                tS = tU.getTimestamp(useUtc=True, before={"days": expireDays})
                selectD.update({"rcsb_latest_update": {"$lt": tU.getDateTimeObj(tS)}})
            self.__matchD = self.__getReferenceData(self.__databaseName, self.__matchIndexCollectionName, selectD=selectD)
            #
        return self.__matchD

    def testCache(self, minMatch=None, logSizes=False):
        self.getMatchData()
        okC = bool(self.__matchD)
        if not okC:
            return okC
        logger.info("Reference data cache lengths: matchD %d", len(self.__matchD))
        if minMatch and len(self.__matchD) < minMatch:
            return False
        #
        if logSizes:
            logger.info("PubChem MatchD %.2f", getObjSize(self.__matchD) / 1000000.0)
        return True

    def __getdumpFilePath(self, fmt="json"):
        stashBaseFileName = "pubchem_match_index_object_list"
        fExt = ".json" if fmt == "json" else ".pic"
        fp = os.path.join(self.__dirPath, stashBaseFileName + fExt)
        return fp

    def dump(self, fmt="json"):
        """Dump PubChem index reference data from the object store.

        Args:
            fmt (str, optional): [description]. Defaults to "json".

        Returns:
            bool: True for success or False otherwise
        """
        ok = False
        try:
            self.getMatchData()
            if fmt in ["json", "pickle"]:
                kwargs = {}
                fp = self.__getdumpFilePath(fmt=fmt)
                logger.info("Saving object store to %s", fp)
                mU = MarshalUtil(workPath=self.__dirPath)
                if fmt in ["json"]:
                    kwargs = {"indent": 3}
                ok = mU.doExport(fp, self.__matchD, fmt=fmt, **kwargs)
        except Exception as e:
            logger.exception("Failing for %r with %s", self.__dirPath, str(e))
        return ok

    def reloadDump(self, fmt="json"):
        """Reload PubChem reference data store from saved dump.

        Args:
            fmt (str, optional): format of the backup file (pickle or json). Defaults to "json".

        Returns:
            (int): number of objects restored.
        """
        numUpd = 0
        try:
            # Read from disk backup and update object store -
            if fmt in ["json", "pickle"]:
                fp = self.__getdumpFilePath(fmt="json")
                logger.info("Restoring object store from %s", fp)
                mU = MarshalUtil(workPath=self.__dirPath)
                matchD = mU.doImport(fp, fmt=fmt)
                numUpd = self.__reloadDump(matchD, self.__databaseName, self.__matchIndexCollectionName, indexAttributeNames=["rcsb_id", "rcsb_last_update"])
        except Exception as e:
            logger.exception("Failing for %r with %s", self.__dirPath, str(e))
        # --
        return numUpd

    def updateMissing(self, expireDays=0, fetchLimit=None, updateUnmatched=True, numProcChemComp=8, numProc=2, **kwargs):
        """Update match index from object store

        Args:
            expireDays (int): expiration days on match data (default 0 meaning none)
            fetchLimit (int): limit to the number of entry updates performed (None)
            updateUnmatched (bool): Previously unmatched search definitions will be retried on update (default=True)
            numProcChemComp (int): for rebuilding local ChemComp indices the number processors to apply (default=8)
            numProc (int): for rebuilding local PubChem indices the number processors to apply (default=2)

        Returns:
            bool: True for success or False otherwise

        ChemicalIdentifierFields = ("idCode", "identifierSource", "identifierType", "identifier")
        ChemicalIdentifier = collections.namedtuple("ChemicalIdentifier", ChemicalIdentifierFields, defaults=(None,) * len(ChemicalIdentifierFields))


                // Failed matches are recorded with NO matchedIds:
                {
                    "_id" : ObjectId("5e8dfb49eab967a0483a04a3"),
                    "rcsb_id" : "local reference ID (ccid|bird)", << LOCAL ID
                    "rcsb_last_update" : ISODate("2020-04-08T16:26:48.025+0000"),
                }
        """
        #
        matchD = {}
        matchedIdList = []
        ok = False
        try:
            # ---
            # Get current the indices of source chemical reference data -
            ok, ccidxP, ccsidxP = self.__rebuildChemCompSourceIndices(numProcChemComp, **kwargs)
            if not ok:
                return matchD
            #
            ccIdxD = ccidxP.getIndex()
            searchIdxD = ccsidxP.getIndex()
            # Index of target of local chemical component and BIRD identifiers
            sourceIdList = sorted(ccIdxD.keys())
            logger.info("Reloading chemical reference data (expireDays %r, updateUnmatched %r)", expireDays, updateUnmatched)
            matchedIdList = self.__getMatchIndexIds(searchIdxD, expireDays=expireDays, updateUnmatched=updateUnmatched)
            # --
            logger.info("Starting matched reference identifier count (%d) ", len(matchedIdList))
            updateIdList = sorted(set(sourceIdList) - set(matchedIdList))
            logger.info("Missing chemical definition correspondences %d fetchLimit %r", len(updateIdList), fetchLimit)
            #
            updateIdList = updateIdList[:fetchLimit] if fetchLimit else updateIdList
            #
            if updateIdList:
                logger.info("Update reference data cache for %d chemical identifiers", len(updateIdList))
                ok, failList = self.__updateReferenceData(updateIdList, searchIdxD, numProc, **kwargs)
                logger.info("Update reference data return status is %r missing count %d", ok, len(failList))
            else:
                logger.info("No reference data updates required")
            # --
            if not ok:
                logger.warning("updateMissing completed with status %r failures %r", ok, len(failList))
            #
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def getMatches(self):
        """Get all PubChem correspondences from the current match index..

        Returns:

            (list): PubChem compound identifier codes.

        """
        self.getMatchData()
        #
        pcidList = []
        try:
            pcidS = set()
            for _, mD in self.__matchD.items():
                if "matched_ids" in mD:
                    for sD in mD["matched_ids"]:
                        pcidS.add(sD["matched_id"])
            pcidList = list(pcidS)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return pcidList

    def getSelectedMatches(self, **kwargs):
        """Select preferred PubChem correspondences from the current match index for the input source component build type.
            and separatel return alternative matches for other source types.

        Args:
            sourceTypes (list, optional):  list of source chemical component build types (default: ["model-xyz"])
            exportPath: (str, optional): export path for correspondences

        Returns:
            dict, dict : mapD { ccId1: [{'pcId': ... , 'inchiKey': ... }], ccId2: ...},
                         altD { ccId1: [{'pcId': ... , 'inchiKey': ... 'sourceType': ... }], ccId2: ...}

                Example match index entry:
                {
                    "_id" : ObjectId("5e8dfb49eab967a0483a0472"),
                    "rcsb_id" : "local reference ID (ccid|bird)", << LOCAL CANONICAL ID (e.g. ATP, PRD_000100)
                    "rcsb_last_update" : ISODate("2020-04-08T16:26:47.993+0000"),
                    "matched_ids" : [
                        {"matched_id":  "<external reference ID code>", "search_id_type" : "oe-smiles", "search_id_source": "model-xyz",
                                        'source_index_name': <>, 'source_inchikey': <>, 'source_smiles': <>},
                        {"matched_id":  "<external reference ID code>", "search_id_type": ... , "search_id_source": ... , ...}
                        ]                          ]
                    },
                }
        """
        #
        self.getMatchData()

        sourceTypes = kwargs.get("sourceTypes", ["model-xyz"])
        exportPath = kwargs.get("exportPath", None)
        #
        mapD = {}
        altMapD = {}
        extraMapD = {}
        try:
            for ccId, mD in self.__matchD.items():
                if "matched_ids" in mD:
                    for sD in mD["matched_ids"]:
                        #
                        if sD and "search_id_source" in sD:
                            pcId = sD["matched_id"]
                            inchiKey = sD["source_inchikey"]
                            #
                            if sD["search_id_source"] in sourceTypes:
                                mapD.setdefault(ccId, []).append({"pcId": pcId, "inchiKey": inchiKey})
                            else:
                                altMapD.setdefault(ccId, []).append({"pcId": pcId, "inchiKey": inchiKey, "sourceType": sD["search_id_source"]})
            #
            difS = set(altMapD.keys()) - set(mapD.keys())
            logger.info("PubChem preferred correspondence length (%d) alternative extras (%d)", len(mapD), len(difS))
            for ccId in difS:
                extraMapD[ccId] = altMapD[ccId]
            if exportPath:
                fp = os.path.join(exportPath, "pubchem_matches.json")
                mU = MarshalUtil(workPath=exportPath)
                mU.doExport(fp, mapD, fmt="json", indent=3)
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return mapD, extraMapD

    #
    # -- Extract current data from object store --
    def __getMatchIndexIds(self, searchIdxD, expireDays=0, updateUnmatched=True):
        """Get CCD/BIRD reference data identifiers in the current match index subject to an
           expiration interval (i.e. not matched or older than deltaDays).

        Args:
            searchIdxD (dict): CCD/BIRD search index dictionary
            expireDays (int, optional): expiration interval in days. Defaults to 0 (no expiration).
            updateUnmatched (bool, optional): include only matched identifiers (i.e. exclude any tried but unmatched cases)

        Returns:
            (list): chemical component/BIRD reference identifier list
        """
        selectD = {}
        if expireDays > 0:
            tU = TimeUtil()
            tS = tU.getTimestamp(useUtc=True, before={"days": expireDays})
            selectD.update({"rcsb_latest_update": {"$lt": tU.getDateTimeObj(tS)}})
        #
        if updateUnmatched:
            # Return only cases with an existing correspondence
            selectD.update({"matched_ids": {"$exists": True}})
        matchD = self.__getReferenceData(self.__databaseName, self.__matchIndexCollectionName, selectD=selectD if selectD else None)
        #
        # For the selected cases in the index-
        retIdList = []
        if searchIdxD:
            # Exclude definitions if source InChIKey in the match index differs with the Key in the current search index.
            for ccId, inD in matchD.items():
                if updateUnmatched and "matched_ids" not in inD:
                    retIdList.append(ccId)
                    continue
                hasChanged = False
                for mD in inD["matched_ids"]:
                    if mD["source_index_name"] not in searchIdxD:
                        hasChanged = True
                        logger.info("Identifier %s no longer in search index", mD["source_index_name"])
                        break
                    if mD["source_inchikey"] != searchIdxD[mD["source_index_name"]]["inchi-key"]:
                        logger.info("Identifier %s InChIKey changed search index", mD["source_index_name"])
                        hasChanged = True
                        break
                if not hasChanged:
                    retIdList.append(ccId)
        #
        return sorted(retIdList)

    #
    def __getReferenceData(self, databaseName, collectionName, selectD=None, selectionList=None):
        logger.info("Searching %s %s with selection query %r", databaseName, collectionName, selectD)
        obEx = ObjectExtractor(
            self.__cfgOb,
            databaseName=databaseName,
            collectionName=collectionName,
            keyAttribute="rcsb_id",
            uniqueAttributes=["rcsb_id"],
            selectionQuery=selectD,
            selectionList=selectionList,
            stripObjectId=True,
        )
        docCount = obEx.getCount()
        logger.info("Reference data object count %d", docCount)
        objD = obEx.getObjects()
        return objD

    def __updateReferenceData(self, idList, searchIdxD, numProc=2, **kwargs):
        """Launch worker methods to update chemical reference data correspondences.

        Args:
            idList (list): list of local chemical identifiers (ChemIdentifier())

        Returns:
            (bool, list): status flag, list of unmatched identifiers
        """
        chunkSize = 50
        exportPath = kwargs.get("exportPath", None)
        logger.info("Length starting list is %d", len(idList))
        optD = {"chunkSize": chunkSize, "exportPath": exportPath, "matchIdOnly": True}
        rWorker = PubChemUpdateWorker(self.__cfgOb, searchIdxD)
        if numProc > 1:
            mpu = MultiProcUtil(verbose=True)
            mpu.setOptions(optD)
            mpu.set(workerObj=rWorker, workerMethod="updateList")
            ok, failList, resultList, _ = mpu.runMulti(dataList=idList, numProc=numProc, numResults=2, chunkSize=chunkSize)
            logger.info("Multi-proc %r failures %r result lengths %r %r", ok, len(failList), len(resultList[0]), len(resultList[1]))
        else:
            successList, _, _, _ = rWorker.updateList(idList, "SingleProc", optD, self.__dirPath)
            failList = list(set(idList) - set(successList))
            ok = len(failList) == 0
            logger.info("Single-proc status %r failures %r", ok, len(failList))
        #
        return ok, failList

    def __reloadDump(self, objD, databaseName, collectionName, indexAttributeNames=None):
        """Internal method to restore the input database/collection using the input data object.

        Args:
            objD (obj): Target reference or index data object
            databaseName (str): target database name
            collectionName (str): target collection name
            indexAttributeNames (list, optional): Primary index attributes. Defaults to None.

        Returns:
            int: inserted or updated object count
        """
        try:
            numUpd = 0
            numTotal = 0
            updateDL = []
            for entityKey, obj in objD.items():
                if "_id" in obj:
                    obj.pop("_id")
                selectD = {"rcsb_id": entityKey}
                updateDL.append({"selectD": selectD, "updateD": obj})
            #
            obUpd = ObjectUpdater(self.__cfgOb)
            ok = obUpd.createCollection(databaseName, collectionName, indexAttributeNames=indexAttributeNames, checkExists=True, bsonSchema=None)
            if ok:
                numUpd = obUpd.update(databaseName, collectionName, updateDL)
                logger.debug("Updated object count is %d", numUpd)
            else:
                logger.error("Create %s %s failed", databaseName, collectionName)
            numTotal = obUpd.count(databaseName, collectionName)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return numTotal

    #                           --- --- ---
    # -- Load or rebuild source chemical reference data indices --
    def __rebuildChemCompSourceIndices(self, numProc, **kwargs):
        """Rebuild source indices of chemical component definitions."""
        logger.info("Rebuilding chemical definition index.")
        ok1, ccidxP = self.__buildChemCompIndex(**kwargs)
        logger.info("__buildChemCompIndex completed with status %r", ok1)
        logger.info("Rebuilding chemical search indices.")
        ok2, ccsidxP = self.__buildChemCompSearchIndex(numProc, **kwargs)
        logger.info("__buildChemCompSearchIndex completed with status %r", ok2)
        return ok1 & ok2, ccidxP, ccsidxP

    def __buildChemCompIndex(self, **kwargs):
        """Build chemical component cache files from the input component dictionaries"""
        try:
            molLimit = kwargs.get("molLimit", None)
            useCache = not kwargs.get("rebuildChemIndices", False)
            logSizes = kwargs.get("logSizes", False)
            ccFileNamePrefix = kwargs.get("ccFileNamePrefix", "cc-full")
            ccUrlTarget = kwargs.get("ccUrlTarget", None)
            birdUrlTarget = kwargs.get("birdUrlTarget", None)
            cachePath = kwargs.get("cachePath", self.__cachePath)
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

    def __buildChemCompSearchIndex(self, numProc, **kwargs):
        """Test build search index chemical component cache files from the input component dictionaries"""
        try:
            cachePath = kwargs.get("cachePath", self.__cachePath)
            molLimit = kwargs.get("molLimit", None)
            useCache = not kwargs.get("rebuildChemIndices", False)
            logSizes = kwargs.get("logSizes", False)
            limitPerceptions = kwargs.get("limitPerceptions", False)
            #
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
