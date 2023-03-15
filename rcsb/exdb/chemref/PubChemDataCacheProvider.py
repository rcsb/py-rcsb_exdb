##
# File: PubChemDataCacheProvider.py
# Date: 2-Apr-2020  jdw
#
# Utilities to cache chemical reference data and mappings for PubChem
#
# Updates:
#  9-May-2020 jdw separate cache behavior with separate option rebuildChemIndices=True/False
# 16-Jul-2020 jdw separate index and reference data management.
# 23-Jul-2021 jdw Make PubChemDataCacheProvider a subclass of StashableBase()
# 15-Mar-2023 aae Update default numProc to 2
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
from rcsb.utils.chemref.PubChemUtils import PubChemUtils, ChemicalIdentifier
from rcsb.utils.io.IoUtil import getObjSize
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.io.StashableBase import StashableBase
from rcsb.utils.io.TimeUtil import TimeUtil
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil


logger = logging.getLogger(__name__)


class PubChemDataUpdateWorker(object):
    """A skeleton worker class that implements the interface expected by the multiprocessing module
    for fetching PubChem chemical reference data --
    """

    def __init__(self, cfgOb, **kwargs):
        self.__cfgOb = cfgOb
        #
        _ = kwargs
        self.__databaseName = "pubchem_exdb"
        self.__refDataCollectionName = "reference_entry"
        self.__createCollections(self.__databaseName, self.__refDataCollectionName, indexAttributeNames=["rcsb_id", "rcsb_last_update"])
        self.__pcU = PubChemUtils()

    def updateList(self, dataList, procName, optionsD, workingDir):
        """Update the input list of reference data identifiers (ChemicalIdentifier()) and return
        matching diagnostics and reference feature data.

        """
        _ = workingDir
        chunkSize = optionsD.get("chunkSize", 50)
        # Path to store raw request data -
        exportPath = optionsD.get("exportPath", None)
        #
        successList = []
        retList1 = []
        retList2 = []
        diagList = []
        emptyList = []
        # -
        try:
            tU = TimeUtil()
            pcidList = dataList
            numChunks = len(list(self.__chunker(pcidList, chunkSize)))
            logger.info("%s search starting for %d reference definitions (in chunks of length %d)", procName, len(pcidList), chunkSize)
            for ii, pcidChunk in enumerate(self.__chunker(pcidList, chunkSize), 1):
                logger.info("%s starting chunk for %d of %d", procName, ii, numChunks)
                tDL = []
                timeS = tU.getDateTimeObj(tU.getTimestamp())
                for pcid in pcidChunk:
                    #
                    chemId = ChemicalIdentifier(idCode=pcid, identifierType="cid", identifier=pcid, identifierSource="ccd-match")
                    #
                    stA = time.time()
                    ok, refDL = self.__pcU.assemble(chemId, exportPath=exportPath)
                    #
                    if not ok:
                        etA = time.time()
                        logger.info("Failing %s search source %s for %s (%.4f secs)", chemId.identifierType, chemId.identifierSource, chemId.idCode, etA - stA)

                    #
                    if ok and refDL:
                        successList.append(pcid)
                        for tD in refDL:
                            tD.update({"rcsb_id": tD["cid"], "rcsb_last_update": timeS})
                            tDL.append(tD)
                    else:
                        logger.info("No match result for any form of %s", pcid)
                # --
                startTimeL = time.time()
                logger.info("Saving chunk %d (len=%d)", ii, len(pcidChunk))
                self.__updateObjectStore(self.__databaseName, self.__refDataCollectionName, tDL)
                endTimeL = time.time()
                logger.info("Saved chunk %d (len=%d) in %.3f secs", ii, len(pcidChunk), endTimeL - startTimeL)
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
        return (iList[i : i + chunkSize] for i in range(0, len(iList), chunkSize))


class PubChemDataCacheProvider(StashableBase):
    """Utilities to cache chemical reference data extracted from PubChem compound data"""

    def __init__(self, cfgOb, cachePath):
        dirName = "PubChem-data"
        super(PubChemDataCacheProvider, self).__init__(cachePath, [dirName])
        self.__cfgOb = cfgOb
        self.__dirPath = os.path.join(cachePath, dirName)
        #
        self.__databaseName = "pubchem_exdb"
        self.__refDataCollectionName = "reference_entry"
        #
        self.__refD = None

    def getRefData(self, expireDays=0):
        if not self.__refD:
            selectD = {}
            if expireDays > 0:
                tU = TimeUtil()
                tS = tU.getTimestamp(useUtc=True, before={"days": expireDays})
                selectD.update({"rcsb_latest_update": {"$lt": tU.getDateTimeObj(tS)}})
            self.__refD = self.__getReferenceData(self.__databaseName, self.__refDataCollectionName, selectD=selectD)
            #
        return self.__refD

    def getRefIdCodes(self, expireDays=0):
        selectD = {}
        selectionList = ["rcsb_id"]
        if expireDays > 0:
            tU = TimeUtil()
            tS = tU.getTimestamp(useUtc=True, before={"days": expireDays})
            selectD.update({"rcsb_latest_update": {"$lt": tU.getDateTimeObj(tS)}})
        refIds = self.__getReferenceData(self.__databaseName, self.__refDataCollectionName, selectD=selectD, selectionList=selectionList)
        #
        return list(refIds.keys()) if refIds else []

    def getRefDataCount(self):
        return len(self.__refD) if self.__refD else 0

    def testCache(self, minCount=None, logSizes=False):
        okC = bool(self.__refD)
        if not okC:
            return okC
        logger.info("Reference data cache lengths: refD %d", len(self.__refD))
        if minCount and len(self.__refD) < minCount:
            return False
        #
        if logSizes:
            logger.info("refD %.2f", getObjSize(self.__refD) / 1000000.0)
        return True

    def __getdumpFilePath(self, fmt="json"):
        stashBaseFileName = "pubchem_match_data_object_list"
        fExt = ".json" if fmt == "json" else ".pic"
        fp = os.path.join(self.__dirPath, stashBaseFileName + fExt)
        return fp

    def dump(self, fmt="json"):
        """Dump PubChem reference data from the object store.

        Args:
            fmt (str, optional): backup file format. Defaults to "json".

        Returns:
            (bool): True for success or False otherwise
        """
        ok = False
        try:
            self.getRefData()
            if fmt in ["json", "pickle"]:
                kwargs = {}
                fp = self.__getdumpFilePath(fmt=fmt)
                logger.info("Saving object store to %s", fp)
                mU = MarshalUtil(workPath=self.__dirPath)
                if fmt in ["json"]:
                    kwargs = {"indent": 3}
                ok = mU.doExport(fp, self.__refD, fmt=fmt, **kwargs)
        except Exception as e:
            logger.exception("Failing for %r with %s", self.__dirPath, str(e))
        return ok

    def reloadDump(self, fmt="json"):
        """Load PubChem reference data store from saved dump.

        Args:
            fmt (str, optional): format of the backup file (pickle or json). Defaults to "json".

        Returns:
            (int): number of objects restored.
        """
        numUpd = 0
        try:
            # Read from disk backup and update object store -
            if fmt in ["json", "pickle"]:
                fp = self.__getdumpFilePath(fmt=fmt)
                logger.info("Restoring object store from %s", fp)
                mU = MarshalUtil(workPath=self.__dirPath)
                refD = mU.doImport(fp, fmt=fmt)
                numUpd = self.__reloadDump(refD, self.__databaseName, self.__refDataCollectionName, indexAttributeNames=["rcsb_id", "rcsb_last_update"])
        except Exception as e:
            logger.exception("Failing for %r with %s", self.__dirPath, str(e))
        # --
        return numUpd

    def updateMissing(self, idList, exportPath=None, numProc=2, chunkSize=5):
        """Fetch and load reference data for any missing PubChem ID codes in the input list.

        Args:
            idList (list): PubChem ID codes
            numProc (int, optional): number of processor to use. Defaults to 2.
            chunkSize (int, optional): chunk size between data store updates. Defaults to 5.
            exportPath (str, optional): store raw fetched data in this path. Defaults to None.

        Returns:
            (bool, list): status flag, list of failed identifiers
        """
        curIdList = self.getRefIdCodes()
        missS = set(idList) - set(curIdList)
        if missS:
            logger.info("Loading (%d) missing identifiers", len(missS))
            ok, failList = self.load(list(missS), numProc=numProc, chunkSize=chunkSize, exportPath=exportPath)
        else:
            logger.info("No missing identifier - nothing to load")
            ok = True
            failList = []

        return ok, failList

    def load(self, idList, exportPath=None, numProc=2, chunkSize=5):
        """Fetch and load reference data for the input list of PubChem compound codes.

        Args:
            idList (list): PubChem ID codes
            exportPath (str, optional): store raw fetched data in this path. Defaults to None.
            numProc (int, optional): number of processor to use. Defaults to 2.
            chunkSize (int, optional): chunk size between data store updates. Defaults to 5.


        Returns:
            (bool, list): status flag, list of failed identifiers

        """
        logger.info("Length starting list is %d", len(idList))
        optD = {"chunkSize": chunkSize, "exportPath": exportPath}
        rWorker = PubChemDataUpdateWorker(self.__cfgOb)
        if numProc > 1:
            mpu = MultiProcUtil(verbose=True)
            mpu.setOptions(optD)
            mpu.set(workerObj=rWorker, workerMethod="updateList")
            ok, failList, resultList, _ = mpu.runMulti(dataList=idList, numProc=numProc, numResults=2, chunkSize=chunkSize)
            logger.info("Multi-proc %r failures %r result lengths %r %r", ok, len(failList), len(resultList[0]), len(resultList[1]))
        else:
            successList, _, _, _ = rWorker.updateList(idList, "SingleProc", optD, None)
            failList = list(set(idList) - set(successList))
            ok = len(failList) == 0
            logger.info("Single-proc status %r failures %r", ok, len(failList))
        #
        return ok, failList

    def getRelatedMapping(self, pcidList):
        """Assemble related identifiers (xrefs) for the input PubChem compound Id list.

        Args:
            pcidList (list): PubChem compound ID list

        Returns:
            dict :{<pcid>: {'relatedId1': ... 'relatedId2': ... }, ...}

        """
        #
        retD = {}
        logger.info("Get XREFs for PubChem compound ID list (%d)", len(pcidList))
        #
        try:
            xrefD = self.__getReferenceData(self.__databaseName, self.__refDataCollectionName, selectD=None, selectionList=["rcsb_id", "cid", "data.xrefs"])
            for pcid in pcidList:
                try:
                    xD = xrefD[pcid]["data"]["xrefs"]
                    if isinstance(xD, list):
                        xD = {}
                except Exception:
                    xD = {}
                #
                mD = {}
                logger.debug("%s (%s) xrefs %r", pcid, xrefD[pcid]["cid"], xD)
                for rNm, rIdL in xD.items():
                    mD[rNm] = rIdL
                retD[pcid] = mD
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return retD

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
                # if "_id" in obj:
                #    obj.pop("_id")
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
