##
# File: ObjectExtractor.py
# Date: 26-Jun-2019  jdw
#
# Utilities to extract document features from the document object server.
#
# Updates:
# 27-Jun-2019  jdw add JSON path tracking utilities.
# 15-Sep-2026  dwp Retrieve document selections in batches (MongoDbUtil.fetchBatched) and build the
#                  object dictionary incrementally; distinguish an empty result set from a failed
#                  fetch and propagate the latter to the caller (see 'raiseOnFailure')
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import copy
import logging
import os

from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil


logger = logging.getLogger(__name__)


class ObjectExtractor(object):
    """Utilities to extract document features from the document object server."""

    def __init__(self, cfgOb, **kwargs):
        """Extract document selections from the document object server.

        Args:
            cfgOb (obj): instance of ConfigUtil()
            fetchBatchSize (int, optional): documents retrieved per server round trip. 0 disables
                batching and restores the legacy single-cursor behavior. Defaults to the
                MongoDbUtil default (DEFAULT_FETCH_BATCH_SIZE).
            fetchSortKey (str, optional): indexed attribute used to page the result set. Defaults to "_id".
            raiseOnFailure (bool, optional): re-raise on a failed selection rather than silently
                returning an empty/partial result. Defaults to True. Note that a query which simply
                matches no documents is NOT a failure - it returns an empty result with status True.
        """
        self.__cfgOb = cfgOb
        self.__resourceName = "MONGO_DB"
        self.__mU = MarshalUtil()
        #
        self.__statusOk = False
        self.__selectionOk = True
        self.__objectD = self.__rebuildCache(**kwargs)
        self.__objPathD = {}
        self.__stringPathList = []
        self.__objValD = {}
        #

    def getStatus(self):
        """Return True if the selection completed successfully (including the case of a
        successful selection that matched no documents), or False otherwise.
        """
        return self.__statusOk

    def getObjects(self):
        return self.__objectD

    def getPathList(self, filterList=True):
        kL = []
        if filterList:
            tL = []
            for ky in self.__objPathD:
                if ky and (ky.find(".") != -1 or ky.startswith("_")) and ky not in ["_id"] and not ky.endswith("[]"):
                    tL.append(ky)
            for ky in tL:
                for tky in tL:
                    ok = True
                    if ky in tky and ky != tky:
                        ok = False
                        break
                if ok:
                    kL.append(ky)
        else:
            kL = list(self.__objPathD.keys())
        #
        return sorted(kL)

    def getValues(self):
        return self.__objValD

    def setPathList(self, stringPathList):
        self.__objPathD = {k: True for k in stringPathList}
        return True

    def getCount(self):
        return len(self.__objectD)

    def __rebuildCache(self, **kwargs):
        cacheFilePath = kwargs.get("cacheFilePath", None)
        cacheKwargs = kwargs.get("cacheKwargs", {"fmt": "pickle"})
        useCache = kwargs.get("useCache", True)
        keyAttribute = kwargs.get("keyAttribute", "entry")
        selectL = kwargs.get("selectionList", [])
        raiseOnFailure = kwargs.get("raiseOnFailure", True)
        #
        cD = {keyAttribute: {}}
        try:
            if useCache and cacheFilePath and os.access(cacheFilePath, os.R_OK):
                cD = self.__mU.doImport(cacheFilePath, **cacheKwargs)
            else:
                if selectL:
                    objectD = self.__select(**kwargs)
                else:
                    objectD = self.__selectObjects(**kwargs)
                cD[keyAttribute] = objectD
                if cacheFilePath:
                    pth, _ = os.path.split(cacheFilePath)
                    ok = self.__mU.mkdir(pth)
                    ok = self.__mU.doExport(cacheFilePath, cD, **cacheKwargs)
                    logger.info("Saved object results (%d) status %r in %s", len(objectD), ok, cacheFilePath)
            self.__statusOk = self.__selectionOk
        except Exception as e:
            # A failed selection must not be silently reported as an empty one - downstream
            # consumers would export empty/truncated resource files from a transient Mongo error.
            logger.exception("Failing with %s", str(e))
            self.__statusOk = False
            if raiseOnFailure:
                raise
        return cD[keyAttribute]

    def __selectObjects(self, **kwargs):
        """Return a dictionary of objects satisfying the input conditions (e.g. method, resolution limit)"""
        databaseName = kwargs.get("databaseName", "pdbx_core")
        collectionName = kwargs.get("collectionName", "pdbx_core_entry")
        selectionQueryD = kwargs.get("selectionQuery", {})
        #
        uniqueAttributes = kwargs.get("uniqueAttributes", ["rcsb_id"])
        #
        tV = kwargs.get("objectLimit", None)
        objLimit = int(tV) if tV is not None else None
        stripObjectId = kwargs.get("stripObjectId", False)
        logIncrement = kwargs.get("logIncrement", 10000)
        #
        fetchBatchSize = kwargs.get("fetchBatchSize", None)
        fetchSortKey = kwargs.get("fetchSortKey", "_id")
        #
        objectD = {}
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            mg = MongoDbUtil(client)
            if not mg.collectionExists(databaseName, collectionName):
                # Not raised - some collections are legitimately absent before their first load -
                # but the selection is flagged so callers can tell this from an empty result set.
                logger.error("Collection %s %s does not exist", databaseName, collectionName)
                self.__selectionOk = False
                return objectD
            logger.info("%s %s estimated document count is %d", databaseName, collectionName, mg.estimatedCount(databaseName, collectionName))
            qD = {}
            if selectionQueryD:
                qD.update(selectionQueryD)
            selectL = ["_id"]
            #
            ii = 0
            done = False
            for dL in mg.fetchBatched(databaseName, collectionName, selectL, queryD=qD, batchSize=fetchBatchSize, sortKey=fetchSortKey):
                for dD in dL:
                    ii += 1
                    if "_id" not in dD:
                        continue
                    rObj = mg.fetchOne(databaseName, collectionName, "_id", dD["_id"])
                    if stripObjectId and rObj and "_id" in rObj:
                        rObj.pop("_id")
                    else:
                        rObj["_id"] = str(rObj["_id"])
                    #
                    stKey = ".".join([rObj[ky] for ky in uniqueAttributes])
                    objectD[stKey] = copy.copy(rObj)
                    if objLimit and ii >= objLimit:
                        done = True
                        break
                    logger.debug("Saving %d %s", ii, stKey)
                    if ii % logIncrement == 0:
                        logger.info("Extracting object (%d)", ii)
                if done:
                    break
            logger.info("Selection %r fetch result count %d", selectL, ii)
        return objectD
        #

    def __select(self, **kwargs):
        """Return a dictionary of object content satisfying the input conditions
        (e.g. method, resolution limit) and selection options.
        """
        databaseName = kwargs.get("databaseName", "pdbx_core")
        collectionName = kwargs.get("collectionName", "pdbx_core_entry")
        selectionQueryD = kwargs.get("selectionQuery", {})
        uniqueAttributes = kwargs.get("uniqueAttributes", ["rcsb_id"])
        selectL = kwargs.get("selectionList", [])
        stripObjectId = kwargs.get("stripObjectId", False)
        #
        tV = kwargs.get("objectLimit", None)
        objLimit = int(tV) if tV is not None else None
        #
        fetchBatchSize = kwargs.get("fetchBatchSize", None)
        fetchSortKey = kwargs.get("fetchSortKey", "_id")
        #
        objectD = {}
        with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
            mg = MongoDbUtil(client)
            if not mg.collectionExists(databaseName, collectionName):
                # Not raised - some collections are legitimately absent before their first load -
                # but the selection is flagged so callers can tell this from an empty result set.
                logger.error("Collection %s %s does not exist", databaseName, collectionName)
                self.__selectionOk = False
                return objectD
            logger.info("%s %s estimated document count is %d", databaseName, collectionName, mg.estimatedCount(databaseName, collectionName))
            qD = {}
            if selectionQueryD:
                qD.update(selectionQueryD)
            #
            # Documents are consumed batch-by-batch so that peak memory tracks the batch size
            # rather than the size of the full result set. Any failure to retrieve a batch is
            # raised here (after the retries applied by fetchBatched) rather than being
            # reported as an empty selection.
            #
            ii = 0
            done = False
            for dL in mg.fetchBatched(databaseName, collectionName, selectL, queryD=qD, suppressId=True, batchSize=fetchBatchSize, sortKey=fetchSortKey):
                for rObj in dL:
                    ii += 1
                    stKey = ".".join([rObj[ky] for ky in uniqueAttributes])
                    if stripObjectId and rObj and "_id" in rObj:
                        rObj.pop("_id")
                    objectD[stKey] = copy.copy(rObj)
                    if objLimit and ii >= objLimit:
                        done = True
                        break
                logger.debug("Selection %r running fetch count %d", selectL, ii)
                if done:
                    break
            logger.info("Selection %r fetch result count %d", selectL, ii)
        return objectD
        #

    def __getKeyValues(self, dct, keyNames):
        """Return the tuple of values of corresponding to the input dictionary key names expressed in dot notation.

        Args:
            dct (dict): source dictionary object (nested)
            keyNames (list): list of dictionary keys in dot notation

        Returns:
            tuple: tuple of values corresponding to the input key names

        """
        rL = []
        try:
            for keyName in keyNames:
                rL.append(self.__getKeyValue(dct, keyName))
        except Exception as e:
            logger.exception("Failing for key names %r with %s", keyNames, str(e))

        return tuple(rL)

    def __getKeyValue(self, dct, keyName):
        """Return the value of the corresponding key expressed in dot notation in the input dictionary object (nested)."""
        try:
            kys = keyName.split(".")
            for key in kys:
                try:
                    dct = dct[key]
                except KeyError:
                    return None
            return dct
        except Exception as e:
            logger.exception("Failing for key %r with %s", keyName, str(e))

        return None

    def __toJsonPathString(self, path):
        pL = [ky if ky else "[]" for ky in path]
        sp = ".".join(pL)
        sp = sp.replace(".[", "[")
        return sp

    def __pathCallBack(self, path, value):
        sp = self.__toJsonPathString(path)
        self.__objPathD[sp] = self.__objPathD[sp] + 1 if sp in self.__objPathD else 1
        return value

    def __saveCallBack(self, path, value):
        sP = self.__toJsonPathString(path)
        if sP in self.__objPathD:
            ky = sP.replace("[]", "")
            if sP.find("[") != -1:  # multivalued
                if isinstance(value, list):
                    self.__objValD.setdefault(ky, []).extend(value)
                else:
                    self.__objValD.setdefault(ky, []).append(value)
            else:
                self.__objValD[ky] = value
        return value

    def genPathList(self, dObj, path=None):
        return self.__walk(dObj, jsonPath=path, funct=self.__pathCallBack)

    def genValueList(self, dObj, path=None, clear=True):
        self.__objValD = {} if clear else self.__objValD
        return self.__walk(dObj, jsonPath=path, funct=self.__saveCallBack)

    def __walk(self, jsonObj, jsonPath=None, funct=None):
        """Walk JSON data types. An optional funct() is called to mutate
        the value of each element. The jsonPath is updated at each element.
        """
        if jsonPath is None:
            jsonPath = []

        if isinstance(jsonObj, dict):
            value = {k: self.__walk(v, jsonPath + [k], funct) for k, v in jsonObj.items()}
        elif isinstance(jsonObj, list):
            value = [self.__walk(elem, jsonPath + [[]], funct) for elem in jsonObj]
        else:
            value = jsonObj

        if funct is None:
            return value
        else:
            return funct(jsonPath, value)

    def __toPath(self, path):
        """Convert path strings into path lists."""
        if isinstance(path, list):
            return path  # already in list format

        def _iterPath(path):
            for parts in path.split("[]"):
                for part in parts.strip(".").split("."):
                    yield part
                yield []

        return list(_iterPath(path))[:-1]
