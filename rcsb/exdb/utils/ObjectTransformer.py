##
# File: ObjectTransformer.py
# Date: 17-Oct-2019  jdw
#
# Utilities to extract and update object from the document object server.
#
# Updates:
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil
from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.db.utils.TimeUtil import TimeUtil

logger = logging.getLogger(__name__)


class ObjectTransformer(object):
    """Utilities to extract and update object from the document object server."""

    def __init__(self, cfgOb, objectAdapter=None, **kwargs):
        self.__cfgOb = cfgOb
        self.__oAdapt = objectAdapter
        self.__resourceName = "MONGO_DB"
        _ = kwargs
        self.__statusList = []

    def doTransform(self, **kwargs):
        desp = DataExchangeStatus()
        statusStartTimestamp = desp.setStartTime()
        #
        databaseName = kwargs.get("databaseName", "pdbx_core")
        collectionName = kwargs.get("collectionName", "pdbx_core_entry")
        selectionQueryD = kwargs.get("selectionQuery", {})
        fetchLimit = kwargs.get("fetchLimit", None)
        tU = TimeUtil()
        updateId = kwargs.get("updateId", tU.getCurrentWeekSignature())
        #
        docSelectList = self.__selectObjectIds(databaseName, collectionName, selectionQueryD)
        docSelectList = docSelectList[:fetchLimit] if fetchLimit else docSelectList
        ok = self.__transform(databaseName, collectionName, docSelectList)
        #
        if updateId:
            okS = self.__updateStatus(updateId, databaseName, collectionName, ok, statusStartTimestamp)
        return ok and okS

    def __selectObjectIds(self, databaseName, collectionName, selectionQueryD):
        """Return a list of object identifiers for the input selection query."""
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.collectionExists(databaseName, collectionName):
                    logger.info("%s %s document count is %d", databaseName, collectionName, mg.count(databaseName, collectionName))
                    qD = {}
                    if selectionQueryD:
                        qD.update(selectionQueryD)
                    selectL = ["_id"]
                    dL = mg.fetch(databaseName, collectionName, selectL, queryD=qD)
                    logger.info("Selection %r fetch result count %d", selectL, len(dL))

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return dL
        #

    def __transform(self, databaseName, collectionName, docSelectList, logIncrement=10000):
        """Return a list of object identifiers for the input selection query."""
        #
        ok = True
        try:
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.collectionExists(databaseName, collectionName):
                    numDoc = len(docSelectList)
                    for ii, dD in enumerate(docSelectList, 1):
                        if "_id" not in dD:
                            continue
                        rObj = mg.fetchOne(databaseName, collectionName, "_id", dD["_id"])
                        del rObj["_id"]
                        #
                        fOk = True
                        if self.__oAdapt:
                            fOk, rObj = self.__oAdapt.filter(rObj)
                        if fOk:
                            rOk = mg.replace(databaseName, collectionName, rObj, dD)
                            if rOk is None:
                                tId = rObj["rcsb_id"] if "rcsb_id" in rObj else "anonymous"
                                logger.error("%r %r (%r) failing", databaseName, collectionName, tId)
                                logger.debug("rObj.keys() %r", list(rObj.keys()))
                                logger.debug("rObj.items() %s", rObj.items())
                                rOk = False
                            ok = ok and rOk
                        #
                        if ii % logIncrement == 0 or ii == numDoc:
                            logger.info("Replace status %r object (%d of %d)", ok, ii, numDoc)
                        #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def getLoadStatus(self):
        return self.__statusList

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
