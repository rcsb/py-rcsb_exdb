##
# File: ObjectUpdater.py
# Date: 9-Oct-2019  jdw
#
# Utilities to update document features from the document object server.
#
# Updates:
#
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil


logger = logging.getLogger(__name__)


class ObjectUpdater(object):
    """Utilities to update document features from the document object server."""

    def __init__(self, cfgOb, **kwargs):
        self.__cfgOb = cfgOb
        self.__resourceName = "MONGO_DB"
        _ = kwargs
        #

    def update(self, databaseName, collectionName, updateDL):
        """Update documents satisfying the selection details with the content of updateDL.

        Args:
            databaseName (str): Target database name
            collectionName (str): Target collection name
            updateDL = [{selectD: ..., updateD: ... }, ....]
                selectD    = {'ky1': 'val1', 'ky2': 'val2',  ...}
                updateD = {'key1.subkey1...': 'val1', 'key2.subkey2..': 'val2', ...}

        """
        try:
            numUpdated = 0
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.collectionExists(databaseName, collectionName):
                    logger.debug("%s %s document count is %d", databaseName, collectionName, mg.count(databaseName, collectionName))
                    for updateD in updateDL:
                        num = mg.update(databaseName, collectionName, updateD["updateD"], updateD["selectD"], upsertFlag=True)
                        numUpdated += num

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return numUpdated

    def count(self, databaseName, collectionName):
        try:
            numTotal = 0
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.collectionExists(databaseName, collectionName):
                    numTotal = mg.count(databaseName, collectionName)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return numTotal

    def createCollection(self, databaseName, collectionName, indexAttributeNames=None, indexName="primary", checkExists=False, bsonSchema=None):
        """Create collection and optionally set index attributes for the named index and validation schema for a new collection.

        Args:
            databaseName (str): target database name
            collectionName (str): target collection name
            indexAttributeNames (list, optional): list of attribute names for the 'primary' index. Defaults to None.
            checkExists (bool, optional): reuse an existing collection if True. Defaults to False.
            bsonSchema (object, optional): BSON compatable validation schema. Defaults to None.

        Returns:
            (bool): True for success or False otherwise
        """
        try:
            logger.debug("Create database %s collection %s", databaseName, collectionName)
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if checkExists and mg.databaseExists(databaseName) and mg.collectionExists(databaseName, collectionName):
                    ok1 = True
                else:
                    ok1 = mg.createCollection(databaseName, collectionName, bsonSchema=bsonSchema)
                ok2 = mg.databaseExists(databaseName)
                ok3 = mg.collectionExists(databaseName, collectionName)
                okI = True
                if indexAttributeNames:
                    okI = mg.createIndex(databaseName, collectionName, indexAttributeNames, indexName=indexName, indexType="DESCENDING", uniqueFlag=False)

            return ok1 and ok2 and ok3 and okI
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False

    def delete(self, databaseName, collectionName, selectD):
        """Remove documents satisfying the input selection details.

        Args:
            databaseName (str): Target database name
            collectionName (str): Target collection name
            selectD    = {'ky1': 'val1', 'ky2': 'val2',  ...}

        """
        try:
            numDeleted = 0
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.collectionExists(databaseName, collectionName):
                    logger.info("%s %s document count is %d", databaseName, collectionName, mg.count(databaseName, collectionName))
                    numDeleted = mg.delete(databaseName, collectionName, selectD)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return numDeleted
