##
#  File:           LigandNeighborMappingProvider.py
#  Date:           28-Jun-2021 jdw
#
#  Updated:
#
##
"""
Accessors for essential ligand neighbor mapping details associated with polymer and branched
entity instances.
"""

import datetime
import logging
import os.path
import time

from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.io.StashableBase import StashableBase
from rcsb.exdb.seq.LigandNeighborMappingExtractor import LigandNeighborMappingExtractor

logger = logging.getLogger(__name__)


class LigandNeighborMappingProvider(StashableBase):
    """Accessors for essential ligand neighbor mapping details associated with polymer and branched
    entity instances."""

    def __init__(self, cachePath, useCache=True):
        #
        self.__cachePath = cachePath
        self.__useCache = useCache
        self.__dirName = "ligand-neighbor-mapping"
        super(LigandNeighborMappingProvider, self).__init__(self.__cachePath, [self.__dirName])
        self.__dirPath = os.path.join(self.__cachePath, self.__dirName)
        #
        self.__mU = MarshalUtil(workPath=self.__dirPath)
        self.__mapD = self.__reload(self.__dirPath, useCache)
        #

    def testCache(self, minCount=0):
        logger.info("Cached ligand neighbor mapping count %d", len(self.__mapD["mapping"]) if "mapping" in self.__mapD else 0)
        if minCount == 0 or self.__mapD and "mapping" in self.__mapD and len(self.__mapD["mapping"]) >= minCount:
            return True
        else:
            return False

    def getLigandNeighbors(self, rcsbEntityId):
        """Get the unique list of ligand neighbors for the input polymer or branched entity instance.

        Args:
            rcsbEntityId (str): entryId '_' entityId

        Returns:
            list: [chem_comp_id, ... ]
        """
        try:
            return list(set([t[0] for t in self.__mapD["mapping"][rcsbEntityId.upper()]]))
        except Exception:
            return []

    def reload(self):
        self.__mapD = self.__reload(self.__dirPath, useCache=True)

    def __reload(self, dirPath, useCache):
        startTime = time.time()
        retD = {}
        ok = False
        mappingPath = self.__getMappingDataPath()
        #
        logger.info("useCache %r mappingPath %r", useCache, mappingPath)
        if useCache and self.__mU.exists(mappingPath):
            retD = self.__mU.doImport(mappingPath, fmt="json")
            ok = True
        else:
            fU = FileUtil()
            fU.mkdir(dirPath)
        # ---
        num = len(retD["mapping"]) if "mapping" in retD else 0
        logger.info("Completed ligand mapping reload (%d) with status (%r) at %s (%.4f seconds)", num, ok, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), time.time() - startTime)
        return retD

    def __getMappingDataPath(self):
        return os.path.join(self.__dirPath, "ligand-neighbor-mapping-data.json")

    def fetchLigandNeighborMapping(self, cfgOb):
        """Fetch ligand neighbor mapping details

        Args:
            cfgOb (obj): instance configuration class ConfigUtil()

        Returns:
            bool: True for success or False otherwise
        """
        try:
            lnmEx = LigandNeighborMappingExtractor(cfgOb)
            lnD = lnmEx.getLigandNeighbors()
            fp = self.__getMappingDataPath()
            tS = datetime.datetime.now().isoformat()
            vS = datetime.datetime.now().strftime("%Y-%m-%d")
            ok = self.__mU.doExport(fp, {"version": vS, "created": tS, "mapping": lnD}, fmt="json", indent=3)
            return ok
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False
