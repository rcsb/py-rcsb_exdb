##
#  File:           GlycanProvider.py
#  Date:           24-May-2021 jdw
#
#  Updated:
#
##
"""
Accessors for glycan mapped annotations.

"""

import logging
import os.path
import time

from rcsb.exdb.branch.GlycanUtils import GlycanUtils
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.io.StashableBase import StashableBase

logger = logging.getLogger(__name__)


class GlycanProvider(StashableBase):
    """Accessors and generators for entity glycan mapped identifiers.

    dirPath -> CACHE/glycan/
                             branched_entity_glycan_identifier_map.json
                             accession-wurcs-mapping.json
                     stash/entity_glycan_mapped_identifiers.tar.gz

    """

    def __init__(self, **kwargs):
        #
        self.__version = "0.50"
        cachePath = kwargs.get("cachePath", ".")
        useCache = kwargs.get("useCache", True)
        self.__dirName = "glycan"
        self.__dirPath = os.path.join(cachePath, self.__dirName)
        super(GlycanProvider, self).__init__(cachePath, [self.__dirName])
        #
        self.__mU = MarshalUtil(workPath=self.__dirPath)
        self.__glyD = self.__reload(fmt="json", useCache=useCache)
        #

    def testCache(self, minCount=1):
        if minCount == 0:
            return True
        if self.__glyD and minCount and ("identifiers" in self.__glyD) and len(self.__glyD["identifiers"]) >= minCount:
            logger.info("Glycan identifiers (%d)", len(self.__glyD["identifiers"]))
            return True
        return False

    def getIdentifiers(self):
        """Return a dictionary of related identifiers organized by branched entity id.

        Returns:
            (dict): {entityId: {'idType1': ids, 'idType1': ids}, ... }
        """
        try:
            return self.__glyD["identifiers"] if self.__glyD["identifiers"] else {}
        except Exception as e:
            logger.error("Failing with %r", str(e))
        return {}

    def __getMappingFilePath(self, fmt="json"):
        baseFileName = "branched_entity_glycan_identifier_map"
        fExt = ".json" if fmt == "json" else ".pic"
        fp = os.path.join(self.__dirPath, baseFileName + fExt)
        return fp

    def update(self, cfgOb, fmt="json", indent=3):
        """Update branched entity glycan accession mapping cache.

        Args:
            cfgObj (object): ConfigInfo() object instance

        Returns:
            (bool): True for success for False otherwise
        """
        ok = False
        try:
            gU = GlycanUtils(cfgOb, self.__dirPath)
            eaD = gU.updateEntityAccessionMap()
            logger.info("Got branched entity glycan accession map (%d)", len(eaD))
            #
            tS = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
            self.__glyD = {"version": self.__version, "created": tS, "identifiers": eaD}
            #
            mappingFilePath = self.__getMappingFilePath(fmt=fmt)
            kwargs = {"indent": indent} if fmt == "json" else {}
            ok = self.__mU.doExport(mappingFilePath, self.__glyD, fmt=fmt, **kwargs)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def reload(self):
        """Reload from the current cache file."""
        ok = False
        try:
            self.__glyD = self.__reload(fmt="json", useCache=True)
            ok = self.__glyD is not None
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def __reload(self, fmt="json", useCache=True):
        mappingFilePath = self.__getMappingFilePath(fmt=fmt)
        tS = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        pcD = {"version": self.__version, "created": tS, "identifiers": {}}

        if useCache and self.__mU.exists(mappingFilePath):
            logger.info("reading cached path %r", mappingFilePath)
            pcD = self.__mU.doImport(mappingFilePath, fmt=fmt)
        return pcD
