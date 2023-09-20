##
#  File:           ChemRefMappingProvider.py
#  Date:           18-Jun-2021 jdw
#
#  Updated:
#
##
"""
Accessors for chemical reference identifier mapping data.
"""

import datetime
import logging
import os.path
import time

from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.io.StashableBase import StashableBase
from rcsb.exdb.chemref.ChemRefExtractor import ChemRefExtractor

logger = logging.getLogger(__name__)


class ChemRefMappingProvider(StashableBase):
    """Accessors for chemical reference identifier mapping data."""

    def __init__(self, cachePath, useCache=True):
        #
        self.__cachePath = cachePath
        self.__useCache = useCache
        self.__dirName = "chemref-mapping"
        super(ChemRefMappingProvider, self).__init__(self.__cachePath, [self.__dirName])
        self.__dirPath = os.path.join(self.__cachePath, self.__dirName)
        #
        self.__mU = MarshalUtil(workPath=self.__dirPath)
        self.__rD = {}
        self.__mapD = self.__reload(self.__dirPath, useCache)
        #

    def testCache(self, minCount=0):
        logger.info("Mapping count %d", len(self.__mapD["mapping"]) if "mapping" in self.__mapD else 0)
        if minCount == 0 or self.__mapD and "mapping" in self.__mapD and len(self.__mapD["mapping"]) >= minCount:
            return True
        else:
            return False

    def getReferenceIds(self, referenceResourceName, localId):
        """Get the identifiers in the reference resource corresponding to input local
        identifiers (Chemical Component or BIRD).

        Args:
            referenceResourceName (str): chemical reference resource name (DrugBank, ChEMBL, ChEBI, PubChem, ...)
            localId (str): local identifier for a Chemical Component or BIRD definition

        Returns:
            list: list of reference identifiers
        """
        if not self.__rD:
            for rN, forwardD in self.__mapD["mapping"].items():
                # {refId :[lId, lId, ...], ...}
                reverseD = {}
                for refId, rcsbIdL in forwardD.items():
                    for rId in rcsbIdL:
                        reverseD.setdefault(rId, []).append(refId)
                self.__rD[rN] = reverseD
        #
        try:
            return self.__rD[referenceResourceName.upper()][localId]
        except Exception:
            return []

    def getLocalIds(self, referenceResourceName, referenceId):
        """Get the local identifiers (Chemical Component or BIRD) corresponding to identifiers in
        chemical reference resource.

        Args:
            referenceResourceName (str): chemical reference resource name (DrugBank, ChEMBL, ChEBI, PubChem, ...)
            referenceId (str): identifier in the chemical reference resource

        Returns:
            list: list of local Chemical Component or BIRD identifiers
        """
        try:
            return self.__mapD["mapping"][referenceResourceName.upper()][referenceId]
        except Exception:
            return []

    def __getMappingDataPath(self):
        return os.path.join(self.__dirPath, "chemref-mapping-data.json")

    def reload(self):
        self.__mapD = self.__reload(self.__dirPath, useCache=True)
        return True

    def __reload(self, dirPath, useCache):
        startTime = time.time()
        fD = {}
        ok = False
        mappingPath = self.__getMappingDataPath()
        #
        logger.info("useCache %r mappingPath %r", useCache, mappingPath)
        if useCache and self.__mU.exists(mappingPath):
            fD = self.__mU.doImport(mappingPath, fmt="json")
            ok = True
        else:
            fU = FileUtil()
            fU.mkdir(dirPath)
        # ---
        logger.info("Completed reload with status (%r) at %s (%.4f seconds)", ok, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), time.time() - startTime)
        return fD

    def fetchChemRefMapping(self, cfgOb, referenceResourceNameList=None):
        """Fetch reference resource mapping for chemical component and BIRD definitions

        Args:
            cfgOb (obj): instance configuration class ConfigUtil()
            referenceResourceNameList (list, optional): list of chemical reference resources. Defaults to [DrugBank, ChEMBL].

        Returns:
            bool: True for success or False otherwise
        """
        try:
            rnL = referenceResourceNameList if referenceResourceNameList is not None else ["DrugBank", "ChEMBL"]
            mD = {}
            crExt = ChemRefExtractor(cfgOb)
            for referenceResourceName in rnL:
                idD = crExt.getChemCompAccessionMapping(referenceResourceName=referenceResourceName)
                logger.info("%s mapping dictionary (%d)", referenceResourceName, len(idD))
                mD[referenceResourceName.upper()] = idD
            #
            fp = self.__getMappingDataPath()
            tS = datetime.datetime.now().isoformat()
            vS = datetime.datetime.now().strftime("%Y-%m-%d")
            ok = self.__mU.doExport(fp, {"version": vS, "created": tS, "mapping": mD}, fmt="json", indent=3)
            return ok
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False
