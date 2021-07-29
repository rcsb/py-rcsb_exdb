##
# File: PubChemEtlWrapper.py
# Date: 19-Jul-2029  jdw
#
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
import os

from rcsb.exdb.chemref.PubChemDataCacheProvider import PubChemDataCacheProvider
from rcsb.exdb.chemref.PubChemIndexCacheProvider import PubChemIndexCacheProvider
from rcsb.utils.chemref.PubChemProvider import PubChemProvider

logger = logging.getLogger(__name__)


class PubChemEtlWrapper(object):
    """Workflow wrapper for updating chemical component/BIRD to PubChem mapping and related PubChem reference data."""

    def __init__(self, cfgOb, cachePath, **kwargs):
        self.__cfgOb = cfgOb
        self.__configName = self.__cfgOb.getDefaultSectionName()
        self.__cachePath = cachePath
        self.__dirPath = os.path.join(self.__cachePath, "PubChem")
        #
        self.__stashRemotePrefix = kwargs.get("stashRemotePrefix", None)
        #
        self.__pcicP = PubChemIndexCacheProvider(self.__cfgOb, self.__cachePath)
        self.__pcdcP = PubChemDataCacheProvider(self.__cfgOb, self.__cachePath)
        self.__pcP = PubChemProvider(cachePath=self.__cachePath)
        #
        self.__identifierD = None
        #

    def reloadDump(self, contentType="index"):
        """Reload the input content type in the data store from saved object store dump.

        Args:
            contentType (str): target content to restore (data|index)

        Returns:
            (int): number of records in restored collection.
        """
        numRecords = 0
        if contentType.lower() == "index":
            numRecords = self.__pcicP.reloadDump()
        elif contentType.lower() == "data":
            numRecords = self.__pcdcP.reloadDump()
        return numRecords

    def dump(self, contentType):
        """Dump PubChem content from the object store.

        Args:
            contentType (str): target content to restore (data|index)

        Returns:
            (bool): True for success or False otherwise
        """
        ok = False
        if contentType.lower() == "index":
            ok = self.__pcicP.dump()
        elif contentType.lower() == "data":
            ok = self.__pcdcP.dump()
        elif contentType.lower() == "identifiers":
            ok = self.__dumpIdentifiers()

        return ok

    def toStash(self, contentType, useGit=True, useStash=True):
        """Store PubChem extracted content () on the remote stash storage resource.

        Args:
            contentType (str): target content to stash (data|index|identifiers)
        Returns:
            (bool): True for success or False otherwise
        """
        if contentType.lower() == "index":
            return self.__pcicP.backup(self.__cfgOb, self.__configName, remotePrefix=self.__stashRemotePrefix, useGit=useGit, useStash=useStash)
        elif contentType.lower() == "data":
            return self.__pcdcP.backup(self.__cfgOb, self.__configName, remotePrefix=self.__stashRemotePrefix, useGit=useGit, useStash=useStash)
        elif contentType.lower() == "identifiers":
            return self.__pcP.backup(self.__cfgOb, self.__configName, remotePrefix=self.__stashRemotePrefix, useGit=useGit, useStash=useStash)
        return False

    def fromStash(self, contentType, useStash=True, useGit=True):
        """Fetch PubChem extracted content from the remote stash storage resource.

        Args:
            contentType (str): target content to fetch (data|index)
        Returns:
            (bool): True for success or False otherwise
        """
        if contentType.lower() == "index":
            return self.__pcicP.restore(self.__cfgOb, self.__configName, remotePrefix=self.__stashRemotePrefix, useGit=useGit, useStash=useStash)
        elif contentType.lower() == "data":
            return self.__pcdcP.restore(self.__cfgOb, self.__configName, remotePrefix=self.__stashRemotePrefix, useGit=useGit, useStash=useStash)
        elif contentType.lower() == "identifiers":
            return self.__pcdcP.restore(self.__cfgOb, self.__configName, remotePrefix=self.__stashRemotePrefix, useGit=useGit, useStash=useStash)
        return False

    def updateIndex(self, **kwargs):
        """Search and store PubChem correspondences for CCD and BIRD reference chemical definitions.

        Args:
            ccUrlTarget (str, optional): target url for chemical component dictionary resource file (default: None=all public)
            birdUrlTarget (str, optional): target url for bird dictionary resource file (cc format) (default: None=all public)
            ccFileNamePrefix (str, optional): index file prefix (default: full)
            rebuildChemIndices (bool, optional): rebuild indices from source (default: False)
            fetchLimit (int, optional): maximum number of definitions to process (default: None)
            exportPath(str, optional): path to export raw PubChem search results  (default: None)
            numProc(int):  number processors to include in multiprocessing mode (default: 12)

            Returns:
                (bool): True for success or False otherwise
        """
        ok = False
        try:
            rebuildChemIndices = kwargs.get("rebuildChemIndices", False)
            ccUrlTarget = kwargs.get("ccUrlTarget", None)
            birdUrlTarget = kwargs.get("birdUrlTarget", None)
            ccFileNamePrefix = kwargs.get("ccFileNamePrefix", "full")
            fetchLimit = kwargs.get("fetchLimit", None)
            exportPath = kwargs.get("exportPath", None)
            expireDays = kwargs.get("expireDays", 0)
            numProc = kwargs.get("numProc", 12)

            #  -- Update/create mapping index cache  ---
            ok = self.__pcicP.updateMissing(
                expireDays=expireDays,
                cachePath=self.__cachePath,
                ccUrlTarget=ccUrlTarget,
                birdUrlTarget=birdUrlTarget,
                ccFileNamePrefix=ccFileNamePrefix,
                exportPath=exportPath,
                rebuildChemIndices=rebuildChemIndices,
                fetchLimit=fetchLimit,
                numProc=numProc,
            )
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def getMatches(self):
        """Return a list of matched PubChem compound identifiers.

        Returns:
            (list, str): list of PubChem compound identifiers
        """
        return self.__pcicP.getMatches()

    def getSelectedMatches(self, **kwargs):
        """
            Return preferred PubChem correspondences from the current match index for the input source
            component build type. Separately return alternative matches for other source types.

        Args:
            sourceTypes (list, optional):  list of source chemical component build types (default: ["model-xyz"])

        Returns:
            (dict, dict): mapD { ccId1: [{'pcId': ... , 'inchiKey': ... }], ccId2: ...},
                         altD { ccId1: [{'pcId': ... , 'inchiKey': ... 'sourceType': ... }], ccId2: ...}
        """
        sourceTypes = kwargs.get("sourceTypes", ["model-xyz"])
        mapD, extraMapD = self.__pcicP.getSelectedMatches(exportPath=self.__dirPath, sourceTypes=sourceTypes)
        logger.debug("mapD (%d) extraMapD (%d) %r", len(mapD), len(extraMapD), extraMapD)
        return mapD, extraMapD

    def updateData(self, pcidList, doExport=False):
        """Update PubChem reference data for the input list of compound identifiers.

        Args:
            pcidList (list,str): PubChem compound identifiers

        Returns:
            (bool): True for success or False otherwise
        """
        ok = False
        try:
            exportPath = self.__dirPath if doExport else None
            ok, failList = self.__pcdcP.updateMissing(pcidList, exportPath=exportPath)
            if failList:
                logger.info("No data updated for %r", failList)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def updateMatchedData(self, exportRaw=False):
        """Update PubChem reference data using matched compound identifiers in the current index.

        Returns:
            (bool): True for success or False otherwise
        """
        ok = False
        try:
            pcidList = self.getMatches()
            exportPath = self.__dirPath if exportRaw else None
            ok, failList = self.__pcdcP.updateMissing(pcidList, exportPath=exportPath)
            if failList:
                logger.info("No data updated for %r", failList)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def __getPubChemIdentifiers(self, pcidList):
        """Return related identifiers (xrefs) for the input PubChem compound identifier list.

        Args:
            pcidList (list): PubChem compound identifier list

        Returns:
            (dict) :{<pcid>: {'relatedId1': ... 'relatedId2': ... }, ...}

        """
        rD = self.__pcdcP.getRelatedMapping(pcidList)
        logger.info("Related identifier map length (%d)", len(rD))
        return rD

    def updateIdentifiers(self, **kwargs):
        """Update PubChem assigned related identifiers for matching compounds for the input chemical component sourceTypes.

        Args:
            sourceTypes (list, optional):  list of source chemical component build types (default: ["model-xyz"])

        Returns:
            (bool): True for success or False otherwise
        """
        ok = False
        try:
            sourceTypes = kwargs.get("sourceTypes", ["model-xyz"])
            mapD, _ = self.getSelectedMatches(sourceTypes=sourceTypes)
            pcIdList = []
            # mapD { ccId1: [{'pcId': ... , 'inchiKey': ... }],
            for mDL in mapD.values():
                pcIdList.extend([mD["pcId"] for mD in mDL])
            logger.info("pcIdList (%d)", len(pcIdList))
            rD = self.__getPubChemIdentifiers(pcIdList)
            #
            #  Update the identifier mappings
            for _, mDL in mapD.items():
                for mD in mDL:
                    pcId = mD["pcId"]
                    if pcId in rD:
                        for rIdName, rIdValue in rD[pcId].items():
                            mD[rIdName] = rIdValue
            #
            self.__identifierD = mapD
            ok = self.__identifierD is not None
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def getIdentifiers(self, **kwargs):
        """Get PubChem assigned related identifiers for matching compounds for the input chemical component sourceTypes.

        Returns:
            dict: riD { ccId1: [{'pcId': ... , 'inchiKey': ... 'ChEBI': ... 'ChEMBL': ... 'CAS': ... }], ccId2: ...},

        """
        if not self.__identifierD:
            self.updateIdentifiers(**kwargs)
        return self.__identifierD

    def __dumpIdentifiers(self):
        rD = self.getIdentifiers()
        ok = self.__pcP.load(rD, "identifiers", fmt="json")
        return ok
