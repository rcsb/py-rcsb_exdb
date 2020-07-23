##
# File: PubChemEtlWrapper.py
# Date: 19-Jul-2029  jdw
#
#
# Updates:
#
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from rcsb.exdb.chemref.PubChemDataCacheProvider import PubChemDataCacheProvider
from rcsb.exdb.chemref.PubChemIndexCacheProvider import PubChemIndexCacheProvider

logger = logging.getLogger(__name__)


class PubChemEtlWrapper(object):
    """ Workflow wrapper for updating chemical component/BIRD to PubChem mapping and related PubChem reference data.
    """

    def __init__(self, cfgOb, cachePath, stashDirPath, **kwargs):
        self.__cfgOb = cfgOb
        self.__cachePath = cachePath
        # remote path
        self.__stashDirPath = stashDirPath
        #
        # Optional configuration for stash services -
        self.__stashUrl = kwargs.get("stashUrl", None)
        self.__stashUserName = kwargs.get("stashUserName", None)
        self.__stashPassword = kwargs.get("stashPassword", None)
        self.__stashRemotePrefix = kwargs.get("stashRemotePrefix", None)
        #
        self.__dirPath = os.path.join(self.__cachePath, "PubChem")
        self.__pcicP = PubChemIndexCacheProvider(self.__cfgOb, self.__cachePath)
        self.__pcdcP = PubChemDataCacheProvider(self.__cfgOb, self.__cachePath)
        #

    def restoreIndex(self):
        """Restore PubChem mapping index data store from saved backup.

        Returns:
            (int): number of records in restored collection.
        """
        numRecords = self.__pcicP.restore()
        return numRecords

    def dumpIndex(self):
        """Dump PubChem mapping index reference data from the object store.

        Returns:
            (bool): True for success or False otherwise
        """
        ok = self.__pcicP.dump()
        return ok

    def toStashIndex(self):
        """Store PubChem mapping index reference data on the remote stash storage resource.

        Returns:
            (bool): True for success or False otherwise
        """
        return self.__pcicP.toStash(self.__stashUrl, self.__stashDirPath, userName=self.__stashUserName, password=self.__stashPassword, remoteStashPrefix=self.__stashRemotePrefix)

    def fromStashIndex(self):
        """Recover PubChem mapping index reference data from the remote stash storage resource.

        Returns:
            (bool): True for success or False otherwise
        """
        return self.__pcicP.fromStash(
            self.__stashUrl, self.__stashDirPath, userName=self.__stashUserName, password=self.__stashPassword, remoteStashPrefix=self.__stashRemotePrefix
        )

    def updateIndex(self, **kwargs):
        """ Search and store PubChem correspondences for CCD and BIRD reference chemical definitions.

        Args:
            ccUrlTarget (str, optional): target url for chemical component dictionary resource file (default: None=all public)
            birdUrlTarget (str, optional): target url for bird dictionary resource file (cc format) (default: None=all public)
            ccFileNamePrefix (str, optional): index file prefix (default: full)
            rebuildChemIndices (bool, optional): rebuild indices from source (default: False)
            fetchlLimit (int, optional): maximum number of definitions to process (default: None)
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
            dict, dict : mapD { ccId1: [{'pcId': ... , 'inchiKey': ... }], ccId2: ...},
                         altD { ccId1: [{'pcId': ... , 'inchiKey': ... 'sourceType': ... }], ccId2: ...}
        """
        sourceTypes = kwargs.get("sourceTypes", ["model-xyz"])
        mapD, extraMapD = self.__pcicP.getSelectedMatches(exportPath=self.__dirPath, sourceTypes=sourceTypes)
        logger.info("mapD (%d) extraMapD (%d) %r", len(mapD), len(extraMapD), extraMapD)
        return mapD, extraMapD

    def updateData(self, pcidList, doExport=False):
        """ Update PubChem reference data for the input list of compound identifiers.

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

    def updateMatchedData(self, doExport=False):
        """ Update PubChem reference data for any matched compound identifiers in the current index.

        Returns:
            (bool): True for success or False otherwise
        """
        ok = False
        try:
            pcidList = self.getMatches()
            exportPath = self.__dirPath if doExport else None
            ok, failList = self.__pcdcP.updateMissing(pcidList, exportPath=exportPath)
            if failList:
                logger.info("No data updated for %r", failList)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def restoreData(self):
        """Restore PubChem reference data store from saved backup.

        Returns:
            (int): number of objects restored.
        """
        numRecords = self.__pcdcP.restore()
        return numRecords

    def dumpData(self):
        """Backup PubChem reference data from the object store.

        Returns:
            (bool): True for success or False otherwise
        """
        ok = self.__pcdcP.dump()
        return ok

    def toStashData(self):
        """Store PubChem reference data on the remote stash storage resource.

        Returns:
            (bool): True for success or False otherwise
        """
        return self.__pcdcP.toStash(self.__stashUrl, self.__stashDirPath, userName=self.__stashUserName, password=self.__stashPassword, remoteStashPrefix=self.__stashRemotePrefix)

    def fromStashData(self):
        """Recover PubChem reference data from the remote stash storage resource.

        Returns:
            (bool): True for success or False otherwise
        """
        return self.__pcdcP.fromStash(
            self.__stashUrl, self.__stashDirPath, userName=self.__stashUserName, password=self.__stashPassword, remoteStashPrefix=self.__stashRemotePrefix
        )

    def getPubChemRelatedIdentifiers(self, pcidList):
        """Return related identifiers (xrefs) for the input PubChem compound identifier list.

        Args:
            pcidList (list): PubChem compound identifier list

        Returns:
            dict :{<pcid>: {'relatedId1': ... 'relatedId2': ... }, ...}

        """
        rD = self.__pcdcP.getRelatedMapping(pcidList)
        logger.info("Related identifier map length (%d)", len(rD))
        return rD

    def getRelatedIdentifiers(self, **kwargs):
        """Return PubChem assigned related identifiers for matching compounds for the input chemical component sourceTypes.

        Args:
            sourceTypes (list, optional):  list of source chemical component build types (default: ["model-xyz"])

        Returns:
            dict: riD { ccId1: [{'pcId': ... , 'inchiKey': ... 'ChEBI': ... 'ChEMBL': ... 'CAS': ... }], ccId2: ...},

        """
        sourceTypes = kwargs.get("sourceTypes", ["model-xyz"])
        mapD, _ = self.getSelectedMatches(sourceTypes=sourceTypes)
        pcIdList = []
        # mapD { ccId1: [{'pcId': ... , 'inchiKey': ... }],
        for mDL in mapD.values():
            pcIdList.extend([mD["pcId"] for mD in mDL])
        logger.info("pcIdList (%d)", len(pcIdList))
        rD = self.getPubChemRelatedIdentifiers(pcIdList)
        #
        #  Update the identifier mappings
        for _, mDL in mapD.items():
            for mD in mDL:
                pcId = mD["pcId"]
                if pcId in rD:
                    for rIdName, rIdValue in rD[pcId].items():
                        mD[rIdName] = rIdValue
        #
        return mapD
