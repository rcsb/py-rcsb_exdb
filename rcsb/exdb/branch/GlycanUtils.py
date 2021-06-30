##
#  File:           GlycanUtils.py
#  Date:           24-May-2021 jdw
#
#  Updated:
##
"""
Utilities for fetching and mapping glycan accessions.
"""

import logging
import os.path

from rcsb.exdb.branch.BranchedEntityExtractor import BranchedEntityExtractor
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.io.UrlRequestUtil import UrlRequestUtil

logger = logging.getLogger(__name__)


class GlycanUtils:
    """Utilities for fetching and mapping glycan annotations."""

    def __init__(self, cfgOb, dirPath):
        self.__cfgOb = cfgOb
        self.__dirPath = dirPath
        self.__mU = MarshalUtil(workPath=self.__dirPath)
        #

    def __getRawGlycanDetailsPath(self):
        return os.path.join(self.__dirPath, "pdb-raw-branched-entity-details.json")

    def getBranchedEntityDetails(self):
        """For branched entities, get BIRD mapping and WURCS details"""
        ok = False
        try:
            bEx = BranchedEntityExtractor(self.__cfgOb)
            branchedEntityD = bEx.getBranchedDetails()
            logger.info("Branched entity descriptor details count %d", len(branchedEntityD))
            detailsPath = self.__getRawGlycanDetailsPath()
            ok = bEx.exportBranchedEntityDetails(detailsPath, fmt="json")
            logger.info("Store raw branched entity data (%r) %s", ok, detailsPath)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return branchedEntityD

    def __getGlycanAccessionMapPath(self):
        return os.path.join(self.__dirPath, "accession-wurcs-mapping.json")

    def fetchGlycanAccessionMap(self):
        mapD = {}
        accessionMapPath = self.__getGlycanAccessionMapPath()
        if self.__mU.exists(accessionMapPath):
            mapD = self.__mU.doImport(accessionMapPath, fmt="json")
        return mapD

    def storeGlycanAccessionMap(self, mapD):
        accessionMapPath = self.__getGlycanAccessionMapPath()
        ok = self.__mU.doExport(accessionMapPath, mapD, fmt="json", indent=3)
        return ok

    def updateEntityAccessionMap(self):
        """Update entity to glycan accession mapping

        Returns:
            dict: {entityId: {'glyTouCanId':... , 'prdId': ..., }, ... }
        """
        entityAccessionMapD = {}
        wurcsTupL = []
        uniqueWurcsD = {}
        accessionMapD = self.fetchGlycanAccessionMap()
        branchedEntityD = self.getBranchedEntityDetails()
        for entityId, iD in branchedEntityD.items():
            if iD["wurcs"] and iD["wurcs"] not in accessionMapD and iD["wurcs"] not in uniqueWurcsD:
                wurcsTupL.append((entityId, iD["wurcs"]))
                uniqueWurcsD.setdefault(iD["wurcs"], []).append(entityId)
        if wurcsTupL:
            tMap = self.getAccessionMapping(wurcsTupL)
            accessionMapD.update(tMap)
            self.storeGlycanAccessionMap(accessionMapD)
        #

        for entityId, iD in branchedEntityD.items():
            if iD["wurcs"] in accessionMapD:
                prdId = iD["prdId"] if iD["wurcs"] else None
                entityAccessionMapD[entityId] = {"glyTouCanId": accessionMapD[iD["wurcs"]][0], "prdId": prdId}
        return entityAccessionMapD

    def getAccessionMapping(self, wurcsTupL):
        """Fetch GlyTouCan accessions for the input WURCS desriptor list"""
        accessionMapD = {}
        logger.info("Fetching (%d) WURCS descriptors", len(wurcsTupL))
        baseUrl = "https://api.glycosmos.org"
        endPoint = "glytoucan/sparql/wurcs2gtcids"
        numDescriptors = len(wurcsTupL)
        for ii, (entityId, wurcs) in enumerate(wurcsTupL, 1):
            try:
                pD = {}
                pD["wurcs"] = wurcs
                uR = UrlRequestUtil()
                rDL, retCode = uR.post(baseUrl, endPoint, pD, returnContentType="JSON")
                logger.debug(" %r wurcs fetch result (%r) %r", entityId, retCode, rDL)
                if rDL:
                    for rD in rDL:
                        if "id" in rD:
                            accessionMapD.setdefault(wurcs, []).append(rD["id"])
                        else:
                            logger.info("%r fetch fails (%r) (%r) %r", entityId, retCode, wurcs, rDL)
                if ii % 5 == 0:
                    logger.info("Fetched %d/%d", ii, numDescriptors)
            except Exception as e:
                logger.exception("Failing for (%r) wurcs (%r) with %s", entityId, wurcs, str(e))
        return accessionMapD
