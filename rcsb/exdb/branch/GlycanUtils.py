##
#  File:           GlycanUtils.py
#  Date:           24-May-2021 jdw
#
# Updates:
#  8-Jun-2026 dwp Switch to using built-in requests library; update GlyTouCan API URL
##
"""
Utilities for fetching and mapping glycan accessions.
"""

import time
import logging
import os.path
import requests

from rcsb.exdb.branch.BranchedEntityExtractor import BranchedEntityExtractor
from rcsb.utils.io.MarshalUtil import MarshalUtil

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
        """Fetch GlyTouCan accessions for the input WURCS descriptor list.
        Retries on timeouts, connection errors, and HTTP 5XX responses, while immediately failing on 4XX responses.

        Example:
          curl -d wurcs='WURCS=2.0/1,2,1/[a2122h-1a_1-5]/1-1/a4-b1' https://api.glycosmos.org/sparqlist/wurcs2gtcids

        Docs: https://doc.glycosmos.org/api/glytoucan
        """
        accessionMapD = {}
        numDescriptors = len(wurcsTupL)
        logger.info("Fetching (%d) WURCS descriptors", numDescriptors)
        url = "https://api.glycosmos.org/sparqlist/wurcs2gtcids"

        for ii, (entityId, wurcs) in enumerate(wurcsTupL, 1):
            try:
                maxRetries = 3
                for attempt in range(maxRetries):
                    try:
                        response = requests.post(url, data={"wurcs": wurcs}, timeout=30)
                        retCode = response.status_code

                        if 500 <= retCode < 600:
                            raise requests.HTTPError(f"Server error ({retCode})", response=response)

                        response.raise_for_status()
                        rDL = response.json()
                        break

                    except (requests.Timeout, requests.ConnectionError) as e:
                        if attempt == maxRetries - 1:
                            raise
                        waitTime = 2 ** attempt
                        logger.warning(
                            "Retry %d/%d for entityId %r after %d second(s) due to %s",
                            attempt + 1,
                            maxRetries,
                            entityId,
                            waitTime,
                            str(e),
                        )
                        time.sleep(waitTime)

                    except requests.HTTPError as e:
                        if e.response is not None and 400 <= e.response.status_code < 500:
                            raise

                        if attempt == maxRetries - 1:
                            raise

                        waitTime = 2 ** attempt
                        logger.warning(
                            "Retry %d/%d for entityId %r after %d second(s) due to HTTP %d",
                            attempt + 1,
                            maxRetries,
                            entityId,
                            waitTime,
                            retCode,
                        )
                        time.sleep(waitTime)

                logger.debug(" %r wurcs fetch result (%r) %r", entityId, retCode, rDL)
                if rDL:
                    for rD in rDL:
                        if "id" in rD:
                            accessionMapD.setdefault(wurcs, []).append(rD["id"])
                        else:
                            logger.info("%r fetch fails (%r) (%r) %r", entityId, retCode, wurcs, rDL)
                else:
                    logger.info("No results returned (%r) for entityId %r wurcs '%r'", retCode, entityId, wurcs)

                if ii % 5 == 0:
                    logger.info("Fetched %d/%d", ii, numDescriptors)

            except requests.RequestException as e:
                logger.exception("Failing for (%r) wurcs (%r) with %s", entityId, wurcs, str(e))

        return accessionMapD
