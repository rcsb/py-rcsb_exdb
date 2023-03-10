##
# File: ReferenceSequenceAnnotationProvider.py
# Date: 14-Feb-2020  jdw
#
# Utilities to cache content required to update referencence sequence annotations.
#
# Updates:
#  25-May-2022 dwp Add error checking for SIFTS data loading
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
from collections import defaultdict

from rcsb.exdb.seq.ReferenceSequenceCacheProvider import ReferenceSequenceCacheProvider
from rcsb.utils.ec.EnzymeDatabaseProvider import EnzymeDatabaseProvider
from rcsb.utils.go.GeneOntologyProvider import GeneOntologyProvider
from rcsb.utils.io.IoUtil import getObjSize
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.seq.GlyGenProvider import GlyGenProvider
from rcsb.utils.seq.InterProProvider import InterProProvider
from rcsb.utils.seq.PfamProvider import PfamProvider
from rcsb.utils.seq.SiftsSummaryProvider import SiftsSummaryProvider
from rcsb.utils.seq.UniProtUtils import UniProtUtils

logger = logging.getLogger(__name__)


class ReferenceSequenceAnnotationProvider(object):
    """Utilities to cache content required to update referencence sequence annotations."""

    def __init__(self, cfgOb, databaseName, collectionName, polymerType, maxChunkSize=10, fetchLimit=None, numProc=2, expireDays=14, **kwargs):
        self.__cfgOb = cfgOb
        self.__mU = MarshalUtil()
        #
        self.__maxChunkSize = maxChunkSize
        self.__statusList = []
        #
        self.__ggP = self.__fetchGlyGenProvider(self.__cfgOb, self.__cfgOb.getDefaultSectionName(), **kwargs)
        self.__pfP = self.__fetchPfamProvider(self.__cfgOb, self.__cfgOb.getDefaultSectionName(), **kwargs)
        self.__ipP = self.__fetchInterProProvider(self.__cfgOb, self.__cfgOb.getDefaultSectionName(), **kwargs)
        self.__ssP = self.__fetchSiftsSummaryProvider(self.__cfgOb, self.__cfgOb.getDefaultSectionName(), **kwargs)
        self.__goP = self.__fetchGoProvider(self.__cfgOb, self.__cfgOb.getDefaultSectionName(), **kwargs)
        self.__ecP = self.__fetchEcProvider(self.__cfgOb, self.__cfgOb.getDefaultSectionName(), **kwargs)
        #
        self.__rsaP = ReferenceSequenceCacheProvider(
            self.__cfgOb, databaseName, collectionName, polymerType, siftsProvider=self.__ssP, maxChunkSize=maxChunkSize, numProc=numProc, fetchLimit=fetchLimit, expireDays=expireDays
        )
        self.__matchD = self.__rsaP.getMatchInfo()
        self.__refD = self.__rsaP.getRefData()
        self.__missingMatchedIdCodes = self.__rsaP.getMissingMatchedIdCodes()

    def goIdExists(self, goId):
        try:
            return self.__goP.exists(goId)
        except Exception as e:
            logger.exception("Failing for %r with %s", goId, str(e))
        return False

    def getGeneOntologyName(self, goId):
        try:
            return self.__goP.getName(goId)
        except Exception as e:
            logger.exception("Failing for %r with %s", goId, str(e))
        return None

    def getGeneOntologyLineage(self, goIdL):
        # "id"     "name"
        gL = []
        try:
            gTupL = self.__goP.getUniqueDescendants(goIdL)
            for gTup in gTupL:
                gL.append({"id": gTup[0], "name": gTup[1]})
        except Exception as e:
            logger.exception("Failing for %r with %s", goIdL, str(e))
        return gL

    def getGlyGenProvider(self):
        return self.__ggP

    def getPfamProvider(self):
        return self.__pfP

    def getPfamName(self, idCode):
        return self.__pfP.getDescription(idCode)

    def getInterProProvider(self):
        return self.__ipP

    def getInterProName(self, idCode):
        return self.__ipP.getDescription(idCode)

    def getInterProLineage(self, idCode):
        linL = []
        try:
            tupL = self.__ipP.getLineageWithNames(idCode)
            for tup in tupL:
                linL.append({"id": tup[0], "name": tup[1], "depth": tup[2]})
        except Exception as e:
            logger.exception("Failing for %r with %s", idCode, str(e))
        return linL

    def getEcProvider(self):
        return self.__ecP

    def getSiftsSummaryProvider(self):
        return self.__ssP

    def getMatchInfo(self):
        return self.__matchD

    def getRefData(self):
        return self.__refD

    def getDocuments(self, formatType="exchange"):
        fobj = UniProtUtils(saveText=False)
        exObjD = fobj.reformat(self.__refD, formatType=formatType)
        return list(exObjD.values())

    def getRefDataCount(self):
        return len(self.__refD)

    def testCache(self, minMatchPrimaryPercent=None, logSizes=False, minMissing=0):
        okC = True
        logger.info("Reference sequence cache lengths: matchD %d refD %d", len(self.__matchD), len(self.__refD))
        logger.info("missingMatchedIdCodes %r minMissing %r", self.__missingMatchedIdCodes, minMissing)
        ok = bool(self.__matchD and self.__refD and self.__ssP and self.__missingMatchedIdCodes <= minMissing)
        logger.info("Initial testCache check status %r", ok)
        #
        numRef = len(self.__matchD)
        countD = defaultdict(int)
        logger.info("Match dictionary length %d", len(self.__matchD))
        for _, mD in self.__matchD.items():
            if "matched" in mD:
                countD[mD["matched"]] += 1
        logger.info("Reference length %d match length %d coverage %r", len(self.__refD), len(self.__matchD), countD.items())
        if minMatchPrimaryPercent:
            try:
                okC = 100.0 * float(countD["primary"]) / float(numRef) > minMatchPrimaryPercent
            except Exception:
                okC = False
            logger.info("Primary reference match percent test status %r", okC)
        #
        if logSizes:
            logger.info(
                "SIFTS %.2f GO %.2f EC %.2f RefMatchD %.2f RefD %.2f",
                getObjSize(self.__ssP) / 1000000.0,
                getObjSize(self.__goP) / 1000000.0,
                getObjSize(self.__ecP) / 1000000.0,
                getObjSize(self.__matchD) / 1000000.0,
                getObjSize(self.__refD) / 1000000.0,
            )
        return ok and okC

    def __fetchSiftsSummaryProvider(self, cfgOb, configName, **kwargs):
        abbreviated = kwargs.get("siftsAbbreviated", "TEST")
        cachePath = kwargs.get("cachePath", ".")
        cacheKwargs = kwargs.get("cacheKwargs", {"fmt": "pickle"})
        useCache = kwargs.get("useCache", True)
        #
        siftsSummaryDataPath = cfgOb.getPath("SIFTS_SUMMARY_DATA_PATH", sectionName=configName)
        if siftsSummaryDataPath.lower().startswith("http"):
            srcDirPath = siftsSummaryDataPath
        else:
            srcDirPath = os.path.join(cachePath, siftsSummaryDataPath)
        cacheDirPath = os.path.join(cachePath, cfgOb.get("SIFTS_SUMMARY_CACHE_DIR", sectionName=configName))
        logger.debug("ssP %r %r", srcDirPath, cacheDirPath)
        ssP = SiftsSummaryProvider(srcDirPath=srcDirPath, cacheDirPath=cacheDirPath, useCache=useCache, abbreviated=abbreviated, cacheKwargs=cacheKwargs)
        ok = ssP.testCache()
        if not ok:
            logger.error("Failed to refetch SIFTS summary data using srcDirPath %s, cacheDirPath %s", srcDirPath, cacheDirPath)
            return None
        logger.debug("SIFTS cache status %r", ok)
        logger.debug("ssP entry count %d", ssP.getEntryCount())
        return ssP

    def __fetchGoProvider(self, cfgOb, configName, **kwargs):
        cachePath = kwargs.get("cachePath", ".")
        useCache = kwargs.get("useCache", True)
        #
        cacheDirPath = os.path.join(cachePath, cfgOb.get("EXDB_CACHE_DIR", sectionName=configName))
        logger.debug("goP %r %r", cacheDirPath, useCache)
        goP = GeneOntologyProvider(goDirPath=cacheDirPath, useCache=useCache)
        ok = goP.testCache()
        logger.debug("Gene Ontology (%r) root node count %r", ok, goP.getRootNodes())
        return goP

    def __fetchEcProvider(self, cfgOb, configName, **kwargs):
        cachePath = kwargs.get("cachePath", ".")
        useCache = kwargs.get("useCache", True)
        #
        cacheDirPath = os.path.join(cachePath, cfgOb.get("ENZYME_CLASSIFICATION_CACHE_DIR", sectionName=configName))
        logger.debug("ecP %r %r", cacheDirPath, useCache)
        ecP = EnzymeDatabaseProvider(enzymeDirPath=cacheDirPath, useCache=useCache)
        ok = ecP.testCache()
        logger.debug("Enzyme cache status %r", ok)
        return ecP

    def __fetchGlyGenProvider(self, cfgOb, configName, **kwargs):
        _ = cfgOb
        _ = configName
        cachePath = kwargs.get("cachePath", ".")
        useCache = kwargs.get("useCache", True)
        ggP = GlyGenProvider(cachePath=cachePath, useCache=useCache)
        ok = ggP.testCache()
        return ggP if ok else None

    def __fetchPfamProvider(self, cfgOb, configName, **kwargs):
        _ = cfgOb
        _ = configName
        cachePath = kwargs.get("cachePath", ".")
        useCache = kwargs.get("useCache", True)
        pfP = PfamProvider(cachePath=cachePath, useCache=useCache)
        ok = pfP.testCache()
        return pfP if ok else None

    def __fetchInterProProvider(self, cfgOb, configName, **kwargs):
        _ = cfgOb
        _ = configName
        cachePath = kwargs.get("cachePath", ".")
        useCache = kwargs.get("useCache", True)
        ipP = InterProProvider(cachePath=cachePath, useCache=useCache)
        ok = ipP.testCache()
        return ipP if ok else None
