##
# File: ReferenceSequenceAssignmentProvider.py
# Date: 8-Oct-2019  jdw
#
# Utilities to cache content required to update referencence sequence assignments.
#
# Updates:
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
from collections import defaultdict


from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor
from rcsb.utils.ec.EnzymeDatabaseProvider import EnzymeDatabaseProvider
from rcsb.utils.io.IoUtil import getObjSize
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.seq.InterProProvider import InterProProvider
from rcsb.utils.seq.PfamProvider import PfamProvider
from rcsb.utils.seq.SiftsSummaryProvider import SiftsSummaryProvider
from rcsb.utils.seq.UniProtUtils import UniProtUtils
from rcsb.utils.go.GeneOntologyProvider import GeneOntologyProvider

logger = logging.getLogger(__name__)


class ReferenceSequenceAssignmentProvider(object):
    """  Utilities to cache content required to update referencence sequence assignments.

    """

    def __init__(
        self,
        cfgOb,
        databaseName="pdbx_core",
        collectionName="pdbx_core_polymer_entity",
        polymerType="Protein",
        referenceDatabaseName="UniProt",
        provSource="PDB",
        maxChunkSize=100,
        fetchLimit=None,
        **kwargs
    ):
        self.__cfgOb = cfgOb
        self.__polymerType = polymerType
        self.__mU = MarshalUtil()
        #
        self.__maxChunkSize = maxChunkSize
        self.__statusList = []
        #
        self.__pfP = self.__fetchPfamProvider(self.__cfgOb, self.__cfgOb.getDefaultSectionName(), **kwargs)
        self.__ipP = self.__fetchInterProProvider(self.__cfgOb, self.__cfgOb.getDefaultSectionName(), **kwargs)
        self.__ssP = self.__fetchSiftsSummaryProvider(self.__cfgOb, self.__cfgOb.getDefaultSectionName(), **kwargs)
        self.__goP = self.__fetchGoProvider(self.__cfgOb, self.__cfgOb.getDefaultSectionName(), **kwargs)
        self.__ecP = self.__fetchEcProvider(self.__cfgOb, self.__cfgOb.getDefaultSectionName(), **kwargs)
        self.__refIdMapD, self.__matchD, self.__refD = self.__reload(databaseName, collectionName, polymerType, referenceDatabaseName, provSource, fetchLimit, **kwargs)

    def goIdExists(self, goId):
        try:
            return self.__goP.exists(goId)
        except Exception as e:
            logger.exception("Failing for %r with %s", goId, str(e))
        return False

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

    def getPfamProvider(self):
        return self.__pfP

    def getInterProProvider(self):
        return self.__ipP

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

    def getRefIdMap(self):
        return self.__refIdMapD

    def getRefDataCount(self):
        return len(self.__refD)

    def testCache(self, minMatchPrimaryPercent=None, logSizes=False):
        okC = True
        logger.info("Reference cache lengths: refIdMap %d matchD %d refD %d", len(self.__refIdMapD), len(self.__matchD), len(self.__refD))
        ok = bool(self.__refIdMapD and self.__matchD and self.__refD)
        #
        numRef = len(self.__refIdMapD)
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
                "Pfam %.2f InterPro %.2f SIFTS %.2f GO %.2f EC %.2f RefIdMap %.2f RefMatchD %.2f RefD %.2f",
                getObjSize(self.__pfP) / 1000000.0,
                getObjSize(self.__ipP) / 1000000.0,
                getObjSize(self.__ssP) / 1000000.0,
                getObjSize(self.__goP) / 1000000.0,
                getObjSize(self.__ecP) / 1000000.0,
                getObjSize(self.__refIdMapD) / 1000000.0,
                getObjSize(self.__matchD) / 1000000.0,
                getObjSize(self.__refD) / 1000000.0,
            )
        return ok and okC

    def __reload(self, databaseName, collectionName, polymerType, referenceDatabaseName, provSource, fetchLimit, **kwargs):
        assignRefD = self.__getPolymerReferenceSequenceAssignments(databaseName, collectionName, polymerType, fetchLimit)
        refIdMapD, _ = self.__getAssignmentMap(assignRefD, referenceDatabaseName=referenceDatabaseName, provSource=provSource)
        #
        entryIdL = [rcsbId[:4] for rcsbId in assignRefD]
        siftsUniProtL = self.__ssP.getEntryUniqueIdentifiers(entryIdL, idType="UNPID")
        logger.info("Incorporating %d SIFTS accessions for %d entries", len(siftsUniProtL), len(entryIdL))
        unpIdList = sorted(set(list(refIdMapD.keys()) + siftsUniProtL))
        #
        logger.info("Rebuild cache for %d UniProt accessions (consolidated)", len(unpIdList))
        #
        matchD, refD = self.__rebuildReferenceCache(unpIdList, referenceDatabaseName, **kwargs)
        return refIdMapD, matchD, refD

    def __getPolymerReferenceSequenceAssignments(self, databaseName, collectionName, polymerType, fetchLimit):
        """ Get all accessions assigned to input reference sequence database for the input polymerType.

            Returns:
             (dict): {"1abc_1": "rcsb_polymer_entity_container_identifiers": {"reference_sequence_identifiers": []},
                                "rcsb_polymer_entity_align": [],
                                "rcsb_entity_source_organism"" {"ncbi_taxonomy_id": []}
        """
        try:
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName=databaseName,
                collectionName=collectionName,
                cacheFilePath=None,
                useCache=False,
                keyAttribute="entity",
                uniqueAttributes=["rcsb_id"],
                cacheKwargs=None,
                objectLimit=fetchLimit,
                selectionQuery={"entity_poly.rcsb_entity_polymer_type": polymerType},
                selectionList=[
                    "rcsb_id",
                    "rcsb_polymer_entity_container_identifiers.reference_sequence_identifiers",
                    "rcsb_polymer_entity_container_identifiers.auth_asym_ids",
                    # "rcsb_polymer_entity_align",
                    # "rcsb_entity_source_organism.ncbi_taxonomy_id",
                    # "rcsb_polymer_entity_container_identifiers.related_annotation_identifiers",
                    # "rcsb_polymer_entity_annotation",
                    "rcsb_entity_source_organism.ncbi_taxonomy_id",
                ],
            )
            eCount = obEx.getCount()
            logger.info("Polymer entity count type %s is %d", polymerType, eCount)
            objD = obEx.getObjects()
            logger.info("Reading polymer entity count %d ref accession length %d ", eCount, len(objD))
            #
        except Exception as e:
            logger.exception("Failing for %s (%s) with %s", databaseName, collectionName, str(e))
        return objD

    def __getAssignmentMap(self, objD, referenceDatabaseName="UniProt", provSource="PDB"):
        refIdD = defaultdict(list)
        taxIdD = defaultdict(list)
        numMissing = 0
        numMissingTaxons = 0
        for entityKey, eD in objD.items():
            try:
                accS = set()
                for ii, tD in enumerate(eD["rcsb_polymer_entity_container_identifiers"]["reference_sequence_identifiers"]):
                    if tD["database_name"] == referenceDatabaseName and tD["provenance_source"] == provSource:
                        accS.add(tD["database_accession"])
                        refIdD[tD["database_accession"]].append(entityKey)
                        #
                        # pick up the corresponding taxonomy -
                        try:
                            taxIdD[tD["database_accession"]].append(eD["rcsb_entity_source_organism"][ii]["ncbi_taxonomy_id"])
                        except Exception:
                            logger.debug("Failing taxonomy lookup for %s %r", entityKey, tD["database_accession"])
                            numMissingTaxons += 1

                logger.debug("PDB assigned sequences length %d", len(accS))
            except Exception as e:
                numMissing += 1
                logger.debug("No sequence assignments for %s with %s", entityKey, str(e))
        #
        numMultipleTaxons = 0
        for refId, taxIdL in taxIdD.items():
            taxIdL = list(set(taxIdL))
            if len(taxIdL) > 1:
                logger.debug("Multitple taxIds assigned to reference sequence id %s: %r", refId, taxIdL)
                numMultipleTaxons += 1

        logger.info("Entities with missing taxonomy %d", numMissingTaxons)
        logger.info("Reference sequences with multiple taxonomies %d", numMultipleTaxons)
        logger.info("Unique %s accession assignments by %s %d (entities missing archive accession assignments %d) ", referenceDatabaseName, provSource, len(refIdD), numMissing)
        return refIdD, taxIdD

    #
    def __rebuildReferenceCache(self, idList, refDbName, **kwargs):
        """
        """
        fetchLimit = None
        doMissing = True
        dD = {}
        cachePath = kwargs.get("cachePath", ".")
        dirPath = os.path.join(cachePath, "exdb")
        # cacheKwargs = kwargs.get("cacheKwargs", {"fmt": "json", "indent": 3})
        cacheKwargs = kwargs.get("cacheKwargs", {"fmt": "pickle"})
        useCache = kwargs.get("useCache", True)
        saveText = kwargs.get("saveText", False)
        #
        ext = "pic" if cacheKwargs["fmt"] == "pickle" else "json"
        fn = refDbName + "-ref-sequence-data-cache" + "." + ext
        dataCacheFilePath = os.path.join(dirPath, fn)
        #
        fn = refDbName + "-ref-sequence-id-cache" + ".json"
        accCacheFilePath = os.path.join(dirPath, fn)
        #
        self.__mU.mkdir(dirPath)
        if not useCache:
            for fp in [dataCacheFilePath, accCacheFilePath]:
                try:
                    os.remove(fp)
                except Exception:
                    pass
        #
        if useCache and accCacheFilePath and self.__mU.exists(accCacheFilePath) and dataCacheFilePath and self.__mU.exists(dataCacheFilePath):
            dD = self.__mU.doImport(dataCacheFilePath, **cacheKwargs)
            idD = self.__mU.doImport(accCacheFilePath, fmt="json")
            logger.info("Reading cached reference sequence ID and data cache files - cached match reference length %d", len(idD["matchInfo"]))
            idD["matchInfo"] = self.__rebuildReferenceMatchIndex(idList, dD["refDbCache"])
            # Check for completeness -
            if doMissing:
                missingS = set(idList) - set(idD["matchInfo"].keys())
                if missingS:
                    logger.info("Reference sequence cache missing %d accessions", len(missingS))
                    extraD, extraIdD = self.__fetchReferenceEntries(refDbName, list(missingS), saveText=saveText, fetchLimit=fetchLimit)
                    dD["refDbCache"].update(extraD["refDbCache"])
                    idD["matchInfo"].update(extraIdD["matchInfo"])
                    #
                    idD["matchInfo"] = self.__rebuildReferenceMatchIndex(idList, dD["refDbCache"])
                    #
                    if accCacheFilePath and dataCacheFilePath and cacheKwargs:
                        self.__mU.mkdir(dirPath)
                        ok1 = self.__mU.doExport(dataCacheFilePath, dD, **cacheKwargs)
                        ok2 = self.__mU.doExport(accCacheFilePath, idD, fmt="json", indent=3)
                        logger.info("Cache updated with missing references with status %r", ok1 and ok2)
            #
        else:
            logger.info("Rebuilding reference cache for %s for %d accessions with limit %r", refDbName, len(idList), fetchLimit)
            dD, idD = self.__fetchReferenceEntries(refDbName, idList, saveText=saveText, fetchLimit=fetchLimit)
            if accCacheFilePath and dataCacheFilePath and cacheKwargs:
                self.__mU.mkdir(dirPath)
                ok1 = self.__mU.doExport(dataCacheFilePath, dD, **cacheKwargs)
                ok2 = self.__mU.doExport(accCacheFilePath, idD, fmt="json", indent=3)
                logger.info("Cache save status %r", ok1 and ok2)

        return idD["matchInfo"], dD["refDbCache"]

    def __rebuildReferenceMatchIndex(self, idList, referenceD):
        fobj = UniProtUtils()
        logger.info("Rebuilding match index on idList (%d) using reference data (%d) %r", len(idList), len(referenceD), type(referenceD))
        matchD = fobj.rebuildMatchResultIndex(idList, referenceD)
        return matchD

    def __fetchReferenceEntries(self, refDbName, idList, saveText=False, fetchLimit=None):
        """ Fetch database entries from the input reference sequence database name.
        """
        dD = {"refDbName": refDbName, "refDbCache": {}}
        idD = {"matchInfo": {}, "refIdMap": {}}

        try:
            idList = idList[:fetchLimit] if fetchLimit else idList
            logger.info("Starting fetch for %d %s entries", len(idList), refDbName)
            if refDbName == "UniProt":
                fobj = UniProtUtils(saveText=saveText)
                logger.info("Maximum reference chunk size %d", self.__maxChunkSize)
                refD, matchD = fobj.fetchList(idList, maxChunkSize=self.__maxChunkSize)
                dD = {"refDbName": refDbName, "refDbCache": refD}
                idD = {"matchInfo": matchD}
            #
            # Check the coverage -
            #
            countD = defaultdict(int)
            logger.info("Match dictionary length %d", len(matchD))
            for _, mD in matchD.items():
                if "matched" in mD:
                    countD[mD["matched"]] += 1
            logger.info("Reference length %d match length %d coverage %r", len(refD), len(matchD), countD.items())
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return dD, idD

    def __fetchSiftsSummaryProvider(self, cfgOb, configName, **kwargs):
        abbreviated = kwargs.get("siftsAbbreviated", "TEST")
        cachePath = kwargs.get("cachePath", ".")
        cacheKwargs = kwargs.get("cacheKwargs", {"fmt": "pickle"})
        useCache = kwargs.get("useCache", True)
        #
        srcDirPath = os.path.join(cachePath, cfgOb.getPath("SIFTS_SUMMARY_DATA_PATH", sectionName=configName))
        cacheDirPath = os.path.join(cachePath, cfgOb.get("SIFTS_SUMMARY_CACHE_DIR", sectionName=configName))
        logger.debug("ssP %r %r", srcDirPath, cacheDirPath)
        ssP = SiftsSummaryProvider(srcDirPath=srcDirPath, cacheDirPath=cacheDirPath, useCache=useCache, abbreviated=abbreviated, cacheKwargs=cacheKwargs)
        ok = ssP.testCache()
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
