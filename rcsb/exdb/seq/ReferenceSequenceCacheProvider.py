##
# File: ReferenceSequenceCacheProvider.py
# Date: 10-Feb-2020  jdw
#
# Utilities to cache referencence sequence data and mappings.
#
# Updates:
# 8-Apr-2020 jdw change testCache() conditions to specifically track missing matched reference Id codes.
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
from collections import defaultdict


from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor
from rcsb.exdb.utils.ObjectUpdater import ObjectUpdater
from rcsb.utils.io.IoUtil import getObjSize
from rcsb.utils.io.TimeUtil import TimeUtil
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil
from rcsb.utils.seq.UniProtUtils import UniProtUtils

logger = logging.getLogger(__name__)


class ReferenceUpdateWorker(object):
    """A skeleton class that implements the interface expected by the multiprocessing
    for fetching reference sequences --
    """

    def __init__(self, cfgOb, **kwargs):
        self.__cfgOb = cfgOb
        _ = kwargs
        self.__refDatabaseName = "uniprot_exdb"
        self.__refDataCollectionName = "reference_entry"
        self.__refMatchDataCollectionName = "reference_match"
        #
        self.__createCollections(self.__refDatabaseName, self.__refDataCollectionName, indexAttributeNames=["rcsb_id", "rcsb_last_update"])
        self.__createCollections(self.__refDatabaseName, self.__refMatchDataCollectionName, indexAttributeNames=["rcsb_id", "rcsb_last_update"])

    def updateList(self, dataList, procName, optionsD, workingDir):
        """Update the input list of reference sequence identifiers and return
        matching diagnostics and reference feature data.
        """
        _ = optionsD
        _ = workingDir
        saveText = optionsD.get("saveText", False)
        fetchLimit = optionsD.get("fetchLimit", None)
        refDbName = optionsD.get("refDbName", "UniProt")
        maxChunkSize = optionsD.get("maxChunkSize", 50)
        successList = []
        retList1 = []
        retList2 = []
        diagList = []
        emptyList = []
        #
        try:
            tU = TimeUtil()
            idList = dataList[:fetchLimit] if fetchLimit else dataList
            logger.info("%s starting fetch for %d %s entries", procName, len(idList), refDbName)
            if refDbName == "UniProt":
                fobj = UniProtUtils(saveText=saveText)
                logger.debug("Maximum reference chunk size %d", maxChunkSize)
                refD, matchD = fobj.fetchList(idList, maxChunkSize=maxChunkSize)
                if len(matchD) == len(idList):
                    for uId, tD in matchD.items():
                        tD["rcsb_id"] = uId.strip()
                        tD["rcsb_last_update"] = tU.getDateTimeObj(tU.getTimestamp())
                        retList1.append(tD)
                    for uId, tD in refD.items():
                        tD["rcsb_id"] = uId.strip()
                        tD["rcsb_last_update"] = tU.getDateTimeObj(tU.getTimestamp())
                        retList2.append(tD)
                    successList.extend(idList)
                    self.__updateReferenceData(self.__refDatabaseName, self.__refDataCollectionName, retList2)
                    self.__updateReferenceData(self.__refDatabaseName, self.__refMatchDataCollectionName, retList1)
                else:
                    logger.info("Failing with fetch for %d entries with matchD %r", len(idList), matchD)
            else:
                logger.error("Unsupported reference database %r", refDbName)
        except Exception as e:
            logger.exception("Failing %s for %d data items %s", procName, len(dataList), str(e))
        logger.info("%s dataList length %d success length %d rst1 %d rst2 %d", procName, len(dataList), len(successList), len(retList1), len(retList2))
        #
        return successList, emptyList, emptyList, diagList

    def __updateReferenceData(self, databaseName, collectionName, objDL):
        updateDL = []
        for objD in objDL:
            try:
                selectD = {"rcsb_id": objD["rcsb_id"]}
                updateDL.append({"selectD": selectD, "updateD": objD})
            except Exception as e:
                logger.exception("Failing with %s", str(e))
        obUpd = ObjectUpdater(self.__cfgOb)
        numUpd = obUpd.update(databaseName, collectionName, updateDL)
        logger.debug("Updated reference count is %d", numUpd)

    def __createCollections(self, databaseName, collectionName, indexAttributeNames=None):
        obUpd = ObjectUpdater(self.__cfgOb)
        ok = obUpd.createCollection(databaseName, collectionName, indexAttributeNames=indexAttributeNames, checkExists=True, bsonSchema=None)
        return ok


class ReferenceSequenceCacheProvider(object):
    """Utilities to cache referencence sequence data and correspondence mappings."""

    def __init__(self, cfgOb, databaseName, collectionName, polymerType, siftsProvider=None, maxChunkSize=50, fetchLimit=None, expireDays=14, numProc=1, **kwargs):
        self.__cfgOb = cfgOb
        #
        self.__maxChunkSize = maxChunkSize
        self.__numProc = numProc
        #
        self.__refDatabaseName = "uniprot_exdb"
        self.__refDataCollectionName = "reference_entry"
        self.__refMatchDataCollectionName = "reference_match"

        self.__ssP = siftsProvider
        self.__matchD, self.__refD, self.__missingMatchIds = self.__reload(databaseName, collectionName, polymerType, fetchLimit, expireDays, **kwargs)

    def getMatchInfo(self):
        return self.__matchD

    def getRefData(self):
        return self.__refD

    def getMissingMatchedIdCodes(self):
        return self.__missingMatchIds

    def getDocuments(self, formatType="exchange"):
        fobj = UniProtUtils(saveText=False)
        exObjD = fobj.reformat(self.__refD, formatType=formatType)
        return list(exObjD.values())

    def getRefDataCount(self):
        return len(self.__refD)

    def testCache(self, minMatchPrimaryPercent=None, logSizes=False, minMissing=0):
        """Test the state of reference sequence data relative to proportion of matched primary sequence
        in the primary data set.

        Args:
            minMatchPrimaryPercent (float, optional): minimal acceptable of matching primary accessions. Defaults to None.
            logSizes (bool, optional): flag to log resource sizes. Defaults to False.
            minMissing (int, optional):  minimum acceptable missing matched reference Ids. Defaults to 0.

        Returns:
            bool: True for success or False otherwise
        """
        try:
            ok = bool(self.__matchD and self.__refD and self.__missingMatchIds <= minMissing)
            logger.info("Reference cache lengths: matchD %d refD %d missing matches %d", len(self.__matchD), len(self.__refD), self.__missingMatchIds)
            if ok:
                return ok
        except Exception as e:
            logger.error("Failing with unexpected cache state %s", str(e))
            return False
        #
        # -- The remaining check on the portion is not currently --
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
            logger.info("Primary reference match count test status %r", okC)
        #
        if logSizes:
            logger.info(
                "RefMatchD %.2f RefD %.2f",
                getObjSize(self.__matchD) / 1000000.0,
                getObjSize(self.__refD) / 1000000.0,
            )
        return ok and okC

    def __reload(self, databaseName, collectionName, polymerType, fetchLimit, expireDays, **kwargs):
        _ = kwargs

        # --  This
        logger.info("Reloading sequence reference data fetchLimit %r expireDays %r", fetchLimit, expireDays)
        numMissing = self.__refreshReferenceData(expireDays=expireDays, failureFraction=0.75)
        logger.info("Reference identifiers expired/missing %d", numMissing)
        # --
        refIdMapD = {}
        matchD = {}
        refD = {}
        failList = []
        #
        # assignRefD: Dict of all entities of polymerType "Protein" (or other), with associated container_identifiers and other info as corresponding values
        assignRefD = self.__getPolymerReferenceSequenceAssignments(databaseName, collectionName, polymerType, fetchLimit)
        logger.info("Polymer reference sequence assignments %d (assignRefD)", len(assignRefD))
        #
        # refIdMapD: Dict of all *unique* UniProt Ids of entities that have:
        #    "rcsb_polymer_entity_container_identifiers.reference_sequence_identifiers.provenance_source":"PDB",
        #    "rcsb_polymer_entity_container_identifiers.reference_sequence_identifiers.database_name":"UniProt",
        #    "entity_poly.rcsb_entity_polymer_type":"Protein"
        #  Values are the list of entities that have those UniProt IDs
        #  i.e. refIdMapD[<database_accession>] = [entity_key1, entity_key2,...]
        # This will usually only contain several hundred to a few thousand IDs
        refIdMapD, _ = self.__getAssignmentMap(assignRefD)
        logger.info("Reference ID assignemnt map length %d (refIdMapD)", len(refIdMapD))
        #
        # List of all entry IDs for entities in assignRefD (will contain duplicates for entries with >1 entity)
        entryIdL = [rcsbId[:4] for rcsbId in assignRefD]
        #
        # List of *unique* UniProt IDs from SIFTS for all protein (or, "polymerType") entries currently in ExDB
        siftsUniProtL = self.__ssP.getEntryUniqueIdentifiers(entryIdL, idType="UNPID") if self.__ssP else []
        logger.info("Incorporating all %d SIFTS accessions for %d entities", len(siftsUniProtL), len(entryIdL))
        #
        # unpIdList: List of all *unique* UniProt IDs combined from 'refIdMapD' and 'siftsUniProtL'
        #            Since not everything will be covered by SIFTS, this will be slightly more than siftsUniProtL
        unpIdList = sorted(set(list(refIdMapD.keys()) + siftsUniProtL))
        logger.info("UniProt ID list length %d (unpIdList)", len(unpIdList))
        #
        # cacheUnpIdList: List of UniProt IDs from uniprot_exdb.reference_match, from today backwards
        cacheUnpIdList = self.__getReferenceDataIds(expireDays=0)
        logger.info("Using %d cached reference sequences", len(cacheUnpIdList))
        #
        # updateUnpIdList: List of the *delta* UniProt IDs between what's possible based on entity collections (unpIdList)
        #                  and what's already in uniprot_exdb.reference_match (cacheUnpIdList)
        updateUnpIdList = sorted(set(unpIdList) - set(cacheUnpIdList))
        logger.info("UniProt list lengths (unique): set(unpIdList) %d - set(cacheUnpIdList) %d", len(set(unpIdList)), len(set(cacheUnpIdList)))
        #
        if updateUnpIdList:
            logger.info("Updating cache for %d UniProt accessions (consolidated PDB + SIFTS)", len(updateUnpIdList))
            ok, failList = self.__updateReferenceData(updateUnpIdList)
            logger.info("Fetch references update status is %r missing count %d", ok, len(failList))
        else:
            logger.info("No reference sequence updates required")
        #
        matchD = self.__getReferenceData(self.__refDatabaseName, self.__refMatchDataCollectionName)
        refD = self.__getReferenceData(self.__refDatabaseName, self.__refDataCollectionName)
        logger.info("Completed - returning match length %d and reference data length %d num missing %d", len(matchD), len(refD), len(failList))
        return matchD, refD, len(failList)

    def __refreshReferenceData(self, expireDays=14, failureFraction=0.75):
        """Update expired reference data and purge any obsolete data not to exceeding the
        the input failureFraction.

        Args:
            expireDays (int, optional): expiration interval in days. Defaults to 14.
            failureFraction (float, optional): fractional limit of obsolete entries purged. Defaults to 0.75.

        Returns:
            (int): number of obsolete entries purged

        """
        idList = self.__getReferenceDataIds(expireDays=expireDays)
        logger.info("Expired (days=%d) reference identifiers %d", expireDays, len(idList))
        if not idList:
            return 0
        #
        ok, failList = self.__updateReferenceData(idList)
        logger.info("After reference update (status=%r) missing expired match identifiers %d", ok, len(failList))
        tFrac = float(len(failList)) / float(len(idList))
        if tFrac < failureFraction:
            obUpd = ObjectUpdater(self.__cfgOb)
            selectD = {"rcsb_id": failList}
            numPurge = obUpd.delete(self.__refDatabaseName, self.__refMatchDataCollectionName, selectD)
            if len(failList) != numPurge:
                logger.info("Update match failures %d purge count %d", len(failList), numPurge)
            numPurge = obUpd.delete(self.__refDatabaseName, self.__refDataCollectionName, selectD)
            if len(failList) != numPurge:
                logger.info("Update reference data failures %d purge count %d", len(failList), numPurge)
        return len(failList)

    def __getReferenceDataIds(self, expireDays=14):
        """Get reference data identifiers subject to an expiration interval
         (i.e. not updated in/older than deltaDays)

        Args:
            expireDays (int, optional): expiration interval in days. Defaults to 14.

        Returns:
            (list): reference identifier list
        """
        selectD = None
        if expireDays > 0:
            tU = TimeUtil()
            tS = tU.getTimestamp(useUtc=True, before={"days": expireDays})
            selectD = {"rcsb_latest_update": {"$lt": tU.getDateTimeObj(tS)}}
        matchD = self.__getReferenceData(self.__refDatabaseName, self.__refMatchDataCollectionName, selectD=selectD)
        return sorted(matchD.keys())

    def __updateReferenceData(self, idList):
        numProc = self.__numProc
        chunkSize = self.__maxChunkSize
        logger.info("Length starting list is %d", len(idList))
        optD = {"maxChunkSize": chunkSize}
        rWorker = ReferenceUpdateWorker(self.__cfgOb)
        mpu = MultiProcUtil(verbose=True)
        mpu.setOptions(optD)
        mpu.set(workerObj=rWorker, workerMethod="updateList")
        ok, failList, resultList, _ = mpu.runMulti(dataList=idList, numProc=numProc, numResults=2, chunkSize=chunkSize)
        logger.info("Multi-proc %r failures %r result lengths %r %r", ok, len(failList), len(resultList[0]), len(resultList[1]))
        return ok, failList

    def __getReferenceData(self, databaseName, collectionName, selectD=None):
        logger.info("Searching %s %s with selection query %r", databaseName, collectionName, selectD)
        obEx = ObjectExtractor(
            self.__cfgOb,
            databaseName=databaseName,
            collectionName=collectionName,
            keyAttribute="rcsb_id",
            uniqueAttributes=["rcsb_id"],
            selectionQuery=selectD,
        )
        docCount = obEx.getCount()
        logger.debug("Reference data match count %d", docCount)
        objD = obEx.getObjects()
        return objD

    def __getPolymerReferenceSequenceAssignments(self, databaseName, collectionName, polymerType, fetchLimit):
        """Get all accessions assigned to input reference sequence database for the input polymerType.

        Returns:
         (dict): {"1abc_1": "rcsb_polymer_entity_container_identifiers": {"reference_sequence_identifiers": []},
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
                    "rcsb_entity_source_organism.ncbi_taxonomy_id",
                ],
            )
            eCount = obEx.getCount()
            logger.info("Polymer entity count type %s is %d", polymerType, eCount)
            objD = obEx.getObjects()
            logger.info("Reading polymer entity count %d reference accession length %d ", eCount, len(objD))
            #
        except Exception as e:
            logger.exception("Failing for %s (%s) with %s", databaseName, collectionName, str(e))
        return objD

    def __getAssignmentMap(self, polymerEntityObjD):
        referenceDatabaseName = "UniProt"
        provSource = "PDB"
        refIdD = defaultdict(list)
        taxIdD = defaultdict(list)
        numMissing = 0
        numMissingTaxons = 0
        for entityKey, eD in polymerEntityObjD.items():
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
