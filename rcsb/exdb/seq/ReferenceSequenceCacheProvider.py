##
# File: ReferenceSequenceCacheProvider.py
# Date: 10-Feb-2020  jdw
#
# Utilities to cache referencence sequence data and mappings.
#
# Updates:
#
##
__docformat__ = "restructuredtext en"
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
    """  A skeleton class that implements the interface expected by the multiprocessing
         for fetching reference sequences --
    """

    def __init__(self, cfgOb, **kwargs):
        self.__cfgOb = cfgOb
        _ = kwargs
        self.__databaseName = "uniprot_exdb"
        self.__refDataCollectionName = "reference_entry"
        self.__matchDataCollectionName = "reference_match"
        self.__createCollections(self.__databaseName, self.__refDataCollectionName, indexAttributeNames=["rcsb_id", "rcsb_last_update"])
        self.__createCollections(self.__databaseName, self.__matchDataCollectionName, indexAttributeNames=["rcsb_id", "rcsb_last_update"])

    def updateList(self, dataList, procName, optionsD, workingDir):
        """  Update the input list of reference sequence identifiers and return
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
                    self.__updateReferenceData(self.__databaseName, self.__refDataCollectionName, retList2)
                    self.__updateReferenceData(self.__databaseName, self.__matchDataCollectionName, retList1)
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
    """  Utilities to cache referencence sequence data and mappings.

    """

    def __init__(self, cfgOb, siftsProvider=None, maxChunkSize=100, fetchLimit=None, expireDays=14, numProc=1, **kwargs):
        self.__cfgOb = cfgOb
        #
        self.__maxChunkSize = maxChunkSize
        self.__numProc = numProc
        #
        self.__databaseName = "uniprot_exdb"
        self.__refDataCollectionName = "reference_entry"
        self.__matchDataCollectionName = "reference_match"

        self.__ssP = siftsProvider
        self.__matchD, self.__refD = self.__reload(fetchLimit, expireDays, **kwargs)

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

    def testCache(self, minMatchPrimaryPercent=None, logSizes=False):
        okC = True
        if okC:
            return okC
        logger.info("Reference cache lengths: matchD %d refD %d", len(self.__matchD), len(self.__refD))
        ok = bool(self.__matchD and self.__refD)
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
                "RefMatchD %.2f RefD %.2f", getObjSize(self.__matchD) / 1000000.0, getObjSize(self.__refD) / 1000000.0,
            )
        return ok and okC

    def __reload(self, fetchLimit, expireDays, **kwargs):
        _ = kwargs
        logger.info("Reloading sequence reference data fetchLimit %r expireDays %r", fetchLimit, expireDays)
        numMissing = self.__refreshReferenceData(expireDays=expireDays, failureFraction=0.75)
        logger.info("Reference identifiers expired/missing %d", numMissing)
        #
        refIdMapD = {}
        matchD = {}
        refD = {}
        assignRefD = self.__getPolymerReferenceSequenceAssignments(fetchLimit)
        refIdMapD, _ = self.__getAssignmentMap(assignRefD)
        # refIdD[<database_accession>] = [entity_key1, entity_key2,...]
        entryIdL = [rcsbId[:4] for rcsbId in assignRefD]
        siftsUniProtL = self.__ssP.getEntryUniqueIdentifiers(entryIdL, idType="UNPID") if self.__ssP else []
        logger.info("Incorporating all %d SIFTS accessions for %d entries", len(siftsUniProtL), len(entryIdL))
        unpIdList = sorted(set(list(refIdMapD.keys()) + siftsUniProtL))
        #
        cacheUnpIdList = self.__getReferenceDataIds(expireDays=0)
        logger.info("Using %d cached reference sequences", len(cacheUnpIdList))
        #
        updateUnpIdList = sorted(set(unpIdList) - set(cacheUnpIdList))
        #
        if updateUnpIdList:
            logger.info("Update cache for %d UniProt accessions (consolidated)", len(updateUnpIdList))
            ok, failList = self.__updateReferenceData(updateUnpIdList)
            logger.info("Fetch references status is %r missing count %d", ok, len(failList))
        else:
            logger.info("No reference sequence updates required")
        #
        matchD = self.__getReferenceData(self.__databaseName, self.__matchDataCollectionName)
        refD = self.__getReferenceData(self.__databaseName, self.__refDataCollectionName)
        logger.info("Completed - returning match length %d and reference data length %d", len(matchD), len(refD))
        return matchD, refD

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
        logger.debug("After update (status=%r) Missing expired reference identifiers %d", ok, len(failList))
        tFrac = float(len(failList)) / float(len(idList))
        if tFrac < failureFraction:
            obUpd = ObjectUpdater(self.__cfgOb)
            selectD = {"rcsb_id": failList}
            numPurge = obUpd.delete(self.__databaseName, self.__matchDataCollectionName, selectD)
            if len(failList) != numPurge:
                logger.debug("Update match failures %d purge count %d", len(failList), numPurge)
            numPurge = obUpd.delete(self.__databaseName, self.__refDataCollectionName, selectD)
            if len(failList) != numPurge:
                logger.debug("Update reference data failures %d purge count %d", len(failList), numPurge)
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
        matchD = self.__getReferenceData(self.__databaseName, self.__matchDataCollectionName, selectD=selectD)
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
            self.__cfgOb, databaseName=databaseName, collectionName=collectionName, keyAttribute="rcsb_id", uniqueAttributes=["rcsb_id"], selectionQuery=selectD,
        )
        docCount = obEx.getCount()
        logger.debug("Reference data match count %d", docCount)
        objD = obEx.getObjects()
        return objD

    def __getPolymerReferenceSequenceAssignments(self, fetchLimit):
        """ Get all accessions assigned to input reference sequence database for the input polymerType.

            Returns:
             (dict): {"1abc_1": "rcsb_polymer_entity_container_identifiers": {"reference_sequence_identifiers": []},
                                "rcsb_entity_source_organism"" {"ncbi_taxonomy_id": []}
        """
        try:
            databaseName = "pdbx_core"
            collectionName = "pdbx_core_polymer_entity"
            polymerType = "Protein"
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
