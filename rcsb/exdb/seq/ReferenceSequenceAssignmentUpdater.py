##
# File: ReferenceSequenceAssignmentUpdater.py
# Date: 8-Oct-2019  jdw
#
# Selected utilities to update reference sequence assignments information
# in the core_entity collection.
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

from rcsb.db.processors.DataExchangeStatus import DataExchangeStatus
from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor
from rcsb.exdb.utils.ObjectUpdater import ObjectUpdater
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.seq.SiftsSummaryProvider import SiftsSummaryProvider
from rcsb.utils.seq.UniProtUtils import UniProtUtils

logger = logging.getLogger(__name__)


class ReferenceSequenceAssignmentUpdater(object):
    """  Selected utilities to update reference sequence assignments information
         in the core_entity collection.

    """

    def __init__(self, cfgOb, databaseName="pdbx_core", collectionName="pdbx_core_entity", polymerType="Protein", referenceDatabaseName="UniProt", provSource="PDB", **kwargs):
        self.__cfgOb = cfgOb
        self.__polymerType = polymerType
        self.__mU = MarshalUtil()
        #
        self.__databaseName = databaseName
        self.__collectionName = collectionName
        self.__statusList = []
        #
        self.__ssP = self.__fetchSiftsSummaryProvider(self.__cfgOb, self.__cfgOb.getDefaultSectionName(), **kwargs)
        self.__assignRefD, self.__refD, self.__matchD = self.__reload(databaseName, collectionName, polymerType, referenceDatabaseName, provSource, **kwargs)

    def __reload(self, databaseName, collectionName, polymerType, referenceDatabaseName, provSource, **kwargs):
        assignRefD = self.__getPolymerReferenceSequenceAssignments(databaseName, collectionName, polymerType, **kwargs)
        refIdD = self.__getUniqueAssignments(assignRefD, referenceDatabaseName=referenceDatabaseName, provSource=provSource)
        refD, matchD = self.__rebuildReferenceCache(referenceDatabaseName, list(refIdD.keys()), **kwargs)
        return assignRefD, refD, matchD

    def doUpdate(self, updateId, updateLimit=None):
        desp = DataExchangeStatus()
        statusStartTimestamp = desp.setStartTime()
        #
        numUpd = 0
        updateDL = self.__buildUpdate(self.__assignRefD)
        if updateDL:
            if updateLimit:
                numUpd = self.__doUpdate(self.__cfgOb, updateDL[:updateLimit], self.__databaseName, self.__collectionName)
            else:
                numUpd = self.__doUpdate(self.__cfgOb, updateDL, self.__databaseName, self.__collectionName)
        self.__updateStatus(updateId, self.__databaseName, self.__collectionName, True, statusStartTimestamp)
        return len(updateDL), numUpd

    def __doUpdate(self, cfgOb, updateDL, databaseName, collectionName):
        obUpd = ObjectUpdater(cfgOb)
        numUpd = obUpd.update(databaseName, collectionName, updateDL)
        logger.info("Update count is %d", numUpd)

        return numUpd

    def __getPolymerReferenceSequenceAssignments(self, databaseName, collectionName, polymerType, **kwargs):
        """ Get all accessions assigned to input reference sequence database for the input polymerType.

            Returns:
             (dict): {"1abc_1": "rcsb_entity_container_identifiers": {"reference_sequence_identifiers": []}, "rcsb_polymer_entity_align": []}
        """
        cachePath = kwargs.get("cachePath", ".")
        exDbDir = "exdb"
        cacheKwargs = kwargs.get("cacheKwargs", {"fmt": "json", "indent": 3})
        useCache = kwargs.get("useCache", True)
        fetchLimit = kwargs.get("fetchLimit", None)
        cacheFilePath = os.path.join(cachePath, exDbDir, "entity-poly-ref-seq-assign-cache.json")
        #
        try:
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName=databaseName,
                collectionName=collectionName,
                cacheFilePath=cacheFilePath,
                useCache=useCache,
                keyAttribute="entity",
                uniqueAttributes=["rcsb_id"],
                cacheKwargs=cacheKwargs,
                objectLimit=fetchLimit,
                selectionQuery={"entity_poly.rcsb_entity_polymer_type": polymerType},
                selectionList=[
                    "rcsb_id",
                    "rcsb_entity_container_identifiers.reference_sequence_identifiers",
                    "rcsb_entity_container_identifiers.auth_asym_ids",
                    "rcsb_polymer_entity_align",
                ],
            )
            eCount = obEx.getCount()
            logger.info("Entity count is %d", eCount)
            objD = obEx.getObjects()
            logger.info("Reading polymer entity entity count %d ref accession length %d ", eCount, len(objD))
            #
        except Exception as e:
            logger.exception("Failing for %s (%s) with %s", databaseName, collectionName, str(e))
        return objD

    def __getUniqueAssignments(self, objD, referenceDatabaseName="UniProt", provSource="PDB"):
        refIdD = defaultdict(list)
        for entityKey, eD in objD.items():
            try:
                accS = set()
                for tD in eD["rcsb_entity_container_identifiers"]["reference_sequence_identifiers"]:
                    if tD["database_name"] == referenceDatabaseName and tD["provenance_source"] == provSource:
                        accS.add(tD["database_accession"])
                        refIdD[tD["database_accession"]].append(entityKey)
                logger.debug("PDB assigned sequences length %d", len(accS))
            except Exception as e:
                logger.warning("No sequence assignments for %s with %s", entityKey, str(e))
        #
        logger.info("Unique %s accession assignments by %s %d ", referenceDatabaseName, provSource, len(refIdD))
        return refIdD

    def __reMapAccessions(self, rsiDL, referenceDatabaseName="UniProt", provSourceL=None):
        """Internal method to re-map accessions for the input databae and assignment source

        Args:
            rsiDL (list): list of accession
            databaseName (str, optional): resource database name. Defaults to 'UniProt'.
            provSource (str, optional): assignment provenance. Defaults to 'PDB'.

        Returns:
            bool, list: flag for mapping success, and remapped (and unmapped) accessions in the input object list
        """
        unMapped = 0
        matched = 0
        provSourceL = provSourceL if provSourceL else []
        retDL = []
        for rsiD in rsiDL:
            if rsiD["database_name"] == referenceDatabaseName and rsiD["provenance_source"] in provSourceL:
                try:
                    rsiD["database_accession"] = self.__matchD[rsiD["database_accession"]]["matchedIds"][0]
                    matched += 1
                except Exception:
                    unMapped += 1
                retDL.append(rsiD)
        return not unMapped, matched, retDL

    def __reMapAlignments(self, alignDL, referenceDatabaseName="UniProt", provSourceL=None):
        """Internal method to re-map alignments for the input databae and assignment source

        Args:
            alignDL (list): list of aligned regions
            databaseName (str, optional): resource database name. Defaults to 'UniProt'.
            provSourceL (list, optional): assignment provenance. Defaults to 'PDB'.

        Returns:
            bool, list: flag for mapping success, and remapped (and unmapped) accessions in the input align list
        """
        unMapped = 0
        matched = 0
        retDL = []
        provSourceL = provSourceL if provSourceL else []
        for alignD in alignDL:
            if alignD["reference_database_name"] == referenceDatabaseName and alignD["provenance_code"] in provSourceL:
                try:
                    alignD["reference_database_accession"] = self.__matchD[alignD["reference_database_accession"]]["matchedIds"][0]
                    matched += 1
                except Exception:
                    unMapped += 1
                retDL.append(alignD)
        return not unMapped, matched, retDL

    def __getSiftsAccessions(self, entityKey, authAsymIdL):
        retL = []
        saoLD = self.__ssP.getLongestAlignments(entityKey[:4], authAsymIdL)
        for (_, dbAccession), _ in saoLD.items():
            retL.append({"database_name": "UniProt", "database_accession": dbAccession, "provenance_source": "SIFTS"})
        return retL

    def __getSiftsAlignments(self, entityKey, authAsymIdL):
        retL = []
        saoLD = self.__ssP.getLongestAlignments(entityKey[:4], authAsymIdL)
        for (_, dbAccession), saoL in saoLD.items():
            dD = {"reference_database_name": "UniProt", "reference_database_accession": dbAccession, "provenance_code": "SIFTS", "aligned_regions": []}
            for sao in saoL:
                dD["aligned_regions"].append({"ref_beg_seq_id": sao.getDbSeqIdBeg(), "entity_beg_seq_id": sao.getEntitySeqIdBeg(), "length": sao.getEntityAlignLength()})
            retL.append(dD)
        return retL

    def __buildUpdate(self, assignRefD):
        #
        updateDL = []
        for entityKey, eD in assignRefD.items():
            selectD = {"rcsb_id": entityKey}
            try:
                updateD = {}
                authAsymIdL = []
                ersDL = (
                    eD["rcsb_entity_container_identifiers"]["reference_sequence_identifiers"]
                    if "reference_sequence_identifiers" in eD["rcsb_entity_container_identifiers"]
                    else None
                )
                if ersDL:
                    authAsymIdL = eD["rcsb_entity_container_identifiers"]["auth_asym_ids"]
                    isMapped, isMatched, updErsDL = self.__reMapAccessions(ersDL, referenceDatabaseName="UniProt", provSourceL=["PDB"])
                    if not isMapped or not isMatched:
                        tL = self.__getSiftsAccessions(entityKey, authAsymIdL)
                        if tL:
                            logger.info("Using SIFTS accession mapping for %s", entityKey)
                        updErsDL = tL if tL else updErsDL
                    if updErsDL:
                        updateD["rcsb_entity_container_identifiers.reference_sequence_identifiers"] = updErsDL
                #
                alignDL = eD["rcsb_polymer_entity_align"] if "rcsb_polymer_entity_align" in eD else None
                if alignDL and authAsymIdL:
                    isMapped, isMatched, updAlignDL = self.__reMapAlignments(alignDL, referenceDatabaseName="UniProt", provSourceL=["PDB"])
                    #
                    if not isMapped or not isMatched:
                        tL = self.__getSiftsAlignments(entityKey, authAsymIdL)
                        if tL:
                            logger.info("Using SIFTS alignment mapping for %s", entityKey)
                        updAlignDL = tL if tL else updAlignDL
                    if updAlignDL:
                        updateD["rcsb_polymer_entity_align"] = updAlignDL
                #
                if updateD:
                    updateDL.append({"selectD": selectD, "updateD": updateD})
            except Exception as e:
                logger.exception("Mapping error for %s with %s", entityKey, str(e))
        #
        return updateDL

    def __rebuildReferenceCache(self, refDbName, idList, **kwargs):
        """
        """
        dD = {}
        cachePath = kwargs.get("cachePath", ".")
        dirPath = os.path.join(cachePath, "exdb")
        cacheKwargs = kwargs.get("cacheKwargs", {"fmt": "json", "indent": 3})
        useCache = kwargs.get("useCache", True)
        fetchLimit = kwargs.get("fetchLimit", None)
        saveText = kwargs.get("saveText", False)
        #
        ext = "pic" if cacheKwargs["fmt"] == "pickle" else "json"
        fn = "ref-sequence-data-cache" + "." + ext
        cacheFilePath = os.path.join(dirPath, fn)
        #
        self.__mU.mkdir(dirPath)
        if not useCache:
            for fp in [cacheFilePath]:
                try:
                    os.remove(fp)
                except Exception:
                    pass
        #
        if useCache and cacheFilePath and self.__mU.exists(cacheFilePath):
            dD = self.__mU.doImport(cacheFilePath, **cacheKwargs)
            # Check for completeness -
            missingS = set(dD["refDbCache"].keys()) - set(idList)
            if missingS:
                logger.info("Reference sequence cache missing %d accessions", len(missingS))
                extraD = self.__fetchReferenceEntries(refDbName, list(missingS), saveText=saveText, fetchLimit=fetchLimit)
                dD["refDbCache"].update(extraD["refDbCache"])
                dD["matchInfo"].update(extraD["matchInfo"])
                if cacheFilePath and cacheKwargs:
                    self.__mU.mkdir(dirPath)
                    ok = self.__mU.doExport(cacheFilePath, dD, **cacheKwargs)
                    logger.info("Cache updated with status %r", ok)
            #
        else:
            dD = self.__fetchReferenceEntries(refDbName, idList, saveText=saveText, fetchLimit=fetchLimit)
            if cacheFilePath and cacheKwargs:
                self.__mU.mkdir(dirPath)
                ok = self.__mU.doExport(cacheFilePath, dD, **cacheKwargs)
                logger.info("Cache save status %r", ok)

        return dD["refDbCache"], dD["matchInfo"]

    def __fetchReferenceEntries(self, refDbName, idList, saveText=False, fetchLimit=None):
        """ Fetch database entries from the input reference sequence database name.
        """
        dD = {"refDbName": refDbName, "refDbCache": {}, "matchInfo": {}}

        try:
            idList = idList[:fetchLimit] if fetchLimit else idList
            logger.info("Starting fetch for %d %s entries", len(idList), refDbName)
            if refDbName == "UniProt":
                fobj = UniProtUtils(saveText=saveText)
                refD, matchD = fobj.fetchList(idList)
                dD = {"refDbName": refDbName, "refDbCache": refD, "matchInfo": matchD}

        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return dD

    def __fetchSiftsSummaryProvider(self, cfgOb, configName, **kwargs):
        abbreviated = kwargs.get("siftsAbbreviated", "PROD")
        cachePath = kwargs.get("cachePath", ".")
        cacheKwargs = kwargs.get("cacheKwargs", {"fmt": "pickle"})
        useCache = kwargs.get("useCache", True)
        #
        srcDirPath = os.path.join(cachePath, cfgOb.getPath("SIFTS_SUMMARY_DATA_PATH", sectionName=configName))
        cacheDirPath = os.path.join(cachePath, cfgOb.get("SIFTS_SUMMARY_CACHE_DIR", sectionName=configName))
        logger.debug("ssP %r %r", srcDirPath, cacheDirPath)
        ssP = SiftsSummaryProvider(srcDirPath=srcDirPath, cacheDirPath=cacheDirPath, useCache=useCache, abbreviated=abbreviated, cacheKwargs=cacheKwargs)
        logger.info("ssP entry count %d", ssP.getEntryCount())
        return ssP

    def __dumpEntries(self, refD):
        for (eId, eDict) in refD.items():
            logger.info("------ Reference id %s", eId)
            for k, v in eDict.items():
                logger.info("%-15s = %r", k, v)

    def __getUpdateAssignmentCandidates(self, objD):
        totCount = 0
        difCount = 0
        pdbUnpIdD = defaultdict(list)
        siftsUnpIdD = defaultdict(list)
        assignIdDifD = defaultdict(list)
        #
        for entityKey, eD in objD.items():
            try:
                siftsS = set()
                pdbS = set()
                for tD in eD["rcsb_entity_container_identifiers"]["reference_sequence_identifiers"]:
                    if tD["database_name"] == "UniProt":
                        if tD["provenance_source"] == "SIFTS":
                            siftsS.add(tD["database_accession"])
                            siftsUnpIdD[tD["database_accession"]].append(entityKey)
                        elif tD["provenance_source"] == "PDB":
                            pdbS.add(tD["database_accession"])
                            pdbUnpIdD[tD["database_accession"]].append(entityKey)
                    else:
                        logger.debug("No UniProt for %r", eD["rcsb_entity_container_identifiers"])
                logger.debug("PDB assigned sequence length %d", len(pdbS))
                logger.debug("SIFTS assigned sequence length %d", len(siftsS))

                if pdbS and siftsS:
                    totCount += 1
                    if pdbS != siftsS:
                        difCount += 1
                        for idV in pdbS:
                            assignIdDifD[idV].append(entityKey)

            except Exception as e:
                logger.warning("No identifiers for %s with %s", entityKey, str(e))
        #
        logger.info("Total %d differences %d", totCount, difCount)
        logger.info("Unique UniProt accession assignments PDB %d  SIFTS %d", len(pdbUnpIdD), len(siftsUnpIdD))
        logger.info("Current unique overalapping assignment differences %d ", len(assignIdDifD))
        logger.info("Current unique overalapping assignment differences %r ", assignIdDifD)
        return assignIdDifD, pdbUnpIdD, siftsUnpIdD

    def getReferenceAccessionAlignSummary(self):
        """ Summarize the alignment of PDB accession assignments with the current reference sequence database.
        """
        numPrimary = 0
        numSecondary = 0
        numNone = 0
        for _, mD in self.__matchD.items():
            if mD["matched"] == "primary":
                numPrimary += 1
            elif mD["matched"] == "secondary":
                numSecondary += 1
            else:
                numNone += 1
        logger.debug("Matched primary:  %d secondary: %d none %d", numPrimary, numSecondary, numNone)
        return numPrimary, numSecondary, numNone

    def getLoadStatus(self):
        return self.__statusList

    def __updateStatus(self, updateId, databaseName, collectionName, status, startTimestamp):
        try:
            sFlag = "Y" if status else "N"
            desp = DataExchangeStatus()
            desp.setStartTime(tS=startTimestamp)
            desp.setObject(databaseName, collectionName)
            desp.setStatus(updateId=updateId, successFlag=sFlag)
            desp.setEndTime()
            self.__statusList.append(desp.getStatus())
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False
