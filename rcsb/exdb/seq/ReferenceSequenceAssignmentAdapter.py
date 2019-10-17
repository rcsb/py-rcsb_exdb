##
# File: ReferenceSequenceAssignmentAdapter.py
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
from collections import defaultdict

from rcsb.exdb.utils.ObjectAdapterBase import ObjectAdapterBase

logger = logging.getLogger(__name__)


class ReferenceSequenceAssignmentAdapter(ObjectAdapterBase):
    """  Selected utilities to update reference sequence assignments information
         in the core_entity collection.
    """

    def __init__(self, refSeqAssignProvider):
        super(ReferenceSequenceAssignmentAdapter, self).__init__()
        #
        self.__rsaP = refSeqAssignProvider
        self.__ssP = self.__rsaP.getSiftsSummaryProvider()
        self.__refD = self.__rsaP.getRefData()
        self.__matchD = self.__rsaP.getMatchInfo()

    def filter(self, obj, **kwargs):
        return True, obj

    def __reMapAccessions(self, rsiDL, referenceDatabaseName="UniProt", provSourceL=None, excludeReferenceDatabases=None):
        """Internal method to re-map accessions for the input databae and assignment source

        Args:
            rsiDL (list): list of accession
            databaseName (str, optional): resource database name. Defaults to 'UniProt'.
            provSource (str, optional): assignment provenance. Defaults to 'PDB'.

        Returns:
            bool, list: flag for mapping success, and remapped (and unmapped) accessions in the input object list
        """
        isMatched = False
        unMapped = 0
        matched = 0
        excludeReferenceDatabases = excludeReferenceDatabases if excludeReferenceDatabases else ["PDB"]
        provSourceL = provSourceL if provSourceL else []
        retDL = []
        for rsiD in rsiDL:
            if rsiD["database_name"] in excludeReferenceDatabases:
                unMapped += 1
                continue
            if rsiD["database_name"] == referenceDatabaseName and rsiD["provenance_source"] in provSourceL:
                try:
                    if len(self.__matchD[rsiD["database_accession"]]["matchedIds"]) == 1:
                        rsiD["database_accession"] = self.__matchD[rsiD["database_accession"]]["matchedIds"][0]
                        matched += 1
                    else:
                        logger.info("Skipping mapping to multiple superseding accessions %s", rsiD["database_accession"])
                    #
                except Exception:
                    unMapped += 1
            retDL.append(rsiD)
        if matched == len(retDL):
            isMatched = True
        return not unMapped, isMatched, retDL

    def __reMapAlignments(self, alignDL, referenceDatabaseName="UniProt", provSourceL=None, excludeReferenceDatabases=None):
        """Internal method to re-map alignments for the input databae and assignment source

        Args:
            alignDL (list): list of aligned regions
            databaseName (str, optional): resource database name. Defaults to 'UniProt'.
            provSourceL (list, optional): assignment provenance. Defaults to 'PDB'.

        Returns:
            bool, list: flag for mapping success, and remapped (and unmapped) accessions in the input align list
        """
        isMatched = False
        unMapped = 0
        matched = 0
        excludeReferenceDatabases = excludeReferenceDatabases if excludeReferenceDatabases else ["PDB"]
        retDL = []
        provSourceL = provSourceL if provSourceL else []
        for alignD in alignDL:
            if alignD["reference_database_name"] in excludeReferenceDatabases:
                unMapped += 1
                continue
            if alignD["reference_database_name"] == referenceDatabaseName and alignD["provenance_code"] in provSourceL:
                try:
                    if len(self.__matchD[alignD["reference_database_accession"]]["matchedIds"]) == 1:
                        alignD["reference_database_accession"] = self.__matchD[alignD["reference_database_accession"]]["matchedIds"][0]
                        matched += 1
                    else:
                        logger.info("Skipping alignment mapping to multiple superseding accessions %s", alignD["reference_database_accession"])
                except Exception:
                    unMapped += 1
            retDL.append(alignD)
        if matched == len(retDL):
            isMatched = True
        #
        return not unMapped, isMatched, retDL

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
                #
                #
                if ersDL:
                    authAsymIdL = eD["rcsb_entity_container_identifiers"]["auth_asym_ids"]
                    isMapped, isMatched, updErsDL = self.__reMapAccessions(ersDL, referenceDatabaseName="UniProt", provSourceL=["PDB"])
                    #
                    if not isMapped or not isMatched:
                        tL = self.__getSiftsAccessions(entityKey, authAsymIdL)
                        if tL:
                            logger.debug("Using SIFTS accession mapping for %s", entityKey)
                        else:
                            logger.info("No alternative SIFTS accession mapping for %s", entityKey)
                        updErsDL = tL if tL else []
                    #
                    if len(updErsDL) < len(ersDL):
                        logger.info("Incomplete reference sequence mapping update for %s", entityKey)
                    updateD["rcsb_entity_container_identifiers.reference_sequence_identifiers"] = updErsDL
                #
                alignDL = eD["rcsb_polymer_entity_align"] if "rcsb_polymer_entity_align" in eD else None
                if alignDL and authAsymIdL:
                    isMapped, isMatched, updAlignDL = self.__reMapAlignments(alignDL, referenceDatabaseName="UniProt", provSourceL=["PDB"])
                    #
                    if not isMapped or not isMatched:
                        tL = self.__getSiftsAlignments(entityKey, authAsymIdL)
                        if tL:
                            logger.debug("Using SIFTS alignment mapping for %s", entityKey)
                        else:
                            logger.info("No alternative SIFTS alignment mapping for %s", entityKey)
                        updAlignDL = tL if tL else updAlignDL
                    #
                    if len(updAlignDL) < len(alignDL):
                        logger.info("Incomplete alignment mapping update for %s", entityKey)
                    updateD["rcsb_polymer_entity_align"] = updAlignDL
                #
                if updateD:
                    updateDL.append({"selectD": selectD, "updateD": updateD})
            except Exception as e:
                logger.exception("Mapping error for %s with %s", entityKey, str(e))
        #
        return updateDL

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
