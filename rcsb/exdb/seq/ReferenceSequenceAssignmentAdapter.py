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

import copy
import logging

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
        isTestMode = False
        if isTestMode:
            _, _ = self.__filter(copy.deepcopy(obj))
            return True, obj
        else:
            return self.__filter(obj)

    def __filter(self, obj):
        ok = True
        try:
            entityKey = obj["rcsb_id"]
            logger.info(" ------------- Running filter on %r --------------", entityKey)
            #
            referenceDatabaseName = "UniProt"
            provSourceL = "PDB"
            alignDL = None
            ersDL = None
            authAsymIdL = None
            taxIdL = None
            try:
                ersDL = obj["rcsb_polymer_entity_container_identifiers"]["reference_sequence_identifiers"]
                authAsymIdL = obj["rcsb_polymer_entity_container_identifiers"]["auth_asym_ids"]
            except Exception:
                pass
            #
            try:
                taxIdL = [oD["ncbi_taxonomy_id"] for oD in obj["rcsb_entity_source_organism"]]
                taxIdL = list(set(taxIdL))
                logger.debug("%s taxonomy (%d) %r", entityKey, len(taxIdL), taxIdL)
            except Exception as e:
                logger.debug("Failing with %s", str(e))
            #
            if ersDL:
                retDL = []
                dupD = {}
                for ersD in ersDL:
                    isMatched, isExcluded, updErsD = self.__reMapAccessions(entityKey, ersD, referenceDatabaseName, taxIdL, provSourceL)
                    #
                    logger.info("isMatched %r isExcluded %r updErsD %r", isMatched, isExcluded, updErsD)
                    if isMatched and updErsD["database_accession"] not in dupD:
                        dupD[updErsD["database_accession"]] = True
                        retDL.append(updErsD)
                        continue
                    #
                    if isExcluded:
                        continue
                    #
                    if not isMatched and entityKey not in dupD:
                        dupD[entityKey] = True
                        siftsAccDL = self.__getSiftsAccessions(entityKey, authAsymIdL)
                        for siftsAccD in siftsAccDL:
                            logger.info("Using SIFTS accession mapping for %s", entityKey)
                            retDL.append(siftsAccD)
                        if not siftsAccDL:
                            logger.info("No alternative SIFTS accession mapping for %s", entityKey)

                if retDL:
                    logger.debug("%s retDL %r", entityKey, retDL)
                    obj["rcsb_polymer_entity_container_identifiers"]["reference_sequence_identifiers"] = retDL
                else:
                    del obj["rcsb_polymer_entity_container_identifiers"]["reference_sequence_identifiers"]
                    logger.info("Incomplete reference sequence mapping update for %s", entityKey)
            #
            #
            try:
                alignDL = obj["rcsb_polymer_entity_align"]
            except Exception:
                pass
            if alignDL and authAsymIdL:
                retDL = []
                dupD = {}
                for alignD in alignDL:
                    isMatched, isExcluded, updAlignD, alignHash = self.__reMapAlignments(entityKey, alignD, referenceDatabaseName, taxIdL, provSourceL)
                    #
                    if isMatched and alignHash not in dupD:
                        if alignHash:
                            dupD[alignHash] = True
                        retDL.append(updAlignD)
                        continue
                    #
                    if isExcluded:
                        continue
                    if not isMatched and entityKey not in dupD:
                        dupD[entityKey] = True
                        siftsAlignDL = self.__getSiftsAlignments(entityKey, authAsymIdL)
                        for siftsAlignD in siftsAlignDL:
                            logger.info("Using SIFTS mapping for the alignment of %s", entityKey)
                            retDL.append(siftsAlignD)
                        if not siftsAlignDL:
                            logger.info("No alternative SIFTS alignment for %s", entityKey)
                    #
                if retDL:
                    logger.debug("%s retDL %r", entityKey, retDL)
                    obj["rcsb_polymer_entity_align"] = retDL
                else:
                    del obj["rcsb_polymer_entity_align"]
                    logger.info("Incomplete reference sequence alignment update for %s", entityKey)
        except Exception as e:
            ok = False
            logger.exception("Filter adapter failing with error with %s", str(e))
        #
        return ok, obj

    def __reMapAccessions(self, entityKey, rsiD, referenceDatabaseName, taxIdL, provSourceL, excludeReferenceDatabases=None):
        """Internal method to re-map accession for the input database and assignment source

        Args:
            rsiDL (list): list of accession
            databaseName (str, optional): resource database name. Defaults to 'UniProt'.
            provSource (str, optional): assignment provenance. Defaults to 'PDB'.

        Returns:
            bool, dict: flag for mapping success, and remapped (and unmapped) accessions in the input object list

        Example:
                    "P14118": {
                    "searchId": "P14118",
                    "matchedIds": {
                        "P84099": {
                        "taxId": 10090
                        },
                        "P84100": {
                        "taxId": 10116
                        },
                        "P84098": {
                        "taxId": 9606
                        }
                    },
                    "matched": "secondary"
                },
        """
        isMatched = False
        isExcluded = False
        excludeReferenceDatabases = excludeReferenceDatabases if excludeReferenceDatabases else ["PDB"]
        refDbList = ["UniProt", "GenBank", "EMBL", "NDB", "NORINE", "PIR", "PRF", "RefSeq"]
        #
        rId = rsiD["database_accession"]
        logger.debug("%s rId %r db %r prov %r", entityKey, rId, rsiD["database_name"], rsiD["provenance_source"])
        #
        if rsiD["database_name"] in excludeReferenceDatabases:
            isExcluded = True
        elif rsiD["database_name"] == referenceDatabaseName and rsiD["provenance_source"] in provSourceL:
            try:
                if rId in self.__matchD and self.__matchD[rId]["matched"] in ["primary"]:
                    # no change
                    isMatched = True
                elif rId in self.__matchD and self.__matchD[rId]["matched"] in ["secondary"]:
                    logger.debug("secondary %r matched len %d", self.__matchD[rId]["matched"], len(self.__matchD[rId]["matchedIds"]))
                    if len(self.__matchD[rId]["matchedIds"]) == 1:
                        for mId, mD in self.__matchD[rId]["matchedIds"].items():
                            rsiD["database_accession"] = mId
                            logger.info("%s matched secondary %s -> %s", entityKey, rId, mId)
                            isMatched = True
                    elif taxIdL and len(taxIdL) == 1:
                        #  -- simplest match case --
                        numM = 0
                        for mId, mD in self.__matchD[rId]["matchedIds"].items():
                            if taxIdL[0] == mD["taxId"]:
                                rsiD["database_accession"] = mId
                                numM += 1
                        if numM == 1:
                            isMatched = True
                            logger.info("%s matched secondary with taxId %r %s -> %s", entityKey, taxIdL[0], rId, rsiD["database_accession"])
                    elif not taxIdL:
                        logger.info("%s no taxids with UniProt (%s) secondary mapping", entityKey, rId)
                    else:
                        logger.info("%s ambiguous mapping for a UniProt (%s) secondary mapping - taxIds %r", entityKey, rId, taxIdL)
                #
            except Exception:
                pass

        elif rsiD["provenance_source"] in provSourceL and rsiD["database_name"] in refDbList:
            logger.info("%s leaving reference accession for %s %s assigned by %r", entityKey, rId, rsiD["database_name"], provSourceL)
            isMatched = True
        else:
            logger.info("%s leaving a reference accession for %s %s assigned by %r", entityKey, rId, rsiD["database_name"], rsiD["provenance_source"])
        #
        logger.debug("%s isMatched %r isExcluded %r for accession %r", entityKey, isMatched, isExcluded, rId)
        #
        return isMatched, isExcluded, rsiD

    def __reMapAlignments(self, entityKey, alignD, referenceDatabaseName, taxIdL, provSourceL, excludeReferenceDatabases=None):
        """Internal method to re-map alignments for the input databae and assignment source

        Args:
            alignD (dict): alignment object including accession and aligned regions
            databaseName (str, optional): resource database name. Defaults to 'UniProt'.
            provSourceL (list, optional): assignment provenance. Defaults to 'PDB'.

        Returns:
            bool, list: flag for mapping success, and remapped (and unmapped) accessions in the input align list
        """
        isExcluded = False
        isMatched = False
        excludeReferenceDatabases = excludeReferenceDatabases if excludeReferenceDatabases else ["PDB"]
        refDbList = ["UniProt", "GenBank", "EMBL", "NDB", "NORINE", "PIR", "PRF", "RefSeq"]
        provSourceL = provSourceL if provSourceL else []
        rId = alignD["reference_database_accession"]
        #
        if alignD["reference_database_name"] in excludeReferenceDatabases:
            isExcluded = True
        elif alignD["reference_database_name"] == referenceDatabaseName and alignD["provenance_code"] in provSourceL:
            try:
                if rId in self.__matchD and self.__matchD[rId]["matched"] in ["primary"]:
                    # no change
                    isMatched = True
                elif rId in self.__matchD and self.__matchD[rId]["matched"] in ["secondary"]:
                    if len(self.__matchD[rId]["matchedIds"]) == 1:
                        for mId, mD in self.__matchD[rId]["matchedIds"].items():
                            alignD["reference_database_accession"] = mId
                            isMatched = True
                    elif taxIdL and len(taxIdL) == 1:
                        #  -- simplest match case --
                        numM = 0
                        for mId, mD in self.__matchD[rId]["matchedIds"].items():
                            if taxIdL[0] == mD["taxId"]:
                                alignD["reference_database_accession"] = mId
                                numM += 1
                        if numM == 1:
                            isMatched = True
                    elif not taxIdL:
                        logger.info("%s no taxids with UniProt (%s) secondary mapping", entityKey, rId)
                    else:
                        logger.info("%s ambiguous mapping for a UniProt (%s) secondary mapping - taxIds %r", entityKey, rId, taxIdL)
                #
            except Exception:
                pass
        elif alignD["provenance_code"] in provSourceL and alignD["reference_database_name"] in refDbList:
            logger.info("%s leaving reference alignment for %s %s assigned by %r", entityKey, rId, alignD["reference_database_name"], provSourceL)
            isMatched = True
        else:
            logger.info("%s leaving a reference alignment for %s %s assigned by %r", entityKey, rId, alignD["reference_database_name"], alignD["provenance_code"])
        #
        logger.debug("%s isMatched %r isExcluded %r for alignment %r", entityKey, isMatched, isExcluded, rId)
        return isMatched, isExcluded, alignD, self.__hashAlignment(alignD)

    def __hashAlignment(self, aD):
        """
        Example:

            {'reference_database_name': 'UniProt', 'reference_database_accession': 'P62942', 'provenance_code': 'PDB',
              'aligned_regions': [{'entity_beg_seq_id': 1, 'ref_beg_seq_id': 1, 'length': 107}]}]
        """
        hsh = None
        hL = []
        try:
            hL.append(aD["reference_database_accession"])
            for aR in aD["aligned_regions"]:
                hL.append(aR["entity_beg_seq_id"])
                hL.append(aR["ref_beg_seq_id"])
                hL.append(aR["length"])
            hsh = tuple(hL)
        except Exception:
            pass
        return hsh

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
