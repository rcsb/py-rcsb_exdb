##
# File: PolymerEntityExtractor.py
# Date: 5-Dec-2020  jdw
#
# Utilities to extract selected details from the core polymer entity collections.
#
#
# Updates:
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from rcsb.exdb.seq.UniProtExtractor import UniProtExtractor
from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


def getRangeOverlap(entityBeg, entityEnd, refBeg, refEnd):
    r1 = range(entityBeg, entityEnd)
    r2 = range(refBeg, refEnd)
    if r1.start == r1.stop or r2.start == r2.stop:
        return set()
    if not ((r1.start < r2.stop and r1.stop > r2.start) or (r1.stop > r2.start and r2.stop > r1.start)):
        return set()
    return set(range(max(r1.start, r2.start), min(r1.stop, r2.stop) + 1))


class PolymerEntityExtractor(object):
    """Utilities to extract selected details from the core polymer entity collections."""

    def __init__(self, cfgOb):
        self.__cfgOb = cfgOb

    def exportProteinSequenceDetails(self, filePath, fmt="json", minSeqLen=0):
        """Export protein sequence and taxonomy data (required to build protein sequence fasta file)"""
        rD, missingSrcD = self.getProteinSequenceDetails(minSeqLen=minSeqLen)
        # ----
        mU = MarshalUtil()
        ok1 = mU.doExport(filePath, rD, fmt=fmt, indent=3)
        #
        pth, _ = os.path.split(filePath)
        mU = MarshalUtil()
        ok2 = mU.doExport(os.path.join(pth, "missingSrcNames.json"), missingSrcD, fmt="json")
        logger.info("Exporting (%d) protein sequence records with missing source count (%d) status %r", len(rD), len(missingSrcD), ok1 and ok2)

    def getProteinSequenceDetails(self, minSeqLen=0):
        """Get protein sequence and taxonomy data (required to build protein sequence fasta file)"""
        missingSrcD = {}
        rD = {}
        try:
            unpEx = UniProtExtractor(self.__cfgOb)
            unpD = unpEx.getReferenceSequenceDetails()
            #
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName="pdbx_core",
                collectionName="pdbx_core_polymer_entity",
                useCache=False,
                keyAttribute="entity",
                uniqueAttributes=["rcsb_id"],
                selectionQuery={"entity_poly.rcsb_entity_polymer_type": "Protein"},
                selectionList=[
                    "rcsb_id",
                    "rcsb_entity_source_organism",
                    "rcsb_polymer_entity.rcsb_source_part_count",
                    "rcsb_polymer_entity.rcsb_source_taxonomy_count",
                    "rcsb_polymer_entity.src_method",
                    "entity_poly",
                    "rcsb_polymer_entity_align",
                ],
            )
            #
            eCount = obEx.getCount()
            logger.info("Polymer entity count is %d", eCount)
            objD = obEx.getObjects()
            rD = {}
            for rId, eD in objD.items():

                try:
                    pD = eD["entity_poly"]
                    seqS = pD["pdbx_seq_one_letter_code_can"]
                    seqLen = len(seqS)
                except Exception:
                    logger.warning("%s no one-letter-code sequence", rId)
                #
                if seqLen < minSeqLen:
                    continue
                #
                srcMethod = None
                try:
                    pD = eD["rcsb_polymer_entity"]
                    srcMethod = pD["src_method"]
                except Exception:
                    pass
                #
                if "rcsb_entity_source_organism" not in eD:
                    logger.debug("%s No source information (%r) skipping (seqLen %d)", rId, srcMethod, seqLen)
                    continue
                try:
                    sL = []
                    for tD in eD["rcsb_entity_source_organism"]:
                        srcName = tD["scientific_name"] if "scientific_name" in tD else None
                        if "beg_seq_num" in tD and "end_seq_num" in tD:
                            begSeqNum = tD["beg_seq_num"]
                            endSeqNum = tD["end_seq_num"] if tD["end_seq_num"] <= seqLen else seqLen
                        else:
                            begSeqNum = 1
                            endSeqNum = seqLen
                        srcId = tD["pdbx_src_id"]
                        srcType = tD["source_type"]
                        taxId = tD["ncbi_taxonomy_id"] if "ncbi_taxonomy_id" in tD else -1
                        if srcName and taxId == -1:
                            missingSrcD.setdefault(srcName, []).append(rId)
                        orgName = tD["ncbi_scientific_name"] if "ncbi_scientific_name" in tD else ""
                        sL.append({"srcId": srcId, "taxId": taxId, "orgName": orgName, "entitySeqBeg": begSeqNum, "entitySeqEnd": endSeqNum})
                    if len(sL) == 1:
                        sL[0]["entitySeqBeg"] = 1
                        sL[0]["entitySeqEnd"] = seqLen

                except Exception as e:
                    logger.exception("Failing for (%r) tD %r with %s", rId, tD, str(e))
                #
                try:
                    pD = eD["rcsb_polymer_entity"]
                    partCount = pD["rcsb_source_part_count"]
                except Exception:
                    logger.warning("%s no source part count", rId)
                    partCount = 1
                try:
                    pD = eD["rcsb_polymer_entity"]
                    taxCount = pD["rcsb_source_taxonomy_count"]
                except Exception:
                    if srcType == "synthetic":
                        taxCount = 0
                    else:
                        logger.warning("%s (srcName %r) no source taxonomy count type %r", rId, srcName, srcType)
                        if srcName:
                            taxCount = 1
                        else:
                            taxCount = 0
                #
                uDL = []
                try:
                    for tD in eD["rcsb_polymer_entity_align"]:
                        uD = {}
                        if tD["reference_database_name"] in ["UniProt", "GenBank", "PIR", "EMBL", "NORINE", "PRF"]:
                            uD["refDbId"] = tD["reference_database_accession"]
                            uD["refDbName"] = tD["reference_database_name"]
                            uD["provSource"] = tD["provenance_source"]
                            if tD["reference_database_accession"] in unpD:
                                uD.update(unpD[tD["reference_database_accession"]])
                            aL = []
                            for qD in tD["aligned_regions"]:
                                if qD["entity_beg_seq_id"] + qD["length"] - 1 > seqLen:
                                    qD["length"] = seqLen - qD["entity_beg_seq_id"] + 1
                                srcId = self.__getSourcePart(rId, sL, qD["entity_beg_seq_id"], qD["length"])

                                aL.append({"srcId": srcId, "entitySeqBeg": qD["entity_beg_seq_id"], "refSeqBeg": qD["ref_beg_seq_id"], "length": qD["length"]})
                            uD["alignList"] = aL
                            uDL.append(uD)
                        else:
                            logger.info("%s reference database %s", rId, tD["reference_database_name"])

                except Exception:
                    pass
                rD[rId] = {"alignmentL": uDL, "sourceOrgL": sL, "partCount": partCount, "taxCount": taxCount, "sequence": seqS, "seqLen": seqLen}

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return rD, missingSrcD

    def __getSourcePart(self, entityId, sourceOrgL, entityBeg, seqLen):
        """Return the source part containing the input entity range -

        Args:
            sourceOrgL (list): list of source dictionaries
            entityBeg (int):  begining entity sequence position (matched region)
            seqLen (int):  length sequence range (matched region)

        Returns:
            (int): corresponding source part id or None
        """
        entityEnd = entityBeg + seqLen - 1
        for sD in sourceOrgL:
            srcId = sD["srcId"]
            if sD["entitySeqBeg"] <= entityBeg and sD["entitySeqEnd"] >= entityEnd:
                return srcId
        #
        if len(sourceOrgL) == 1:
            logger.error("%r (%d) Inconsistent range for beg %r end %r sourceOrgL %r", entityId, len(sourceOrgL), entityBeg, entityEnd, sourceOrgL)
            return 1
        else:
            ovTupL = []
            for sD in sourceOrgL:
                srcId = sD["srcId"]
                logger.debug("%r %r beg %r end %r beg %r end %r", entityId, srcId, sD["entitySeqBeg"], sD["entitySeqEnd"], entityBeg, entityEnd)
                oVS = getRangeOverlap(sD["entitySeqBeg"], sD["entitySeqEnd"], entityBeg, entityEnd)
                ovTupL.append((srcId, len(oVS)))
            rL = sorted(ovTupL, key=lambda x: x[1], reverse=True)
            logger.debug("ovTupL %r", rL)
            #
            return rL[0][0]

    def exportProteinEntityFasta(self, fastaPath, taxonPath, detailsPath, minSeqLen=10):
        """Export protein entity Fasta file and associated taxon mapping file (for mmseqs2)

        Args:
            fastaPath (str): protein sequence FASTA output file path
            taxonPath (str): taxon mapping file path (seqid TaxId) (tdd format)
            detailPath (str): protein entity details file path (json)

        Returns:
            bool: True for success or False otherwise

        Example:
            "5H7D_1": {
                    "alignmentL": [
                        {
                            "refDbId": "P42588",
                            "refDbName": "UniProt",
                            "provSource": "PDB",
                            "accession": "P42588",
                            "taxId": 83333,
                            "scientific_name": "Escherichia coli (strain K12)",
                            "gene": "patA",
                            "name": "PATase",
                            "alignList": [
                            {
                                "srcId": "1",
                                "entitySeqBeg": 5,
                                "refSeqBeg": 7,
                                "length": 447
                            }
                            ]
                        },
                        {
                            "refDbId": "P38507",
                            "refDbName": "UniProt",
                            "provSource": "PDB",
                            "accession": "P38507",
                            "taxId": 1280,
                            "scientific_name": "Staphylococcus aureus",
                            "gene": "spa",
                            "name": "IgG-binding protein A",
                            "alignList": [
                            {
                                "srcId": "2",
                                "entitySeqBeg": 452,
                                entitySeqBeg"220,
                                "length": 48
                            }
                            ]
                        }
                    ],
                    "sourceOrgL": [
                        {
                            "srcId": "1",
                            "taxId": 83333,
                            "orgName": "Escherichia coli K-12",
                            "entitySeqBeg": 1,
                            "entitySeqEnd": 451
                        },
                        {
                            "srcId": "2",
                            "taxId": 1280,
                            "orgName": "Staphylococcus aureus",
                            "entitySeqBeg": 452,
                            "entitySeqEnd": 499
                        }
                    ],
                    "partCount": 2,
                    "taxCount": 2,
                    "sequence": "GSHMSASALACSAHALNLIEKRTLDHEEMKALNREVIEYFKEHVNPGF...",
                    "seqLen": 499
                },
        >1ABC_#|prt|<taxid>|beg|end|refdb|refId|refTaxId|refbeg|refend|ref_gn|ref_nm
        """
        proteinSeqD, _ = self.getProteinSequenceDetails(minSeqLen=minSeqLen)
        ok = False

        try:
            taxonL = []
            seqDict = {}
            for eId, eD in proteinSeqD.items():
                #
                seq = eD["sequence"]
                for sD in eD["sourceOrgL"]:
                    srcId = sD["srcId"]
                    taxId = sD["taxId"]
                    seqBeg = int(sD["entitySeqBeg"])
                    seqEnd = int(sD["entitySeqEnd"])
                    seqLen = 1 + (seqEnd - seqBeg)
                    # orgName = sD["orgName"]
                    cD = {"sequence": seq[seqBeg - 1 : seqEnd], "entityId": eId, "srcId": srcId, "seqBeg": seqBeg, "seqEnd": seqEnd, "seqLen": seqLen, "taxId": taxId}
                    seqId = ""
                    cL = []
                    for k, v in cD.items():
                        if k in ["sequence"]:
                            continue
                        cL.append(str(v))
                        cL.append(str(k))
                    seqId = "|".join(cL)
                    seqDict[seqId] = cD
                    taxonL.append("%s\t%s" % (seqId, taxId))
                # ----
            mU = MarshalUtil()
            ok = mU.doExport(detailsPath, proteinSeqD, fmt="json", indent=3)
            ok = mU.doExport(fastaPath, seqDict, fmt="fasta")
            ok = mU.doExport(taxonPath, taxonL, fmt="list")
        except Exception as e:
            logger.exception("Failing %r with %s", fastaPath, str(e))
        return ok
