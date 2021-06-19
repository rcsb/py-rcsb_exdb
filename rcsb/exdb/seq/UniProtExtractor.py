##
# File: UniProtExtractor.py
# Date: 5-Dec-2020  jdw
#
# Utilities to extract selected details from the UniProt exchange collections.
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class UniProtExtractor(object):
    """Utilities to extract selected details from the UniProt exchange collections."""

    def __init__(self, cfgOb):
        self.__cfgOb = cfgOb

    def exportReferenceSequenceDetails(self, filePath, fmt="json"):
        rD = self.getReferenceSequenceDetails()
        mU = MarshalUtil()
        ok = mU.doExport(filePath, rD, fmt=fmt, indent=3)
        logger.info("Exporting (%d) UniProt reference sequences (status=%r)", len(rD), ok)
        return ok

    def getReferenceSequenceDetails(self):
        """Get reference protein sequence essential details (sequence, taxonomy, name, gene, ...)"""
        uD = None
        try:
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName="uniprot_exdb",
                collectionName="reference_entry",
                useCache=False,
                keyAttribute="uniprot",
                uniqueAttributes=["rcsb_id"],
                selectionQuery={},
                selectionList=[
                    "source_scientific",
                    "taxonomy_id",
                    "rcsb_id",
                    "gene",
                    "names",
                    "sequence",
                ],
            )
            #
            eCount = obEx.getCount()
            logger.info("Reference entry count is %d", eCount)
            objD = obEx.getObjects()
            rD = {}
            for rId, uD in objD.items():
                taxId = uD["taxonomy_id"]
                sn = uD["source_scientific"]
                sequence = uD["sequence"]
                gn = None
                pn = None
                if "gene" in uD:
                    for tD in uD["gene"]:
                        if tD["type"] == "primary":
                            gn = tD["name"]
                            break
                for tD in uD["names"]:
                    if tD["nameType"] == "recommendedName":
                        pn = tD["name"]
                        break
                rD[rId] = {"accession": rId, "taxId": taxId, "scientific_name": sn, "gene": gn, "name": pn, "sequence": sequence}

        except Exception as e:
            logger.exception("Failing uD %r with %s", uD, str(e))
        #
        return rD
