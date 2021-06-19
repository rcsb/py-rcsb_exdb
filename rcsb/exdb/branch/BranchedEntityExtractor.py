##
# File: BranchedEntityExtractor.py
# Date: 24-May-2021  jdw
#
# Utilities to extract selected details from the core branched entity collections.
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

from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class BranchedEntityExtractor(object):
    """Utilities to extract selected details from the core branched entity collections."""

    def __init__(self, cfgOb):
        self.__cfgOb = cfgOb

    def exportBranchedEntityDetails(self, filePath, fmt="json"):
        """Export branched entity details (BIRD mapping and WURCS descriptors)"""
        rD = self.getBranchedDetails()
        # ----
        mU = MarshalUtil()
        ok = mU.doExport(filePath, rD, fmt=fmt, indent=3)
        logger.info("Exporting (%d) branched entities status %r", len(rD), ok)
        return ok

    def getBranchedDetails(self):
        """Get branched entity details (BIRD mapping and WURCS descriptors)"""
        rD = {}
        try:

            #
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName="pdbx_core",
                collectionName="pdbx_core_branched_entity",
                useCache=False,
                keyAttribute="entity",
                uniqueAttributes=["rcsb_id"],
                selectionQuery={},
                selectionList=["rcsb_id", "pdbx_entity_branch_descriptor", "rcsb_branched_entity_container_identifiers"],
            )
            #
            # eCount = obEx.getCount()
            # logger.info("Branched entity count is %d", eCount)
            objD = obEx.getObjects()
            rD = {}
            for _, eD in objD.items():
                rcsbId = eD["rcsb_id"]
                #
                prdId = None
                try:
                    pD = eD["rcsb_branched_entity_container_identifiers"]
                    prdId = pD["prd_id"]
                except Exception:
                    pass
                #
                wurcs = None
                try:
                    for tD in eD["pdbx_entity_branch_descriptor"]:
                        if tD["type"] == "WURCS":
                            wurcs = tD["descriptor"]
                except Exception:
                    pass
                if prdId or wurcs:
                    rD[rcsbId] = {"prdId": prdId, "wurcs": wurcs}

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return rD
