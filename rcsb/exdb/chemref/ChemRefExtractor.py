##
# File: ChemRefExtractor.py
# Date: 2-Jul-2018  jdw
#
# Selected utilities to extract data from chemical component core collections.
#
# Updates:
#  7-Jan-2019  jdw moved from ChemRefEtlWorker.
#  3-Sep-2019  jdw moved again to module rcsb.exdb.chemref
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor

logger = logging.getLogger(__name__)


class ChemRefExtractor(object):
    """Selected utilities to extract data from chemical component core collections."""

    def __init__(self, cfgOb):
        self.__cfgOb = cfgOb
        self.__resourceName = "MONGO_DB"
        #

    def getChemCompAccessionMapping(self, referenceResourceName):
        """Get the accession code mapping between chemical component identifiers and identifier(s) for the
            input external reference resource.

        Args:
            referenceResourceName (str):  resource name (e.g. DrugBank, ChEMBL, CCDC)

        Returns:
            dict: {referenceResourceId: chem_comp/bird_id, referenceResourceId: chem_comp/bird_id, ...  }

        """
        idD = {}
        try:
            databaseName = "bird_chem_comp_core"
            collectionName = "bird_chem_comp_core"
            selectD = {"rcsb_chem_comp_related.resource_name": referenceResourceName}
            selectionList = ["rcsb_id", "rcsb_chem_comp_related"]
            logger.info("Searching %s %s with selection query %r", databaseName, collectionName, selectD)
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName=databaseName,
                collectionName=collectionName,
                keyAttribute="rcsb_id",
                uniqueAttributes=["rcsb_id"],
                selectionQuery=selectD,
                selectionList=selectionList,
                stripObjectId=True,
            )
            logger.info("Reference data object count %d", obEx.getCount())
            objD = obEx.getObjects()
            for _, doc in objD.items():
                dL = doc["rcsb_chem_comp_related"] if "rcsb_chem_comp_related" in doc else []
                for dD in dL:
                    if dD["resource_name"] == referenceResourceName and "resource_accession_code" in dD:
                        idD.setdefault(dD["resource_accession_code"], []).append(dD["comp_id"])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return idD
