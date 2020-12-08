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
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.db.mongo.Connection import Connection
from rcsb.db.mongo.MongoDbUtil import MongoDbUtil

logger = logging.getLogger(__name__)


class ChemRefExtractor(object):
    """Selected utilities to extract data from chemical component core collections."""

    def __init__(self, cfgOb):
        self.__cfgOb = cfgOb
        self.__resourceName = "MONGO_DB"
        #

    def getChemCompAccessionMapping(self, extResource, **kwargs):
        """Get the accession code mapping between chemical component identifiers and identifier(s) for the
            input external resource.

        Args:
            extFesource (str):  resource name (e.g. DrugBank, CCDC)
            **kwargs: unused

        Returns:
            dict: {dbExtId: True, dbExtId: True, ...  }

        """
        idD = {}
        logger.debug("With %r %r", extResource, kwargs)
        try:
            docList = []
            with Connection(cfgOb=self.__cfgOb, resourceName=self.__resourceName) as client:
                mg = MongoDbUtil(client)
                if mg.collectionExists("chem_comp_core", "bird_chem_comp_core"):
                    logger.info("Document count is %d", mg.count("chem_comp_core", "chem_comp_core"))
                    qD = {"rcsb_chem_comp_related.resource_name": extResource}
                    selectL = ["rcsb_chem_comp_related"]
                    tL = mg.fetch("chem_comp_core", "chem_comp_core", selectL, queryD=qD)
                    logger.info("CC mapping count %d", len(tL))
                    docList.extend(tL)

                if mg.collectionExists("bird_chem_comp_core", "bird_chem_comp_core"):
                    qD = {"rcsb_chem_comp_related.resource_name": extResource}
                    selectL = ["rcsb_chem_comp_related"]
                    tL = mg.fetch("bird_chem_comp_core", "bird_chem_comp_core", selectL, queryD=qD)
                    logger.info("CC/BIRD mapping count %d", len(tL))
                    docList.extend(tL)
                #
                for doc in docList:
                    dL = doc["rcsb_chem_comp_related"] if "rcsb_chem_comp_related" in doc else []
                    for dD in dL:
                        if dD["resource_name"] == extResource and "resource_accession_code" in dD:
                            idD[dD["resource_accession_code"]] = True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return idD
