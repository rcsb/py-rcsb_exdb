##
# File: TaxonomyExtractor.py
# Date: 15-Oct-2019  jdw
#
# Utilities to extract taxonomy details from the core entity collection.
#
# Updates:
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor

logger = logging.getLogger(__name__)


class TaxonomyExtractor(object):
    """  Utilities to extract taxonomy details from the core entity collection.
    """

    def __init__(self, cfgOb):
        self.__cfgOb = cfgOb
        self.__databaseName = "pdbx_core"
        self.__collectionName = "pdbx_core_polymer_entity"

    def getUniqueTaxons(self):
        taxIdL = self.__extractEntityTaxons()
        return taxIdL

    def __extractEntityTaxons(self):
        """ Test case - extract unique entity source and host taxonomies
        """
        try:
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName=self.__databaseName,
                collectionName=self.__collectionName,
                cacheFilePath=None,
                useCache=False,
                keyAttribute="entity",
                uniqueAttributes=["rcsb_id"],
                cacheKwargs=None,
                objectLimit=None,
                # selectionQuery={"entity.type": "polymer"},
                selectionQuery=None,
                selectionList=["rcsb_id", "rcsb_entity_source_organism.ncbi_taxonomy_id", "rcsb_entity_host_organism.ncbi_taxonomy_id"],
            )
            eCount = obEx.getCount()
            logger.info("Polymer entity count is %d", eCount)
            taxIdS = set()
            objD = obEx.getObjects()
            for _, eD in objD.items():
                try:
                    for tD in eD["rcsb_entity_source_organism"]:
                        taxIdS.add(tD["ncbi_taxonomy_id"])
                except Exception:
                    pass
                try:
                    for tD in eD["rcsb_entity_host_organism"]:
                        taxIdS.add(tD["ncbi_taxonomy_id"])
                except Exception:
                    pass
            logger.info("Unique taxons %d", len(taxIdS))
            return list(taxIdS)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
