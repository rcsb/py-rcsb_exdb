##
# File: AnnotationExtractor.py
# Date: 15-Oct-2019  jdw
#
# Utilities to extract GO identifiers from the core polymer entity collection.
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


class AnnotationExtractor(object):
    """  Utilities to extract GO details from the core entity collection.
    """

    def __init__(self, cfgOb):
        self.__cfgOb = cfgOb
        self.__databaseName = "pdbx_core"
        self.__collectionName = "pdbx_core_polymer_entity"

    def getUniqueIdentifiers(self, annotationType):
        idL = self.__extractEntityAnnotationIdentifiers(annotationType)
        return idL

    def __extractEntityAnnotationIdentifiers(self, annotationType):
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
                # selectionQuery={"rcsb_polymer_entity_annotation.type": annotationType},
                selectionQuery=None,
                selectionList=["rcsb_id", "rcsb_polymer_entity_annotation.annotation_id", "rcsb_polymer_entity_annotation.type"],
            )
            eCount = obEx.getCount()
            logger.info("For type %r polymer entity annotation object count is %d", annotationType, eCount)
            idS = set()
            objD = obEx.getObjects()
            for _, eD in objD.items():
                try:
                    for tD in eD["rcsb_polymer_entity_annotation"]:
                        if tD["type"] == annotationType:
                            idS.add(tD["annotation_id"])
                except Exception:
                    pass
            logger.info("Unique identifiers %d", len(idS))
            return list(idS)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
