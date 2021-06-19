##
# File: AnnotationExtractor.py
# Date: 15-Oct-2019  jdw
#
# Utilities to extract selected annotation details from the exchange collections.
#
# Currently, used to established covered annotations for scoping tree brower displays
# for expansive annotation hierarchies.
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

logger = logging.getLogger(__name__)


class AnnotationExtractor(object):
    """Utilities to extract selected annotation details from the exchange collections."""

    def __init__(self, cfgOb):
        self.__cfgOb = cfgOb

    def getUniqueIdentifiers(self, annotationType):
        """Extract unique rcsb_polymer_entity_annotation ids for the input annotation type.

        Args:
            annotationType (str): a value of rcsb_polymer_entity_annotation.type

        Returns:
            list: unique list of identifiers of annotationType
        """
        idL = self.__extractEntityAnnotationIdentifiers(annotationType)
        return idL

    def __extractEntityAnnotationIdentifiers(self, annotationType):
        """Extract unique rcsb_polymer_entity_annotation ids for the input annotation type."""
        try:
            databaseName = "pdbx_core"
            collectionName = "pdbx_core_polymer_entity"
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName=databaseName,
                collectionName=collectionName,
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
