##
# File: LigandNeighborMappingExtractor.py
# Date: 28-Jun-2021  jdw
#
# Utilities to extract ligand neighbor mapping details from the exchange collections.
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


class LigandNeighborMappingExtractor(object):
    """Utilities to extract ligand neighbor mapping details from the exchange collections."""

    def __init__(self, cfgOb):
        self.__cfgOb = cfgOb

    def getLigandNeighbors(self):
        """Extract unique chemical component ids involved in neighbor interactions with each
        polymer and branched entity instance.

        Returns:
            dict: {'entryId_entityId':  [(chem_comp_id, isBound),...], }
        """
        return self.__extractLigandNeighbors()

    def __extractLigandNeighbors(self):
        """Extract unique chemical component ids involved in neighbor interactions with each
        polymer and branched entity instance."""
        try:
            databaseName = "pdbx_core"
            collectionName = "pdbx_core_polymer_entity_instance"
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName=databaseName,
                collectionName=collectionName,
                cacheFilePath=None,
                useCache=False,
                keyAttribute="rcsb_id",
                uniqueAttributes=["rcsb_id"],
                cacheKwargs=None,
                objectLimit=None,
                # selectionQuery={"rcsb_polymer_entity_annotation.type": annotationType},
                selectionQuery=None,
                selectionList=[
                    "rcsb_id",
                    "rcsb_polymer_entity_instance_container_identifiers.entry_id",
                    "rcsb_polymer_entity_instance_container_identifiers.entity_id",
                    "rcsb_polymer_entity_instance_container_identifiers.asym_id",
                    "rcsb_ligand_neighbors.ligand_comp_id",
                    "rcsb_ligand_neighbors.ligand_is_bound",
                ],
            )
            eCount = obEx.getCount()
            logger.info("Total neighbor count (%d)", eCount)
            rD = {}
            objD = obEx.getObjects()
            for _, peiD in objD.items():
                try:
                    entryId = peiD["rcsb_polymer_entity_instance_container_identifiers"]["entry_id"]
                    entityId = peiD["rcsb_polymer_entity_instance_container_identifiers"]["entity_id"]
                    ky = entryId + "_" + entityId
                    for lnD in peiD["rcsb_ligand_neighbors"] if "rcsb_ligand_neighbors" in peiD else []:
                        if "ligand_comp_id" in lnD and "ligand_is_bound" in lnD:
                            rD.setdefault(ky, set()).add((lnD["ligand_comp_id"], lnD["ligand_is_bound"]))
                        else:
                            logger.warning("%s %s missing details lnD %r", entryId, entityId, lnD)
                except Exception as e:
                    logger.exception("Failing with %s", str(e))
            rD = {k: list(v) for k, v in rD.items()}
            logger.info("Unique instance %d", len(rD))
            return rD
        except Exception as e:
            logger.exception("Failing with %s", str(e))
