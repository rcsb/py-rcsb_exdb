##
# File: CitationExtractor.py
# Date: 19-Feb-2019  jdw
#
# Selected utilities to extract citation data from the core_entry exchange database schema.
#
# Updates:
#
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

# import copy
import logging
import os

from rcsb.exdb.utils.ObjectExtractor import ObjectExtractor
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class CitationExtractor(object):
    """Utilities to extract citation related data from the core_entry collection."""

    def __init__(self, cfgOb, **kwargs):
        self.__cfgOb = cfgOb
        self.__resourceName = "MONGO_DB"
        self.__databaseName = "pdbx_core"
        self.__collectionName = "pdbx_core_entry"
        #
        self.__mU = MarshalUtil()
        #
        self.__entryD = self.__rebuildCache(**kwargs)
        self.__idxD = self.__buildIndices(self.__entryD)
        #

    def __rebuildCache(self, **kwargs):
        useCache = kwargs.get("useCache", True)
        dirPath = kwargs.get("exdbDirPath", ".")
        cacheKwargs = kwargs.get("cacheKwargs", {"fmt": "pickle"})
        #
        ext = "pic" if cacheKwargs["fmt"] == "pickle" else "json"
        fn = "entry-citation-extracted-data-cache" + "." + ext
        cacheFilePath = os.path.join(dirPath, fn)

        cD = {"entryD": {}}
        try:
            if useCache and cacheFilePath and os.access(cacheFilePath, os.R_OK):
                logger.info("Using cached entry citation file %s", cacheFilePath)
                cD = self.__mU.doImport(cacheFilePath, **cacheKwargs)
            else:
                entryD = self.__extractCitations()
                cD["entryD"] = entryD
                if cacheFilePath:
                    ok = self.__mU.mkdir(dirPath)
                    ok = self.__mU.doExport(cacheFilePath, cD, **cacheKwargs)
                    logger.info("Saved entry citation results (%d) status %r in %s", len(entryD), ok, cacheFilePath)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return cD["entryD"]

    def __buildIndices(self, entryD):
        """
        Example:
         "entryD": {
                      "5KAL": {
                         "citation": [
                            {
                               "country": "UK",
                               "id": "primary",
                               "journal_abbrev": "Nucleic Acids Res.",
                               "journal_id_ASTM": "NARHAD",
                               "journal_id_CSD": "0389",
                               "journal_id_ISSN": "1362-4962",
                               "journal_volume": "44",
                               "page_first": "10862",
                               "page_last": "10878",
                               "title": "RNA Editing TUTase 1: structural foundation of substrate recognition, complex interactions and drug targeting.",
                               "year": 2016,
                               "pdbx_database_id_DOI": "10.1093/nar/gkw917",
                               "pdbx_database_id_PubMed": 27744351,
                               "rcsb_authors": [
                                  "Rajappa-Titu, L.",
                                  "Suematsu, T.",
                                  "Munoz-Tello, P.",
                                  "Long, M.",
                                  "Demir, O.",
                                  "Cheng, K.J.",
                                  "Stagno, J.R.",
                                  "Luecke, H.",
                                  "Amaro, R.E.",
                                  "Aphasizheva, I.",
                                  "Aphasizhev, R.",
                                  "Thore, S."
                               ]
                            }
                         ],
                         "_entry_id": "5KAL"
                      },
        """
        indD = {}
        missingCitationCount = 0
        missingJournalName = 0
        numPubMed = 0
        numDOI = 0
        numCitations = 0
        mD = {}
        issnD = {}
        missingISSNCount = 0
        missingPubMedCount = 0
        try:
            for entryId, eD in entryD.items():
                cDL = eD["citation"] if "citation" in eD else None
                if cDL:
                    for cD in cDL[:1]:
                        if cD and "journal_abbrev" in cD:
                            indD[cD["journal_abbrev"]] = indD[cD["journal_abbrev"]] + 1 if cD["journal_abbrev"] in indD else 1
                        else:
                            logger.info("Missing journal name in entryId %s %r ", entryId, cD)
                            missingJournalName += 1
                        if cD and "pdbx_database_id_DOI" in cD:
                            numDOI += 1

                        if cD and "pdbx_database_id_PubMed" in cD:
                            numPubMed += 1
                        else:
                            mD[cD["journal_abbrev"]] = mD[cD["journal_abbrev"]] + 1 if cD["journal_abbrev"] in mD else 1
                            missingPubMedCount += 1

                        if "journal_id_ISSN" in cD and len(cD["journal_id_ISSN"]) > 7:
                            issnD[cD["journal_id_ISSN"]] = issnD[cD["journal_id_ISSN"]] + 1 if cD["journal_id_ISSN"] in issnD else 1
                        else:
                            missingISSNCount += 1

                        if cD:
                            numCitations += 1
                else:
                    missingCitationCount += 1
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        logger.info("Number of citatons %d", numCitations)
        logger.info("Number of PubMed ids %d", numPubMed)
        logger.info("Number of DOIs %d", numDOI)
        logger.info("No citation category count %d missing journal name %d", missingCitationCount, missingJournalName)
        #
        logger.info("Journal index name length %d", len(indD))
        # logger.info("Journal name length %r",indD.items())
        #
        logger.info("Missing pubmed index length %d", len(mD))
        logger.info("Missing pubmed length %d", missingPubMedCount)
        logger.info("Missing PubMed %r", mD.items())
        #
        logger.info("ISSN dictionary length %d", len(issnD))
        logger.info("ISSN missing length %d", missingISSNCount)
        #
        return indD

    def getEntryCount(self):
        return len(self.__entryD)

    def __extractCitations(self):
        """Test case - extract unique entity source and host taxonomies"""
        try:
            obEx = ObjectExtractor(
                self.__cfgOb,
                databaseName=self.__databaseName,
                collectionName=self.__collectionName,
                cacheFilePath=None,
                useCache=False,
                keyAttribute="entry",
                uniqueAttributes=["rcsb_id"],
                cacheKwargs=None,
                objectLimit=None,
                selectionQuery={},
                selectionList=["rcsb_id", "citation"],
            )
            eCount = obEx.getCount()
            logger.info("Entry count is %d", eCount)
            objD = obEx.getObjects()
            # for ky, eD in objD.items():
            #    logger.info("%s: %r", ky, eD)
            return objD
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return {}
