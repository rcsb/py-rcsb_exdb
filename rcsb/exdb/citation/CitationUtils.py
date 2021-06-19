##
# File: CitationExtractor.py
# Date: 19-Feb-2019  jdw
#
# Selected utilities to process and normalize PDB citation data.
#
# Updates:
#
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging

from rcsb.exdb.citation.CitationExtractor import CitationExtractor
from rcsb.utils.io.MarshalUtil import MarshalUtil


logger = logging.getLogger(__name__)


class CitationUtils(object):
    """Utilities to process and normalize PDB citation data."""

    def __init__(self, cfgOb, **kwargs):
        self.__cfgOb = cfgOb
        self.__mU = MarshalUtil()
        #
        self.__ce = self.__getEntryCitations(**kwargs)

    def getCitationEntryCount(self):
        return self.__ce.getEntryCount()

    def __getEntryCitations(self, **kwargs):
        """Extract entry citations"""
        ce = None
        exdbDirPath = kwargs.get("exdbDirPath", None)
        saveKwargs = kwargs.get("cacheKwargs", {"fmt": "pickle"})
        useCache = kwargs.get("useCache", True)
        entryLimit = kwargs.get("entryLimit", True)
        try:
            ce = CitationExtractor(self.__cfgOb, exdbDirPath=exdbDirPath, useCache=useCache, cacheKwargs=saveKwargs, entryLimit=entryLimit)
            eCount = ce.getEntryCount()
            logger.info("Using citation data for %d entries", eCount)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ce
