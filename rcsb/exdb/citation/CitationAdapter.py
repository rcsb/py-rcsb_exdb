##
# File: CitationAdapter.py
# Date: 21-Nov-2019  jdw
#
# Selected utilities to update entry citations in the core_entry collection.
#
# Updates:
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import copy
import logging
from string import capwords

from rcsb.exdb.utils.ObjectAdapterBase import ObjectAdapterBase

logger = logging.getLogger(__name__)


class CitationAdapter(ObjectAdapterBase):
    """Selected utilities to update entry citations in the core_entry collection."""

    def __init__(self, citationReferenceProvider, journalTitleAbbreviationProvider):
        super(CitationAdapter, self).__init__()
        #
        self.__crP = citationReferenceProvider
        self.__jtaP = journalTitleAbbreviationProvider

    def filter(self, obj, **kwargs):
        isTestMode = True
        if isTestMode:
            _, _ = self.__filter(copy.deepcopy(obj))
            return True, obj
        else:
            return self.__filter(obj)

    def __filter(self, obj):
        ok = True
        try:
            rcsbId = obj["rcsb_id"]
            if "citation" in obj:
                for citObj in obj["citation"]:
                    if citObj["id"].upper() != "PRIMARY":
                        continue
                    issn = citObj["journal_id_ISSN"] if "journal_id_ISSN" in citObj else None
                    curAbbrev = citObj["journal_abbrev"] if "journal_abbrev" in citObj else None
                    revAbbrev = self.__updateJournalAbbreviation(rcsbId, issn, curAbbrev)
                    logger.debug("%s: revised: %r current: %r", rcsbId, revAbbrev, curAbbrev)

        except Exception as e:
            ok = False
            logger.exception("Filter adapter failing with error with %s", str(e))
        #
        return ok, obj

    def __updateJournalAbbreviation(self, rcsbId, issn, curAbbrev):
        try:
            revAbbrev = None
            if issn:
                medlineAbbrev = self.__crP.getMedlineJournalAbbreviation(issn)
                # medlineIsoAbbrev = self.__crP.getMedlineJournalIsoAbbreviation(issn)
                crIssn = issn.replace("-", "")
                crTitle = self.__crP.getCrossRefJournalTitle(crIssn)
                #
                revAbbrev = medlineAbbrev
                if not medlineAbbrev and not crTitle:
                    logger.debug("%s: missing information for issn %r curAbbrev %r", rcsbId, issn, curAbbrev)
                    revAbbrev = capwords(curAbbrev.replace(".", " "))
                elif not medlineAbbrev:
                    revAbbrev = self.__jtaP.getJournalAbbreviation(crTitle, usePunctuation=False)
            else:
                if curAbbrev.upper() in ["TO BE PUBLISHED", "IN PREPARATION"]:
                    revAbbrev = "To be published"
                elif curAbbrev.upper().startswith("THESIS"):
                    revAbbrev = "Thesis"
                else:
                    revAbbrev = capwords(curAbbrev.replace(".", " "))
                    logger.debug("%r: missing issn and non-standard abbrev for %r", rcsbId, curAbbrev)

                if not curAbbrev:
                    logger.info("%r: missing issn and journal abbrev", rcsbId)
                #
            logger.debug("%s: revised: %r current: %r", rcsbId, revAbbrev, curAbbrev)
        except Exception as e:
            logger.exception("Failing on %r %r %r with %r", rcsbId, issn, curAbbrev, str(e))

        return revAbbrev
