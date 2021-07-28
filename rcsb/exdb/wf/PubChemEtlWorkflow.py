##
# File: PubChemEtlWorkflow.py
# Date: 28-Jul-2020  jdw
#
#  Workflow wrapper  --  PubChem ETL utilities
#
#  Updates:
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time


from rcsb.exdb.chemref.PubChemEtlWrapper import PubChemEtlWrapper
from rcsb.utils.config.ConfigUtil import ConfigUtil

logger = logging.getLogger(__name__)


class PubChemEtlWorkflow(object):
    def __init__(self, **kwargs):
        """Workflow wrapper  --  PubChem ETL utilities

        Args:
            configPath (str, optional): path to configuration file (default: exdb-config-example.yml)
            configName (str, optional): configuration section name (default: site_info_configuration)
            cachePath (str, optional):  path to cache directory (default: '.')
            stashRemotePrefix (str, optional): file name prefix (channel) applied to remote stash file artifacts (default: None)
        """
        configPath = kwargs.get("configPath", "exdb-config-example.yml")
        self.__configName = kwargs.get("configName", "site_info_configuration")
        mockTopPath = kwargs.get("mockTopPath", None)
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=self.__configName, mockTopPath=mockTopPath)
        #
        self.__cachePath = kwargs.get("cachePath", ".")
        self.__cachePath = os.path.abspath(self.__cachePath)
        self.__stashRemotePrefix = kwargs.get("stashRemotePrefix", None)
        #
        self.__debugFlag = kwargs.get("debugFlag", False)
        if self.__debugFlag:
            logger.setLevel(logging.DEBUG)
        #

    def dump(self):
        """Dump the current object store of PubChem correspondences and data."""
        ok1 = ok2 = ok3 = ok4 = False
        try:
            #  -- Update local chemical indices and  create PubChem mapping index ---
            pcewP = PubChemEtlWrapper(self.__cfgOb, self.__cachePath, stashRemotePrefix=self.__stashRemotePrefix)
            sTime = time.time()
            logger.info("Dumping index data")
            ok1 = pcewP.dump(contentType="index")
            ok2 = pcewP.toStash(contentType="index")
            eTime = time.time()
            logger.info("Dumping index data done in (%.4f seconds)", eTime - sTime)

            sTime = time.time()
            logger.info("Dumping reference data")
            ok3 = pcewP.dump(contentType="data")
            ok4 = pcewP.toStash(contentType="data")
            eTime = time.time()
            logger.info("Dumping data done in (%.4f seconds)", eTime - sTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok1 and ok2 and ok3 and ok4

    def stash(self):
        """Stash the current cache files containing PubChem correspondences and data."""
        ok1 = ok2 = False
        try:
            #  -- Update local chemical indices and  create PubChem mapping index ---
            pcewP = PubChemEtlWrapper(self.__cfgOb, self.__cachePath, stashRemotePrefix=self.__stashRemotePrefix)
            sTime = time.time()
            ok1 = pcewP.toStash(contentType="index")
            eTime = time.time()
            logger.info("Stashing index data done in (%.4f seconds)", eTime - sTime)

            sTime = time.time()
            logger.info("Stashing reference data")
            ok2 = pcewP.toStash(contentType="data")
            eTime = time.time()
            logger.info("Stashing data done in (%.4f seconds)", eTime - sTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok1 and ok2

    def restore(self):
        """Restore the current object store of PubChem correspondences and data from stashed data sets."""
        ok1 = ok2 = False
        numObjData = numObjIndex = 0
        try:
            #  -- Update local chemical indices and  create PubChem mapping index ---
            pcewP = PubChemEtlWrapper(self.__cfgOb, self.__cachePath, stashRemotePrefix=self.__stashRemotePrefix)
            sTime = time.time()
            logger.info("Restoring stashed index data")
            ok1 = pcewP.fromStash(contentType="index")
            numObjIndex = pcewP.reloadDump(contentType="index")
            eTime = time.time()
            logger.info("Restoring index data done in (%.4f seconds)", eTime - sTime)

            sTime = time.time()
            logger.info("Restoring reference data")
            ok2 = pcewP.fromStash(contentType="data")
            numObjData = pcewP.reloadDump(contentType="data")
            eTime = time.time()
            logger.info("Restoring data done in (%.4f seconds)", eTime - sTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok1 and ok2 and numObjData > 1 and numObjIndex > 1

    def updateMatchedIndex(self, **kwargs):
        """Update chemical indices and correspondence matches.

        Args:
            ccUrlTarget (str, optional): target url for chemical component dictionary resource file (default: None=all public)
            birdUrlTarget (str, optional): target url for bird dictionary resource file (cc format) (default: None=all public)
            ccFileNamePrefix (str, optional): index file prefix (default: full)
            rebuildChemIndices (bool, optional): rebuild indices from source (default: False)
            exportPath(str, optional): path to export raw PubChem search results  (default: None)
            numProc(int):  number processors to include in multiprocessing mode (default: 12)

        Returns:
            (bool): True for success or False otherwise

        """
        try:
            ok1 = ok2 = ok3 = False
            #  -- Update local chemical indices and  create PubChem mapping index ---
            ccUrlTarget = kwargs.get("ccUrlTarget", None)
            birdUrlTarget = kwargs.get("birdUrlTarget", None)
            ccFileNamePrefix = kwargs.get("ccFileNamePrefix", "cc-full")
            numProc = kwargs.get("numProc", 12)
            rebuildChemIndices = kwargs.get("rebuildChemIndices", True)
            exportPath = kwargs.get("exportPath", None)
            #
            pcewP = PubChemEtlWrapper(self.__cfgOb, self.__cachePath, stashRemotePrefix=self.__stashRemotePrefix)
            ok1 = pcewP.updateIndex(
                ccUrlTarget=ccUrlTarget,
                birdUrlTarget=birdUrlTarget,
                ccFileNamePrefix=ccFileNamePrefix,
                exportPath=exportPath,
                rebuildChemIndices=rebuildChemIndices,
                numProc=numProc,
            )
            ok2 = pcewP.dump(contentType="index")
            ok3 = pcewP.toStash(contentType="index")
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return ok1 and ok2 and ok3

    def updateMatchedData(self):
        """Update PubChem annotation data for matched correspondences.  Generate and stash
        related identifiers for corresponding components and BIRD chemical definitions.
        """
        try:
            ok1 = ok2 = ok3 = ok4 = ok5 = ok6 = False
            #  --
            pcewP = PubChemEtlWrapper(self.__cfgOb, self.__cachePath, stashRemotePrefix=self.__stashRemotePrefix)
            ok1 = pcewP.updateMatchedData()
            ok2 = pcewP.dump(contentType="data")
            ok3 = pcewP.toStash(contentType="data")
            #
            ok4 = pcewP.updateIdentifiers()
            ok5 = pcewP.dump(contentType="identifiers")
            ok6 = pcewP.toStash(contentType="identifiers")
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return ok1 and ok2 and ok3 and ok4 and ok5 and ok6
