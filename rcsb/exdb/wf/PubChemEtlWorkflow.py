##
# File: PubChemEtlWorkflow.py
# Date: 28-Jul-2020  jdw
#
#  Workflow wrapper  --  PubChem ETL utilities
#
#  Updates:
#  13-Mar-2023 aae Updates to use multiprocess count, disable git stash testing
#   1-Jun-2023 aae Don't back up resources to GitHub during cache update workflows
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

    def dump(self, **kwargs):
        """Dump the current object store of PubChem correspondences and data.

        Args:
            useStash (bool):  should stash (Buildlocker) be updated? (default: True)
            useGit (bool):  should stash (GitHub) be updated? (default: True)

        Returns:
            (bool): True for success or False otherwise

        """
        ok1 = ok2 = ok3 = ok4 = False
        try:
            useStash = kwargs.get("useStash", True)
            useGit = kwargs.get("useGit", True)  # Revisit stashing in GitHub as file timestamp will always cause a commit
            #  -- Update local chemical indices and  create PubChem mapping index ---
            pcewP = PubChemEtlWrapper(self.__cfgOb, self.__cachePath, stashRemotePrefix=self.__stashRemotePrefix)
            sTime = time.time()
            logger.info("Dumping index data")
            ok1 = pcewP.dump(contentType="index")
            eTime = time.time()
            logger.info("Dumping index data done in (%.4f seconds)", eTime - sTime)
            if useGit or useStash:
                sTime = time.time()
                logger.info("Stashing index data")
                ok2 = pcewP.toStash(contentType="index", useStash=useStash, useGit=useGit)
                eTime = time.time()
                logger.info("Stashing index data done in (%.4f seconds)", eTime - sTime)
            else:
                ok2 = True

            sTime = time.time()
            logger.info("Dumping reference data")
            ok3 = pcewP.dump(contentType="data")
            if useGit or useStash:
                sTime = time.time()
                logger.info("Stashing reference data")
                ok4 = pcewP.toStash(contentType="data", useStash=useStash, useGit=useGit)
                eTime = time.time()
                logger.info("Stashing reference data done in (%.4f seconds)", eTime - sTime)
            else:
                ok4 = True
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
            ok1 = pcewP. toStash(contentType="index")
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
            exportPath (str, optional): path to export raw PubChem search results  (default: None)
            numProcChemComp (int, optional):  number processors to include in multiprocessing mode (default: 8)
            numProc (int, optional):  number processors to include in multiprocessing mode (default: 2)
            useStash (bool, optional):  should stash (Buildlocker) be updated? (default: True)
            useGit (bool, optional):  should stash (GitHub) be updated? (default: True)

        Returns:
            (bool): True for success or False otherwise

        """
        try:
            ok1 = ok2 = ok3 = False
            #  -- Update local chemical indices and  create PubChem mapping index ---
            ccUrlTarget = kwargs.get("ccUrlTarget", None)
            birdUrlTarget = kwargs.get("birdUrlTarget", None)
            ccFileNamePrefix = kwargs.get("ccFileNamePrefix", "cc-full")
            numProcChemComp = kwargs.get("numProcChemComp", 8)
            numProc = kwargs.get("numProc", 2)
            rebuildChemIndices = kwargs.get("rebuildChemIndices", True)
            exportPath = kwargs.get("exportPath", None)
            useStash = kwargs.get("useStash", True)
            useGit = kwargs.get("useGit", False)
            #
            pcewP = PubChemEtlWrapper(self.__cfgOb, self.__cachePath, stashRemotePrefix=self.__stashRemotePrefix)
            ok1 = pcewP.updateIndex(
                ccUrlTarget=ccUrlTarget,
                birdUrlTarget=birdUrlTarget,
                ccFileNamePrefix=ccFileNamePrefix,
                exportPath=exportPath,
                rebuildChemIndices=rebuildChemIndices,
                numProcChemComp=numProcChemComp,
                numProc=numProc,
            )
            logger.info("updateIndex completed with status %r", ok1)
            ok2 = pcewP.dump(contentType="index")
            logger.info("dump completed with status %r", ok2)
            if useGit or useStash:
                ok3 = pcewP.toStash(contentType="index", useStash=useStash, useGit=useGit)
                logger.info("toStash completed with status %r", ok3)
            else:
                ok3 = True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return ok1 and ok2 and ok3

    def updateMatchedData(self, **kwargs):
        """Update PubChem annotation data for matched correspondences.  Generate and stash
        related identifiers for corresponding components and BIRD chemical definitions.

        Args:
            numProc(int):  number processors to include in multiprocessing mode (default: 2)
            useStash(bool):  should stash (Buildlocker) be updated? (default: True)
            useGit(bool):  should stash (GitHub) be updated? (default: True)

        Returns:
            (bool): True for success or False otherwise
        """
        try:
            ok1 = ok2 = ok3 = ok4 = ok5 = ok6 = False
            #  --
            numProc = kwargs.get("numProc", 2)
            useStash = kwargs.get("useStash", True)
            useGit = kwargs.get("useGit", False)
            #
            pcewP = PubChemEtlWrapper(self.__cfgOb, self.__cachePath, stashRemotePrefix=self.__stashRemotePrefix)
            ok1 = pcewP.updateMatchedData(numProc=numProc)
            logger.info("PubChemEtlWrapper.updateMatchedData completed with status %r", ok1)
            ok2 = pcewP.dump(contentType="data")
            logger.info("PubChemEtlWrapper.dump 'data' completed with status %r", ok2)
            if useGit or useStash:
                ok3 = pcewP.toStash(contentType="data", useStash=useStash, useGit=useGit)
                logger.info("PubChemEtlWrapper.toStash 'data' completed with status %r", ok3)
            else:
                ok3 = True
            #
            ok4 = pcewP.updateIdentifiers()
            logger.info("PubChemEtlWrapper.updateIdentifiers completed with status %r", ok4)
            ok5 = pcewP.dump(contentType="identifiers")
            logger.info("PubChemEtlWrapper.dump 'identifiers' completed with status %r", ok5)
            if useGit or useStash:
                ok6 = pcewP.toStash(contentType="identifiers", useStash=useStash, useGit=useGit)
                logger.info("PubChemEtlWrapper.toStash 'identifiers' completed with status %r", ok6)
            else:
                ok6 = True
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return ok1 and ok2 and ok3 and ok4 and ok5 and ok6
