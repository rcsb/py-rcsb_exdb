##
# File: GlycanEtlWorkflow.py
# Date: 30-Jun-2021  jdw
#
#  Workflow wrapper  --  Glycan ETL utilities
#
#  Updates:
#   1-Jun-2023 aae Don't back up resources to GitHub during cache update workflows
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os


from rcsb.exdb.branch.GlycanProvider import GlycanProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logger = logging.getLogger(__name__)


class GlycanEtlWorkflow(object):
    def __init__(self, **kwargs):
        """Workflow wrapper  --  Glycan mapping ETL utilities

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

    def updateMatchedIndex(self, backup=True):
        """Update Glycan correspondence index.

        Returns:
            (bool): True for success or False otherwise

        """
        try:
            ok1 = ok2 = False
            gP = GlycanProvider(cachePath=self.__cachePath, useCache=True)
            ok = gP.restore(self.__cfgOb, self.__configName, self.__stashRemotePrefix, useStash=True, useGit=True)
            logger.info("Restore glycan matched identifiers status (%r)", ok)

            ok1 = gP.update(self.__cfgOb, fmt="json", indent=3)
            riD = gP.getIdentifiers()
            logger.info("Matched glycan identifiers (%d)", len(riD))
            #
            if backup:
                ok2 = gP.backup(self.__cfgOb, self.__configName, self.__stashRemotePrefix, useGit=False, useStash=True)
                logger.info("Backup matched glycan identifiers (%r)", ok2)
            else:
                ok2 = True

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return ok1 and ok2
