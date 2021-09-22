##
# File: EntryInfoEtlWorkflow.py
# Date: 22-Sep-2021  jdw
#
#  Workflow wrapper  --  Entry-level annotations extracted from ExDB
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


from rcsb.exdb.entry.EntryInfoProvider import EntryInfoProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logger = logging.getLogger(__name__)


class EntryInfoEtlWorkflow(object):
    def __init__(self, **kwargs):
        """Workflow wrapper  --  extract selected entry-level annotations from ExDb

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

    def update(self, backup=True):
        """Update extraction of selected entry-level annotations from ExDB.

        Returns:
            (bool): True for success or False otherwise

        """
        try:
            ok = False
            eiP = EntryInfoProvider(cachePath=self.__cachePath, useCache=True)
            eiP.update(self.__cfgOb, fmt="json", indent=3)
            #
            if backup:
                ok = eiP.backup(self.__cfgOb, self.__configName, self.__stashRemotePrefix, useGit=True, useStash=True)
                logger.info("Backup entry-level annotations (%r)", ok)
            else:
                ok = True

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return ok
