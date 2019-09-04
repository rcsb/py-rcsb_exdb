##
# File: ExDbExec.py
# Date: 22-Apr-2019  jdw
#
#  Execution wrapper  --  for extract and load operations -
#
#  Updates:
#   4-Sep-2019 jdw add Tree and Drugbank loaders
#
##
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import argparse
import logging
import os
import sys

from rcsb.db.helpers.DictMethodResourceProvider import DictMethodResourceProvider
from rcsb.db.mongo.DocumentLoader import DocumentLoader
from rcsb.db.utils.TimeUtil import TimeUtil
from rcsb.exdb.chemref.ChemRefEtlWorker import ChemRefEtlWorker
from rcsb.exdb.tree.TreeNodeListWorker import TreeNodeListWorker
from rcsb.utils.config.ConfigUtil import ConfigUtil

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


def loadStatus(statusList, cfgOb, cachePath, readBackCheck=True):
    sectionName = "data_exchange_configuration"
    dl = DocumentLoader(cfgOb, cachePath, "MONGO_DB", numProc=2, chunkSize=2, documentLimit=None, verbose=False, readBackCheck=readBackCheck)
    #
    databaseName = cfgOb.get("DATABASE_NAME", sectionName=sectionName)
    collectionName = cfgOb.get("COLLECTION_UPDATE_STATUS", sectionName=sectionName)
    ok = dl.load(databaseName, collectionName, loadType="append", documentList=statusList, indexAttributeList=["update_id", "database_name", "object_name"], keyNames=None)
    return ok


def buildResourceCache(cfgOb, configName, cachePath, rebuildCache=False):
    """Generate and cache resource dependencies.
    """
    ret = False
    try:
        rp = DictMethodResourceProvider(cfgOb, configName=configName, cachePath=cachePath)
        ret = rp.cacheResources(useCache=not rebuildCache)
    except Exception as e:
        logger.exception("Failing with %s", str(e))
    return ret


def main():
    parser = argparse.ArgumentParser()
    #
    defaultConfigName = "site_info_configuration"
    parser.add_argument("--data_set_id", default=None, help="Data set identifier (default= 2019_14 for current week)")
    parser.add_argument("--full", default=True, action="store_true", help="Fresh full load in a new tables/collections (Default)")
    parser.add_argument("--etl_chemref", default=False, action="store_true", help="ETL integrated chemical reference data")
    parser.add_argument("--etl_tree_node_lists", default=False, action="store_true", help="ETL tree node lists")
    parser.add_argument("--config_path", default=None, help="Path to configuration options file")
    parser.add_argument("--config_name", default=defaultConfigName, help="Configuration section name")
    parser.add_argument("--db_type", default="mongo", help="Database server type (default=mongo)")
    parser.add_argument("--read_back_check", default=False, action="store_true", help="Perform read back check on all documents")
    parser.add_argument("--num_proc", default=2, help="Number of processes to execute (default=2)")
    parser.add_argument("--chunk_size", default=10, help="Number of files loaded per process")
    parser.add_argument("--document_limit", default=None, help="Load document limit for testing")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on verbose logging")
    parser.add_argument("--mock", default=False, action="store_true", help="Use MOCK repository configuration for testing")
    parser.add_argument("--cache_path", default=None, help="Top cache path for external and local resource files")
    parser.add_argument("--rebuild_cache", default=False, action="store_true", help="Rebuild cached files from remote resources")
    #
    #
    args = parser.parse_args()
    #
    debugFlag = args.debug
    if debugFlag:
        logger.setLevel(logging.DEBUG)
    # ----------------------- - ----------------------- - ----------------------- - ----------------------- - ----------------------- -
    #                                       Configuration Details
    configPath = args.config_path
    configName = args.config_name
    rebuildCache = args.rebuild_cache
    useCache = not args.rebuild_cache
    if not configPath:
        configPath = os.getenv("DBLOAD_CONFIG_PATH", None)
    try:
        if os.access(configPath, os.R_OK):
            os.environ["DBLOAD_CONFIG_PATH"] = configPath
            logger.info("Using configuation path %s (%s)", configPath, configName)
        else:
            logger.error("Missing or access issue with config file %r", configPath)
            exit(1)
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data") if args.mock else None
        cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
    except Exception as e:
        logger.error("Missing or access issue with config file %r with %s", configPath, str(e))
        exit(1)

    #
    try:
        readBackCheck = args.read_back_check
        tU = TimeUtil()
        dataSetId = args.data_set_id if args.data_set_id else tU.getCurrentWeekSignature()
        numProc = int(args.num_proc)
        chunkSize = int(args.chunk_size)
        documentLimit = int(args.document_limit) if args.document_limit else None
        loadType = "full" if args.full else "replace"
        cachePath = args.cache_path if args.cache_path else "."

        if args.db_type != "mongo":
            logger.error("Unsupported database server type %s", args.db_type)
    except Exception as e:
        logger.exception("Argument processing problem %s", str(e))
        parser.print_help(sys.stderr)
        exit(1)
    # ----------------------- - ----------------------- - ----------------------- - ----------------------- - ----------------------- -
    ##
    #  Rebuild or check resource cache
    ok = buildResourceCache(cfgOb, configName, cachePath, rebuildCache=rebuildCache)
    if not ok:
        logger.error("Cache rebuild or check failure (rebuild %r) %r", rebuildCache, cachePath)
        exit(1)
    # if not useCache:
    #    buildResourceCache(cfgOb, configName, cachePath, rebuildCache=True)
    #
    if args.db_type == "mongo":
        if args.etl_tree_node_lists:
            rhw = TreeNodeListWorker(
                cfgOb, cachePath, numProc=numProc, chunkSize=chunkSize, documentLimit=documentLimit, verbose=debugFlag, readBackCheck=readBackCheck, useCache=useCache
            )
            ok = rhw.load(dataSetId, loadType=loadType)
            okS = loadStatus(rhw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)

        if args.etl_chemref:
            crw = ChemRefEtlWorker(
                cfgOb, cachePath, numProc=numProc, chunkSize=chunkSize, documentLimit=documentLimit, verbose=debugFlag, readBackCheck=readBackCheck, useCache=useCache
            )
            ok = crw.load(dataSetId, extResource="DrugBank", loadType=loadType)
            okS = loadStatus(crw.getLoadStatus(), cfgOb, cachePath, readBackCheck=readBackCheck)

        logger.info("Operation completed with status %r " % ok and okS)


if __name__ == "__main__":
    main()
