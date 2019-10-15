#!/bin/bash
# File: TEST-ETL-EXEC-MOCK.sh
# Date: 3-Sep-2019 jdw
#
# Examples
#
# tree node list load
#
exdb_exec_cli --mock --full --etl_tree_node_lists --rebuild_cache --cache_path ../../../CACHE  --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info_configuration >& ./test-output/LOGTREENODELIST
#
# Chemref load
#
exdb_exec_cli --mock --full --etl_chemref --cache_path ../../../CACHE  --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info_configuration >& ./test-output/LOGCHEMREF
#
# Reference sequence update
#
exdb_exec_cli --mock --upd_ref_seq --cache_path ../../../CACHE  --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info_configuration >& ./test-output/LOGUPDREFSEQ
#
#