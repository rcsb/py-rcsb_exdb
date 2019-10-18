#!/bin/bash
# File: TEST-EXDB-CLI-REFSEQ-EXEC.sh
# Date: 17-Oct-2019 jdw
#
# Reference sequence update
#
exdb_exec_cli --mock  --upd_ref_seq --cache_path ../../../CACHE  --config_path ../../mock-data/config/dbload-setup-example.yml --config_name site_info_configuration >& ./test-output/LOGUPDREFSEQ
#
#