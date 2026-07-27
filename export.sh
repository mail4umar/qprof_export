#!/bin/bash
# export.sh

# called from profile_queries.sh as part of the qprof chain

# Modified by Carl Gerstle - 03/13/2026 Added functions to retry cp ${TABLE_DIRECTORY}/${table}.parquet ${TEMP_BUNDLE} and the export itself
#					Also (per Jim Knicely in 2021!) added OVER() to export to parquet statements to remove internode dependencies
#					And then added a retry loop for the export itself
#					And modified ifDirExists to use overwrite instead of append
#					Placed much more functionality in export_to_parquet_with_retries()
# Modified by Carl Gerstle - 03/12/2026 Modifed export to parquet to add ifDirExists='append' to the parameter set as 10-20% of runs are falsely
#					claiming that the parquet target directory exists... when it does not. 
# Modified by Carl Gerstle - 03/09/2026 v_monitor.query_plan_profiles.epoch now being exported as a decimal value scaled by 1000 to be running_time_ms
#					where this is normally scaled in uS. Jason Slaunwhite had exported as a float but this fails below 100 uS
# Modified by Carl Gerstle - 03/05/2026 add (many) missing tables addressed by qprof UI but not this script

set -euo pipefail

cp_parquet_to_temp_bundle(){
  # sleep up to 10 seconds waiting for the requested parquet file to be ready to copy to ${TEMP_BUNDLE}
  max_wait_seconds=10
  wait_increment=1
  waited=0
  waited_seconds=0
  while [ $waited_seconds -le $max_wait_seconds ]; do
    if [ -f ${TABLE_DIRECTORY}/${table}.parquet ]; then
      # we see the source file as existing, should be OK to copy, break out of wait loop
      break
    else
      sleep $wait_increment
      let "waited_seconds = waited_seconds + $wait_increment"
      let "waited = waited_seconds + 1"
      echo "`date +"%Y-%m-%d %H:%M:%S"` Wait #$waited for copy of ${TABLE_DIRECTORY}/${table}.parquet to complete"
    fi
  done
  # run the cp command and if it fails - allow mainline code to act accordingly
  cp ${TABLE_DIRECTORY}/${table}.parquet ${TEMP_BUNDLE}

}
# end of function cp_parquet_to_temp_bundle

export_to_parquet_with_retries(){
  # sleep up to 10 seconds waiting for the requested parquet file to be ready to copy to ${TEMP_BUNDLE}
  max_wait_seconds=10
  wait_increment=1
  waited=0
  waited_seconds=0
  while [ $waited_seconds -le $max_wait_seconds ]; do
    # The following queries use $COLS as set from the caller which may EITHER be a specific set of columns OR (literal) "*"
    # We first actually perform a test select to get a row count. If the row count is zero we skip
    #    insert into export_events
    #    cp ${TABLE_DIRECTORY}/${table}.parquet ${TEMP_BUNDLE}
    #    Append metadata to JSON file

    rows=$($VSQL -Atc "select count(*) from $TARGET_SCHEMA.$table;")
    if [ $rows -eq 0 ]; then
      echo "Table $t has no data. No Parquet file created."
      break # return only went to the top of the while loop! # done with the function call if there is no data to export
    else
      $VSQL -At -c "export to parquet (directory='$TABLE_DIRECTORY', ifDirExists='overwrite', filename='$table', compression='gzip') OVER() as select $COLS from $TARGET_SCHEMA.$table;" > $ROWS_FILE
      if [ -f ${TABLE_DIRECTORY}/${table}.parquet ]; then
        # we see the source file as existing, should be OK to continue normal processing:
        $VSQL -a -c "insert into ${TARGET_SCHEMA}.export_events VALUES ('$table', 'export', $rows); commit"
        echo "Table $t exported rows $rows"
        cp ${TABLE_DIRECTORY}/${table}.parquet ${TEMP_BUNDLE}

        # Append metadata to JSON file
        echo "\"{\\\"table_type_name\\\": \\\"$table\\\", \\\"table_type_value\\\": \\\"$table\\\", \\\"file_name\\\": \\\"$table.parquet\\\", \\\"exported_rows\\\": $rows}\"," >> $METADATA_FILE
        break
      else
        sleep $wait_increment
        let "waited_seconds = waited_seconds + $wait_increment"
        let "waited = waited_seconds + 1"
        echo "`date +"%Y-%m-%d %H:%M:%S"` Wait #$waited parquet export to ${TABLE_DIRECTORY}/${table}.parquet to complete"
        # We can't retry to $TABLE_DIRECTORY if it still exists - remove it, THEN try the export again.
        rm -rf $TABLE_DIRECTORY
      fi
    fi
  done
}
# end of function export_to_parquet_with_retries

if [ "$#" -lt  "1" ]; then
    echo "Usage: $0 target_schema"
    exit 1
fi

VSQL=${VSQL_ADMIN_COMMAND:-${VSQL:-vsql}}
TARGET_SCHEMA="$1"
LOCAL_DIRECTORY="${2:-}"

# support (virtual) output_debug_linenumbers, If a 3rd parameter appears makes output_debug_linenumbers TRUE, otherwise FALSE
[[ -n "${3:-}" ]] && output_debug_linenumbers=TRUE || output_debug_linenumbers=FALSE

if [ $output_debug_linenumbers = TRUE ]; then
  set -x
  PS4='+$0 +${LINENO}: `date +"%Y-%m-%d %H:%M:%S.%N" | rev | cut -c 7- | rev` '
fi

RAND_ID=$(($RANDOM % 100))
RUN_ID="run_$RAND_ID"

#regular_tables="v_internal.dc_requests_issued v_internal.dc_query_executions v_internal.dc_explain_plans v_monitor.query_profiles v_monitor.execution_engine_profiles v_monitor.resource_acquisitions v_monitor.query_consumption v_monitor.host_resources public.collection_info public.collection_events v_internal.dc_slow_events v_monitor.query_events"
# New extended list as of 2026-03-05 with the following tables NOT listed as they have special-case handling below:
# v_monitor.query_plan_profiles
# v_monitor.resource_pool_status
regular_tables="public.collection_events public.collection_info v_internal.dc_lock_attempts v_internal.dc_plan_activities v_internal.dc_plan_resources v_internal.dc_plan_step_properties v_internal.dc_plan_steps v_internal.dc_explain_plans v_internal.dc_query_executions v_internal.dc_requests_issued v_internal.dc_scan_events v_internal.dc_slow_events v_monitor.execution_engine_profiles v_monitor.host_resources v_monitor.projection_storage v_monitor.projection_usage v_monitor.query_consumption v_monitor.query_events v_monitor.query_profiles v_monitor.resource_acquisitions v_monitor.storage_containers"

# If LOCAL_DIRECTORY is not provided, set it to a default value or handle it accordingly
if [ -z "$LOCAL_DIRECTORY" ]; then
    echo "LOCAL_DIRECTORY not provided. Using current value of $PWD ."
    LOCAL_DIRECTORY=$PWD
fi

LOCAL_TEMP_DIR="${LOCAL_DIRECTORY}/temp_${RAND_ID}"
TEMP_BUNDLE="${LOCAL_TEMP_DIR}/bundle"
rm -rf $LOCAL_TEMP_DIR
mkdir -p $LOCAL_TEMP_DIR
mkdir -p $TEMP_BUNDLE

echo "---------------------------------------------------"
echo "RUN ID  is $RUN_ID"
echo "Working with temp dir $LOCAL_TEMP_DIR"
echo "TARGET_SCHEMA is $TARGET_SCHEMA"
echo "---------------------------------------------------"

$VSQL -a -c "create table if not exists ${TARGET_SCHEMA}.export_events(table_name varchar (256), operation varchar(128), row_count int);"
$VSQL -a -c "truncate table ${TARGET_SCHEMA}.export_events;"

# Initialize an empty metadata JSON file
METADATA_FILE="${TEMP_BUNDLE}/metadata.json"
# The version refers to the BundleVersion from VerticaPy.performance.vertica.collection_tables.py
echo "{\"version\": \"2\", \"tables\": [" > $METADATA_FILE

for t in $regular_tables
do
    echo "++++++++++++++++++++++++++++++++++++++++"
    echo "Considering table for export  $t"
    schema=${t%%.*}¦
    table=${t##*.}
    echo "table is $table schema is $schema"
    TABLE_DIRECTORY="${LOCAL_TEMP_DIR}/$table"
    echo "TABLE_DIRECTORY = ${TABLE_DIRECTORY}"
    ROWS_FILE=$LOCAL_TEMP_DIR/${table}_rows.sql
    # Export to Parquet and check if the file is created
    COLS="*" # will select all columns for the parquet export
    export_to_parquet_with_retries # also performs insert into export_events, calls cp_parquet_to_temp_bundle, appends metadata to JSON file
done

echo "Done with regular tables, considering special table query_plan_profiles"

for t in "v_monitor.query_plan_profiles"
do
    echo "-----------------------------------------------------"
    echo "t is $t"
    schema=${t%%.*}
    table=${t##*.}
    echo "table is $table schema is $schema"
    TABLE_DIRECTORY="${LOCAL_TEMP_DIR}/$table"
    echo "TABLE_DIRECTORY = ${TABLE_DIRECTORY}"
    # Cannot parquet export on interval cols (Jason had said "int" when he clearly meant interval!)
    # extract(epoch from running_time) *1000 as running_time_ms - will re-scale down to uS during COPY
    COLS='transaction_id, statement_id, path_id, path_line_index, path_is_started, path_is_completed, is_executing, extract(epoch from running_time) * 1000 as running_time_ms,  memory_allocated_bytes, read_from_disk_bytes, received_bytes, sent_bytes, path_line'
    ROWS_FILE=$LOCAL_TEMP_DIR/${table}_rows.sql
    export_to_parquet_with_retries # also performs insert into export_events, calls cp_parquet_to_temp_bundle, appends metadata to JSON file
done

echo; echo "Done with regular tables, considering special table resource_pool_status"

for t in "v_monitor.resource_pool_status"
do
    echo "-----------------------------------------------------"
    echo "t is $t"
    schema=${t%%.*}
    table=${t##*.}
    echo "table is $table schema is $schema"
    TABLE_DIRECTORY="${LOCAL_TEMP_DIR}/$table"
    echo "TABLE_DIRECTORY = ${TABLE_DIRECTORY}"
    # queue_timeout_in_seconds is already the int version of interval col queue_timeout
    # So no need to do a fancy extract, we can just copy the int version
    COLS='node_name,pool_oid,pool_name,is_internal,memory_size_kb,memory_size_actual_kb,memory_inuse_kb,general_memory_borrowed_kb,queueing_threshold_kb,max_memory_size_kb,max_query_memory_size_kb,running_query_count,planned_concurrency,max_concurrency,is_standalone,queue_timeout_in_seconds as queue_timeout,queue_timeout_in_seconds,execution_parallelism,priority,runtime_priority,runtime_priority_threshold,runtimecap_in_seconds,single_initiator,query_budget_kb,cpu_affinity_set,cpu_affinity_mask,cpu_affinity_mode'
    ROWS_FILE=$LOCAL_TEMP_DIR/${table}_rows.sql
    export_to_parquet_with_retries # also performs insert into export_events, calls cp_parquet_to_temp_bundle, appends metadata to JSON file
done

for t in "${TARGET_SCHEMA}.export_events"
do
    echo "-----------------------------------------------------"
    echo "t is $t"
    schema=${t%%.*}
    table=${t##*.}
    echo "table is $table schema is $schema"
    TABLE_DIRECTORY="${LOCAL_TEMP_DIR}/$table"
    echo "TABLE_DIRECTORY = ${TABLE_DIRECTORY}"
    COLS="*" # will select all columns for the parquet export
    ROWS_FILE=$LOCAL_TEMP_DIR/${table}_rows.sql
    export_to_parquet_with_retries # also performs insert into export_events, calls cp_parquet_to_temp_bundle, appends metadata to JSON file
done

# Remove the last comma and close the JSON array
sed -i '$ s/,$//' $METADATA_FILE
echo "]}" >> $METADATA_FILE

WORKING_DIR=$PWD
pushd ${TEMP_BUNDLE}
# If there is a tar file creation failure, this will cause the entire script to fail and stop due to the "-o pipefail" command at the top.
tar cvfz "${WORKING_DIR}/${TARGET_SCHEMA}.tar.gz" *.parquet metadata.json
popd
echo
ls -lrth ${TARGET_SCHEMA}.tar.gz

$VSQL -a -c "select * from ${TARGET_SCHEMA}.export_events order by table_name"
# If you want to keep the files, comment out the line directly below.
rm -rf $LOCAL_TEMP_DIR

echo "TARGET_SCHEMA was $TARGET_SCHEMA"
echo "---------------------------------------------------"
echo "RUN ID  was $RUN_ID"
echo "Worked with temp dir $LOCAL_TEMP_DIR"
echo "---------------------------------------------------"
echo "Your final TAR file is: $TARGET_SCHEMA.tar.gz"
echo "---------------------------------------------------"
echo "DONE"
