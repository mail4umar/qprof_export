#!/bin/bash

set -euo pipefail

if [ "$#" -lt  "1" ]; then
    echo "Usage: $0 target_schema"
    exit 1
fi

VSQL=${VSQL_ADMIN_COMMAND:-${VSQL:-vsql}}
TARGET_SCHEMA="$1"
LOCAL_DIRECTORY="${2:-}"

RAND_ID=$(($RANDOM % 100))
RUN_ID="run_$RAND_ID"

regular_tables="v_internal.dc_requests_issued v_internal.dc_query_executions v_internal.dc_explain_plans  v_monitor.query_profiles v_monitor.execution_engine_profiles v_monitor.resource_acquisitions v_monitor.query_consumption v_monitor.host_resources public.collection_info public.collection_events v_internal.dc_slow_events v_monitor.query_events"

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
    schema=${t%%.*}
    table=${t##*.}
    echo "table is $table schema is $schema"
    TABLE_DIRECTORY="${LOCAL_TEMP_DIR}/$table"
    echo "TABLE_DIRECTORY = ${TABLE_DIRECTORY}"
    ROWS_FILE=$LOCAL_TEMP_DIR/${table}_rows.sql
    # Export to Parquet and check if the file is created
    time $VSQL -At -c "export to parquet (directory='$TABLE_DIRECTORY', filename='$table', compression='gzip') as select * from $TARGET_SCHEMA.$table;" > $ROWS_FILE
    rows=$(cat $ROWS_FILE)
    if [ "$rows" -eq 0 ]; then
        echo "Table $t has no data. No Parquet file created."
    else
        $VSQL -a -c "insert into ${TARGET_SCHEMA}.export_events VALUES ('$table', 'export', $rows); commit"
        echo "Table $t exported rows $rows"
        cp ${TABLE_DIRECTORY}/${table}.parquet ${TEMP_BUNDLE}
        
        # Append metadata to JSON file
        echo "\"{\\\"table_type_name\\\": \\\"$table\\\", \\\"table_type_value\\\": \\\"$table\\\", \\\"file_name\\\": \\\"$table.parquet\\\", \\\"exported_rows\\\": $rows}\"," >> $METADATA_FILE
    fi
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
    # Cannot parquet export on integer cols
    # extract(epoch from running_time)::int as running_time
    COLS='transaction_id, statement_id, path_id, path_line_index, path_is_started, path_is_completed, is_executing, extract(epoch from running_time)::float as running_time,  memory_allocated_bytes, read_from_disk_bytes, received_bytes, sent_bytes, path_line'
    ROWS_FILE=$LOCAL_TEMP_DIR/${table}_rows.sql
    time $VSQL -At -c "export to parquet (directory='$TABLE_DIRECTORY', filename='$table', compression='gzip') as select $COLS from $TARGET_SCHEMA.$table" > $ROWS_FILE
    rows=$(cat $ROWS_FILE)
    $VSQL -a -c "insert into ${TARGET_SCHEMA}.export_events VALUES ('$table', 'export', $rows); commit"
    echo "Table $t exported rows $rows"
    cp ${TABLE_DIRECTORY}/${table}.parquet ${TEMP_BUNDLE}
    
    # Append metadata to JSON file
    echo "\"{\\\"table_type_name\\\": \\\"$table\\\", \\\"table_type_value\\\": \\\"$table\\\", \\\"file_name\\\": \\\"$table.parquet\\\", \\\"exported_rows\\\": $rows}\"," >> $METADATA_FILE
done

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
    time $VSQL -At -c "export to parquet (directory='$TABLE_DIRECTORY', filename='$table', compression='gzip') as select $COLS from $TARGET_SCHEMA.$table" > $ROWS_FILE
    rows=$(cat $ROWS_FILE)
    if [ "$rows" -eq 0 ]; then
        echo "Table $t has no data. No Parquet file created."
    else
        $VSQL -a -c "insert into ${TARGET_SCHEMA}.export_events VALUES ('$table', 'export', $rows); commit"
        echo "Table $t exported rows $rows"
        cp ${TABLE_DIRECTORY}/${table}.parquet ${TEMP_BUNDLE}
    
    # Append metadata to JSON file
    echo "\"{\\\"table_type_name\\\": \\\"$table\\\", \\\"table_type_value\\\": \\\"$table\\\", \\\"file_name\\\": \\\"$table.parquet\\\", \\\"exported_rows\\\": $rows}\"," >> $METADATA_FILE
    fi
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
    ROWS_FILE=$LOCAL_TEMP_DIR/${table}_rows.sql
    time $VSQL -At -c "export to parquet (directory='$TABLE_DIRECTORY', filename='$table', compression='gzip') as select * from $TARGET_SCHEMA.$table" > $ROWS_FILE
    rows=$(cat $ROWS_FILE)
    echo "Table $t exported rows $rows"
    cp ${TABLE_DIRECTORY}/${table}.parquet ${TEMP_BUNDLE}
    
    # Append metadata to JSON file
    echo "\"{\\\"table_type_name\\\": \\\"$table\\\", \\\"table_type_value\\\": \\\"$table\\\", \\\"file_name\\\": \\\"$table.parquet\\\", \\\"exported_rows\\\": $rows}\"," >> $METADATA_FILE
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

$VSQL -a -c "select * from ${TARGET_SCHEMA}.export_events"
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
