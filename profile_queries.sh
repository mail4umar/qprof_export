#!/bin/bash
# profile_queries.sh
# Based off Jason Slaunwhite code for exporting query profiling information

# Modified by Carl Gerstle - 03/17/2026 add list of critical tables to check for zero rows in TARGET_SCHEMA
# Modified by Carl Gerstle - 03/04/2026 add (many) missing tables addressed by qprof UI but not this script
#					now allowing --ignore_retention_issues to be requested
# Modified by Carl Gerstle - 02/24/2026 to get --transactions to actually work and allow use of queries w/o PROFILE keyword
#					also, now accepting an option to pre-populate a EEP (execution_engine_profiles) and QPP (query_plan_profiles)
#					over a date/time range to seriously improve multi-query and multi-groups-of-queries qprof performance
#                                       Output the final summary to a file and cat the file to prevent vsql pagination.
# MODIFIED BY UMAR FAROOQ GHUMMAN - 7/24/2024
# This script profiles queries and optionally creates a schema if not provided

##############################
# USER CONFIGURATION SECTION #
##############################

# Database credentials for query execution
QUERY_USER="release"
QUERY_USER_PASSWORD='"your_password_here"'
# or if there is no password:
QUERY_USER_PASSWORD='""'

# Database credentials for administrative operations
ADMIN_USER="release"
ADMIN_PASSWORD='"your_password_here"'
# or if there is no password:
ADMIN_PASSWORD='""'

# Local directory for temporarily storing parquet files
LOCAL_DIRECTORY="$PWD"

# For tracking
PROJECT_NAME="test"
CUSTOMER_NAME="XYZ"
###############################
# END USER CONFIGURATION      #
###############################

set -euo pipefail

# Adding input options

usage() {
    echo "\
$0 accepts inputs as follows:

Usage: $0

    Options:
    -j, --job_file          [Required if --transactions is not provided] e.g., foo.txt
    -s, --target_schema     [Optional] e.g., test_01 (The schema name should be unused)
    -t, --transactions      [Required if --job_file is not provided] (txn_id,stmt_id) or ((txn_id1,stmt_id1),(txn_id2,stmt_id2),...)
                             Profile one or multiple transactions and statements using tuples
    --ignore_retention_issues [Optional] if presented, then do not require that we HAVE all data in all scanned tables, but do report on it
    --no_profile_keyword    [Optional] if presented, then do not require existing queries to HAVE a PROFILE keyword, it is up to the user
                             to have assured that SaveDCEEProfileThresholdUS is low enough that queries HAVE profile data automatically
    --pre_load_large_dc_tables [Optional] if presented, scan execution_engine_profiles and query_plan_profiles to create ROS tables for faster processing
                                          of query groups later on in the same overall superset of queries
    --pre_load_start_timestamp [Optional] if --pre_load_large_dc_tables presented, quoted start_timestamp of first query in all being profiled (approximate) ala "2026-02-25 10:23:12"
    --pre_load_end_timestamp   [Optional] if --pre_load_large_dc_tables presented, quoted end_timestamp of last query in all being profiled (approximate) ala "2026-02-26 12:34:56"

    Help:
    -h, --help              [Help] Show this help info

For example, with a single tuple:
    $0 \
--target_schema test_01 \
--transactions \"(411323123131,1)\"

For example, with multiple tuples:
    $0 \
--target_schema test_01 \
--transactions  \"((45035996273713358,1),(45035996273712247,1))\"

For example, with a job file:
    $0 \
--job_file foo.txt"
}

# end of function usage()

# List of parameter flags
TEMP=$(getopt -o j:s:t:h --long job_file:,target_schema:,transactions:,no_profile_keyword,pre_load_large_dc_tables,pre_load_start_timestamp:,pre_load_end_timestamp:,ignore_retention_issues,output_debug_linenumbers,odbln,help -n 'ERROR: $0' -- "$@")

if [ $? != 0 ]; then usage; exit; fi
eval set -- "$TEMP"

# Bind each parameter NAME
job_file=""
target_schema=""
transactions=""
ignore_retention_issues="FALSE"
no_profile_keyword="FALSE"
pre_load_large_dc_tables=FALSE
pre_load_start_timestamp=""
pre_load_end_timestamp=""
output_debug_linenumbers="FALSE"
odbln="FALSE"

while true; do
    case "$1" in
        -j|--job_file)              job_file=$2; shift 2;;
        -s|--target_schema)         target_schema=$2; shift 2;;
        -t|--transactions)          transactions=$2; shift 2;;
        --no_profile_keyword)       no_profile_keyword="TRUE";shift 1;;
        --ignore_retention_issues)  ignore_retention_issues="TRUE";shift 1;;
        --pre_load_large_dc_tables) pre_load_large_dc_tables="TRUE";shift 1;;
        --pre_load_start_timestamp) pre_load_start_timestamp=$2; shift 2;;
        --pre_load_end_timestamp)   pre_load_end_timestamp=$2; shift 2;;
        --output_debug_linenumbers) output_debug_linenumbers="TRUE";shift 1;;
        # allow odbln to mean the same thing as output_debug_linenumbers - faster to type for those who know!
        --odbln)               output_debug_linenumbers="TRUE";shift 1;;
        -h|--help)             usage; exit 0;;
        --)                    shift; break;;
        *)                     break;;
    esac
done

# Use a trick to not only force bash -x output (easily done from command line) BUT to also output the linenumber being executed
# as discovered at https://unix.stackexchange.com/questions/636388/ps4-debug-bash-script-with-lineno-execute-vs-source
# Then added date/time stamping AND leading the line with the name of the running script so that sub-scripts can be discerned
if [ $output_debug_linenumbers = TRUE ]; then
  set -x
  PS4='+$0 +${LINENO}: `date +"%Y-%m-%d %H:%M:%S.%N" | rev | cut -c 7- | rev` '
  # Now generate a string to pass to subsidiary scripts so they too perform output_debug_linenumbers
  odb="--output_debug_linenumbers"
else
  odb=""
fi

# Enforce either job_file or transactions, but not both
if [ -z "$job_file" ] && [ -z "$transactions" ]; then
    usage
    echo "ERROR: Either --job_file or --transactions must be provided."
    exit 1
elif [ -n "$job_file" ] && [ -n "$transactions" ]; then
    usage
    echo "ERROR: Provide either --job_file or --transactions, but not both."
    exit 1
fi

# Assure that if pre_load_large_dc_tables is TRUE, that we have values (no error checking though) for pre_load_start_timestamp and pre_load_end_timestamp
if [ $pre_load_large_dc_tables = TRUE ]; then
  if [ ${#pre_load_start_timestamp} -eq 0 ]; then
    echo "With pre_load_large_dc_tables = TRUE, you must supply a date/time for pre_load_start_timestamp, none seen, exiting."
    exit
  fi
  if [ ${#pre_load_end_timestamp} -eq 0 ]; then
    echo "With pre_load_large_dc_tables = TRUE, you must supply a date/time for pre_load_end_timestamp, none seen, exiting."
    exit
  fi
fi

# Extract txn_id and stmt_id from transactions input
TXN_IDS=()
STMT_IDS=()

if [ -n "$transactions" ]; then
    # Remove surrounding parentheses and split the input into pairs
    transactions="${transactions//\"/}"
    transactions="${transactions//[\(\)]/}"

    # Split the input by comma, expecting txn_id,stmt_id pairs
    IFS=',' read -ra PAIRS <<< "$transactions"

    # Process pairs in sets of two
    for (( i=0; i<${#PAIRS[@]}; i+=2 )); do
        TXN_ID="${PAIRS[i]}"
        STMT_ID="${PAIRS[i+1]}"

        # Validate each pair
        if [ -z "$TXN_ID" ] || [ -z "$STMT_ID" ]; then
            echo "ERROR: Each pair in --transactions option must provide both txn_id and stmt_id in the format txn_id,stmt_id."
            exit 1
        fi

        TXN_IDS+=("$TXN_ID")
        STMT_IDS+=("$STMT_ID")
    done
else
    JOB_FILE="$job_file"
    if [ ! -e "$JOB_FILE" ]; then
        echo "Configuration file $JOB_FILE does not exist"
        exit 1
    fi
fi


TARGET_SCHEMA="${target_schema:-}"
SCRIPT_DIRNAME=$(dirname $BASH_SOURCE[0])
SCRIPT_PATH=$(readlink -f $SCRIPT_DIRNAME)
# End of options

SQL_DIR="${SCRIPT_PATH}/sql"

if [ ! -e "$SQL_DIR" ]; then
    echo "SQL directory $SQL_DIR does not exist"
    exit 1
fi

VSQL=${VSQL:-vsql}

export VSQL_ADMIN_COMMAND="${VSQL} -U $ADMIN_USER -w $ADMIN_PASSWORD"
VSQL_USER_COMMAND="${VSQL} -U $QUERY_USER -w $QUERY_USER_PASSWORD"

# Authentication check for QUERY_USER
DUMMY_SQL="SELECT 1"

if ! echo "$DUMMY_SQL" | $VSQL_USER_COMMAND >/dev/null 2>&1; then
    echo "ERROR: Authentication failed for QUERY_USER '$QUERY_USER'"
    exit 1
fi

# Authentication check for ADMIN_USER
if ! echo "$DUMMY_SQL" | $VSQL_ADMIN_COMMAND >/dev/null 2>&1; then
    echo "ERROR: Authentication failed for ADMIN_USER '$ADMIN_USER'"
    exit 1
fi

echo "Authentication successful for both QUERY_USER and ADMIN_USER."

# Function to generate a random schema name
generate_random_schema() {
    local SCHEMA_NAME
    while true; do
        SCHEMA_NAME="schema_$(openssl rand -hex 8)"
        # Check if the schema already exists
        if $VSQL_ADMIN_COMMAND -t -c "SELECT COUNT(*) FROM v_catalog.schemata WHERE schema_name = '$SCHEMA_NAME';" | grep -q '0'; then
            echo "$SCHEMA_NAME"
            return
        fi
    done
}
# end of function generate_random_schema()

# Function to check whether the query has the word 'PROFILE' in it
has_profile() {
    # Execute the query and capture the output.. depending on whether user requested --no_profile_keyword
    if [ $no_profile_keyword = FALSE ]; then
      OUTPUT=$($VSQL_ADMIN_COMMAND -Atc "SELECT request FROM query_requests WHERE transaction_id=$TXN_ID AND statement_id=$STMT_ID AND request ILIKE 'PROFILE%';")
    else
      OUTPUT=$($VSQL_ADMIN_COMMAND -Atc "SELECT request FROM query_requests WHERE transaction_id=$TXN_ID AND statement_id=$STMT_ID;")
    fi

    # Check if the output contains more than just the header and row count
    if echo "$OUTPUT" | grep -qEi '^[[:space:]]*PROFILE'; then
        echo "The query contains the word 'PROFILE' for transaction ID '$TXN_ID' and statement ID '$STMT_ID':"
    elif [ ${#OUTPUT} -gt 0 ]; then
        echo "Query without a PROFILE keyword successfully found in query_requests for transaction ID '$TXN_ID' and statement ID '$STMT_ID'"
    else
        echo "ERROR: The query does not contain the word 'PROFILE' AND query ran faster SaveDCEEProfileThresholdUS for transaction ID '$TXN_ID' and statement ID '$STMT_ID'"
        echo "All the queries should contain the word 'PROFILE' in them."
        exit 1
    fi
}
# end of function has_profile()

# Checking retention issues if transactions are provided
if [ -n "$transactions" ]; then
    condition=""
    for (( i=0; i<${#TXN_IDS[@]}; i++ )); do
        if [[ $i -ne 0 ]]; then
            condition+=" OR "
        fi
        condition+="(transaction_id = ${TXN_IDS[i]} AND statement_id = ${STMT_IDS[i]})"
    done

    # Get the minimum start timestamp based on the condition
    query_min_time="$($VSQL_ADMIN_COMMAND -Atc "SELECT MIN(start_timestamp) FROM query_requests WHERE $condition;")"
    if [ "$i" -eq 0 ]; then
        echo "Query minimum start time: $query_min_time"
    else
        echo "Minimum of all queries start time: $query_min_time"
    fi

    # Define the important tables and their respective time columns
    declare -a important_table_list=(
        "v_internal.dc_explain_plans,time"
        "v_internal.dc_query_executions,time"
        "v_internal.dc_requests_issued,time"
        "v_internal.dc_plan_steps,time"
        "v_monitor.query_events,event_timestamp"
        "v_monitor.query_consumption,start_time"
        "query_profiles,query_start"
        "v_monitor.resource_acquisitions,queue_entry_timestamp"
        "v_internal.dc_plan_activities,start_time"
	"v_internal.dc_lock_attempts,time"
	"v_internal.dc_plan_resources,time"
	"v_internal.dc_plan_step_properties,time"
	"v_internal.dc_scan_events,time"
    )

    # Array to keep track of tables with retention issues
    retention_issues=()

    # Loop through each table and check their minimum time
    for table_data in "${important_table_list[@]}"; do
        IFS=',' read -r table table_time <<< "$table_data"

        # Get the minimum timestamp from the current table
        table_min_time="$($VSQL_ADMIN_COMMAND -Atc "SELECT MIN($table_time) FROM $table;")"
        echo "Table '$table' has entries going far back as time $table_min_time"

        # If the table_min_time is not empty and compare it to the query_min_time
        if [[ -n "$table_min_time" ]]; then
            # Format the datetime if it's a string
            if [[ $table_min_time == *+00 ]]; then
                table_min_time="${table_min_time/+00/+0000}"
            fi
            # Convert the timestamps to comparable formats
            table_min_time_epoch=$(date -d "$table_min_time" +"%s")
            query_min_time_epoch=$(date -d "$query_min_time" +"%s")

            # Compare the timestamps
            if [[ "$query_min_time_epoch" -lt "$table_min_time_epoch" ]]; then
                echo "Retention issue detected for table '$table'."
                retention_issues+=("$table")
            fi
        fi
    done

    # Check if there were any retention issues
    if [ ${#retention_issues[@]} -gt 0 ]; then
        echo "Retention issues found in the following tables:"
        for table in "${retention_issues[@]}"; do
            echo " - $table"
        done

        # Exit unless user asserted --ignore_retention_issues
        if [ $ignore_retention_issues = FALSE ]; then
          echo "Retention issues exist. Please re-profile your query or extend the data retention time for the affected tables."
          exit 1
        else
          echo "Retention issues exist but --ignore_retention_issues requested... continuing anyway"
        fi
    else
        echo "All tables have sufficient retention data."
    fi
fi


# Check and handle target schema
if [ -z "$TARGET_SCHEMA" ]; then
    # No schema provided, generate a random one
    TARGET_SCHEMA=$(generate_random_schema)
    echo "No schema provided. Generated schema name: $TARGET_SCHEMA"
else
    # User provided a schema, check if it exists
    if $VSQL_ADMIN_COMMAND -t -c "SELECT COUNT(*) FROM v_catalog.schemata WHERE schema_name = '$TARGET_SCHEMA';" | grep -q '0'; then
        echo "Schema '$TARGET_SCHEMA' does not exist. It will be created."
    else
        echo "Error: Schema '$TARGET_SCHEMA' already exists. Please provide a different unique schema name or omit it to automatically generate a new schema name."
        exit 1
    fi
fi

echo "Using schema name: $TARGET_SCHEMA"

RAND_ID=$(($RANDOM % 100))
RUN_ID="run_qprof_export_$RAND_ID"
SCRATCH_DIR=${PWD}/$RUN_ID
echo "---------------------------------------------------"
echo "RUN_ID = $RUN_ID"
echo "SCRATCH_DIR = $SCRATCH_DIR"
echo "TARGET_SCHEMA = $TARGET_SCHEMA"
echo "---------------------------------------------------"
rm -rf $SCRATCH_DIR

# Check if the directory can be created
if ! mkdir -p "$SCRATCH_DIR"; then
    echo "Error: Unable to create directory $SCRATCH_DIR. Please check permissions."
    exit 1
fi

echo "+++ Making schema +++"

$VSQL_ADMIN_COMMAND -a -c "create schema if not exists $TARGET_SCHEMA;"
#$VSQL_ADMIN_COMMAND -a -c "grant all on schema $TARGET_SCHEMA to $QUERY_USER"

# TODO: use this table to say which txns we collect
# vsql -a -c "create table if not exists $TARGET_SCHEMA.profile_collection_info(transaction_id int, statement_id int, query_name varchar(128))"

# SOURCE_TABLES="dc_requests_issued dc_query_executions dc_explain_plans query_plan_profiles query_profiles execution_engine_profiles resource_acquisitions query_consumption"
# NOTE that in fact we need ORIGINAL_SCHEMA.TABLE names, will hard-code for now
#SOURCE_TABLES="v_internal.dc_requests_issued v_internal.dc_query_executions v_internal.dc_explain_plans "
SOURCE_TABLES="v_internal.dc_requests_issued v_internal.dc_query_executions v_internal.dc_explain_plans  v_monitor.query_profiles v_monitor.execution_engine_profiles v_monitor.resource_acquisitions v_monitor.query_consumption v_monitor.query_plan_profiles v_internal.dc_slow_events v_monitor.query_events v_internal.dc_lock_attempts v_internal.dc_plan_activities v_internal.dc_plan_resources v_internal.dc_plan_step_properties v_internal.dc_scan_events v_internal.dc_plan_steps v_monitor.projection_usage"
SNAPSHOT_TABLES="v_monitor.host_resources v_monitor.resource_pool_status v_monitor.projection_storage v_monitor.storage_containers"

# It would be handy to have tables stored in a list, separate from schemas
for t in $SOURCE_TABLES
do
    echo "-------------------------------------"
    echo "Creating profile destination for $t"
    ORIGINAL_SCHEMA="${t%%.*}"
    TABLE_NAME="${t##*.}"
    echo "Original schema = ${ORIGINAL_SCHEMA}"
    echo "Original table = ${TABLE_NAME}"
    $VSQL_ADMIN_COMMAND -a -c "create table if not exists $TARGET_SCHEMA.$TABLE_NAME as select * from $ORIGINAL_SCHEMA.$TABLE_NAME LIMIT 0;"
    # Be sure to add a column for query_name
    $VSQL_ADMIN_COMMAND -a -c "alter table $TARGET_SCHEMA.$TABLE_NAME add column if not exists query_name varchar(128);"
done

for t in $SNAPSHOT_TABLES
do
echo "-------------------------------------"
    echo "SNAPSHOT TABLE: Creating profile destination for $t"
    ORIGINAL_SCHEMA="${t%%.*}"
    TABLE_NAME="${t##*.}"
    echo "Original schema = ${ORIGINAL_SCHEMA}"
    echo "Original table = ${TABLE_NAME}"
    $VSQL_ADMIN_COMMAND -a -c "create table if not exists $TARGET_SCHEMA.$TABLE_NAME as select * from $ORIGINAL_SCHEMA.$TABLE_NAME LIMIT 0;"
    # Be sure to add a columns for:
    # Transaction id
    # Statement id
    # Query_name
    $VSQL_ADMIN_COMMAND -a -c "alter table $TARGET_SCHEMA.$TABLE_NAME add column if not exists transaction_id int;"
    $VSQL_ADMIN_COMMAND -a -c "alter table $TARGET_SCHEMA.$TABLE_NAME add column if not exists statement_id int;"
    $VSQL_ADMIN_COMMAND -a -c "alter table $TARGET_SCHEMA.$TABLE_NAME add column if not exists query_name varchar(128);"

done


echo "Creating additional collection info tables"

for tracking_table in "${SQL_DIR}/tables/collect_create_collection_info.sql"
do
    tempfile="${SCRATCH_DIR}/$(basename $tracking_table)"
    sed "s|IMPORT_SCHEMA|${TARGET_SCHEMA}|g" $tracking_table > $tempfile
    $VSQL_ADMIN_COMMAND -a -f "$tempfile"
done
# TODO: update collection_events with more columns
#
$VSQL_ADMIN_COMMAND -a -c "create table if not exists $TARGET_SCHEMA.collection_events(transaction_id int, statement_id int, table_name varchar (256), operation varchar(128), row_count int);"

if [ $pre_load_large_dc_tables = TRUE ]; then
  # We *may* have already inserted data for the transaction set (bounded by $pre_load_start_timestamp and $pre_load_end_timestamp) info EEP an QPP
  # Check that by establishing the earliest transaction_id and latest transaction_id within that timeframe, then checking to see if we have those 
  # transactions already stored in EEP and QQP
  earliest_transaction_id_in_range=$($VSQL_ADMIN_COMMAND -Atc "select transaction_id from query_requests qr WHERE qr.start_timestamp BETWEEN '$pre_load_start_timestamp' AND '$pre_load_end_timestamp' AND qr.transaction_id != 0 order by qr.start_timestamp limit 1;")
  latest_transaction_id_in_range=$($VSQL_ADMIN_COMMAND -Atc "select transaction_id from query_requests qr WHERE qr.start_timestamp BETWEEN '$pre_load_start_timestamp' AND '$pre_load_end_timestamp' AND qr.transaction_id != 0 order by qr.start_timestamp desc limit 1;")

  # count transactions seen including either the earliest or latest:
  transaction_count_in_range=$($VSQL_ADMIN_COMMAND -Atc "select count(*) from public.execution_engine_profiles where transaction_id in ('$earliest_transaction_id_in_range','$latest_transaction_id_in_range');")

  # for now, assume that if we stored transactions in public.execution_engine_profiles then we did so as well in public.query_plan_profiles
  if [ $transaction_count_in_range -eq 0 ]; then # we have NOT stored transactions for this time range, do so now

    echo "---------------------------------------------"
    echo "Pre-populating public.execution_engine_profiles for transactions between $pre_load_start_timestamp and $pre_load_end_timestamp"
    echo "---------------------------------------------"
  
    $VSQL_ADMIN_COMMAND -c "create table if not exists public.execution_engine_profiles as select * from ${TARGET_SCHEMA}.execution_engine_profiles LIMIT 0;"
  
    # Instead of populating public.execution_engine_profiles based off cinfo, to be more generalized (and work on other than tpcds), collect ALL profiles based on the list of transaction_id values
    # bounded by $pre_load_start_timestamp and $pre_load_end_timestamp
  
    # Purposely not pre-clearing this table, it is OK to build up lots of content here.
    $VSQL_ADMIN_COMMAND -c "insert into public.execution_engine_profiles select orig.node_name,orig.user_id,orig.user_name,orig.session_id,orig.transaction_id,orig.statement_id,orig.plan_id,orig.operator_name,orig.operator_id,orig.baseplan_id,orig.path_id,orig.localplan_id,orig.activity_id,orig.resource_id,orig.counter_name,orig.counter_tag,orig.counter_value,orig.is_executing from v_monitor.execution_engine_profiles as orig JOIN ( SELECT transaction_id, statement_id FROM query_requests WHERE start_timestamp BETWEEN '$pre_load_start_timestamp' AND '$pre_load_end_timestamp' AND transaction_id != 0 ) qr ON orig.transaction_id = qr.transaction_id AND orig.statement_id = qr.statement_id; commit;"
  
    echo "---------------------------------------------"
    echo "Pre-populating public.query_plan_profiles for transactions between $pre_load_start_timestamp and $pre_load_end_timestamp"
    echo "---------------------------------------------"
  
    $VSQL_ADMIN_COMMAND -c "create table if not exists public.query_plan_profiles as select * from ${TARGET_SCHEMA}.query_plan_profiles LIMIT 0;"
  
    # Purposely not pre-clearing this table, it is OK to build up lots of content here.
    $VSQL_ADMIN_COMMAND -c "insert into public.query_plan_profiles  select orig.transaction_id,orig.statement_id,orig.path_id,orig.path_line_index,orig.path_is_started,orig.path_is_completed,orig.is_executing,orig.running_time,orig.memory_allocated_bytes,orig.read_from_disk_bytes,orig.received_bytes,orig.sent_bytes,orig.path_line from v_monitor.query_plan_profiles as orig JOIN ( SELECT transaction_id, statement_id FROM query_requests WHERE start_timestamp BETWEEN '$pre_load_start_timestamp' AND '$pre_load_end_timestamp' AND transaction_id != 0 ) qr ON orig.transaction_id = qr.transaction_id AND orig.statement_id = qr.statement_id; commit;"
  fi
fi

PROF_COUNT=0
LINE_COUNT=0
# Use provided txn_id and stmt_id if available, otherwise execute the profiling as usual
if [ ${#TXN_IDS[@]} -gt 0 ]; then
    for (( i=0; i<${#TXN_IDS[@]}; i++ )); do
        TXN_ID="${TXN_IDS[i]}"
        STMT_ID="${STMT_IDS[i]}"

        echo "Checking whether the 'PROFILE' word was part of the query or not..."

        has_profile

        echo "Storing existing profiled query using transaction: TXN_ID=$TXN_ID, STMT_ID=$STMT_ID"
        $VSQL_ADMIN_COMMAND -a -c "insert into $TARGET_SCHEMA.collection_info values ($TXN_ID, $STMT_ID, '$PROJECT_NAME', '$CUSTOMER_NAME'); commit;"

        for t in $SNAPSHOT_TABLES; do
            echo "---------------------------------------------"
            echo "Collecting from SNAPSHOT Source Table is $t"
            echo "---------------------------------------------"
            ORIGINAL_SCHEMA="${t%%.*}"
            TABLE_NAME="${t##*.}"
            time $VSQL_ADMIN_COMMAND -a -c "insert into $TARGET_SCHEMA.$TABLE_NAME select *, $TXN_ID, $STMT_ID, '' from $ORIGINAL_SCHEMA.$TABLE_NAME ; commit;"
        done
        PROF_COUNT=$((PROF_COUNT + 1))
    done
else
    # Process profiling based on provided job file if no transaction details were given
    echo "Processing new queries from job file..."

    while read -r line;
    do
        LINE_COUNT=$(($LINE_COUNT + 1))
        if [[ $line == "#"* ]]; then
            echo "Skipping comment on line num $LINE_COUNT, '$line'"
            continue
        fi
        USER_LABEL=$(echo $line | cut -d '|' -f 1)
        USER_COMMENT=$(echo $line | cut -d '|' -f 2)
        QUERY_FILE=$(echo $line | cut -d '|' -f 3 | tr -d ' ')

        if [ -z "$USER_LABEL" ]; then
            echo "Line $LINE_COUNT has empty user label: '$line'"
            exit 1
        fi

        if [ -z "$USER_COMMENT" ]; then
            echo "Line $LINE_COUNT has empty user comment: '$line'"
            exit 1
        fi

        if [ -z "$QUERY_FILE" ]; then
            echo "Line $LINE_COUNT has empty user comment: '$line'"
            exit 1
        fi

        echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        echo "Query: $USER_LABEL, '$USER_COMMENT', $QUERY_FILE"

        QUERY_FILE_BASENAME=$(basename "$QUERY_FILE")

        echo "Adding profile statement..."
        SCRATCH_QUERY_FILE=${SCRATCH_DIR}/${QUERY_FILE_BASENAME}

        cp -v "${QUERY_FILE}" "${SCRATCH_QUERY_FILE}"

        # The following sed command adds profiling ONCE
        sed -i -r -e "0,/(^\(?[Ww][Ii][Tt][Hh]|^\(?[Ss][Ee][Ll][Ee][Cc][Tt]^\(?[Ii][Nn][Ss][Ee][Rr][Tt])/{s#(^\(?WITH|^\(?SELECT|^\(?INSERT)#PROFILE \1 #i}" ${SCRATCH_QUERY_FILE}

        # the follow sed command extends the hint to have a label
        if grep -c '/[*][+]' ${SCRATCH_QUERY_FILE}; then
        # Case - there is a hint in the query already
        # INPUT: select /*+opt_dir('V2OptDisableJoinRanks=true')*/ ...
        # OUTPUT: select /*+opt_dir('V2OptDisableJoinRanks=true'), label('CQCS')*/
        # We must extend the query hint: distinct hints are not allowed
        # It follows that there is exactly one hint the input query: otherwise, the
        # query would be invalid
        sed -i -r -e "s#/[*][+](.*)\)[*]/#/*+\1\), label('$USER_LABEL')*/#" ${SCRATCH_QUERY_FILE}
        else
        # Case - there is no hint in the query already
        sed -i -r -e "0,/(^WITH|^SELECT)/{s#(^WITH|^SELECT)# \1 /*+label('$USER_LABEL')*/ #}" ${SCRATCH_QUERY_FILE}
        fi

        echo "Begin query execution"

        PROFILE_NOTICE_FILE=$SCRATCH_DIR/${QUERY_FILE_BASENAME}.prof_msg
        time $VSQL_USER_COMMAND -o /dev/null -f ${SCRATCH_QUERY_FILE} 2>> $PROFILE_NOTICE_FILE
        echo "Query execution complete"
        cat $PROFILE_NOTICE_FILE
        QUERY_ID_FILE=${SCRATCH_DIR}/${QUERY_FILE_BASENAME}.qid
        grep '^HINT:' $PROFILE_NOTICE_FILE | sed 's#.*transaction_id=\([0-9]\+\) and statement_id=\([0-9]\+\).*#\1|\2#' > $QUERY_ID_FILE
        combo_tid_sid=$(cat ${QUERY_ID_FILE})
        TXN_ID=`echo $combo_tid_sid | cut -f1 -d '|'`
        STMT_ID=`echo $combo_tid_sid | cut -f2 -d '|'`

        echo "TXN: $TXN_ID"
        echo "STMT: $STMT_ID"
        if [ -z "$TXN_ID" ]; then
            echo "Error: TXN_ID was empty, combo_tid_sid was $combo_tid_sid"
            exit 1
        fi
        $VSQL_ADMIN_COMMAND -a -c "insert into $TARGET_SCHEMA.collection_info values ($TXN_ID, $STMT_ID, '$USER_LABEL', '$USER_COMMENT', '$PROJECT_NAME', '$CUSTOMER_NAME'); commit;"

        for t in $SNAPSHOT_TABLES
        do
        echo "---------------------------------------------"
        echo "Collecting from SNAPSHOT Source Table is $t"
        echo "---------------------------------------------"
        ORIGINAL_SCHEMA="${t%%.*}"
        TABLE_NAME="${t##*.}"

        time $VSQL_ADMIN_COMMAND -a -c "insert into $TARGET_SCHEMA.$TABLE_NAME select *, $TXN_ID, $STMT_ID, '$USER_LABEL' from $ORIGINAL_SCHEMA.$TABLE_NAME ; commit;"

        done

        PROF_COUNT=$((PROF_COUNT +1))
        echo "Done with query $USER_LABEL count $PROF_COUNT"

    done < "$JOB_FILE"
fi

for t in $SOURCE_TABLES
do
    echo "---------------------------------------------"
    echo "Collecting from Source Table $t"
    echo "---------------------------------------------"
    ORIGINAL_SCHEMA="${t%%.*}"
    TABLE_NAME="${t##*.}"

    # We need to choose which columns we want in the select statement
    # because all columns is too many.
    # In order to choose all cols, we need a definition of the table
    # Then we need to take the list of columns and update them so that
    # they have the alias 'orig' prepended,
    # orig.col1, orig.col2
    RAW_COLS=`cat sql/cols/$TABLE_NAME.cols`

    QUALIFIED_COLS=$(echo $RAW_COLS | sed -r 's|([a-zA-Z0-9_]+),|orig.\1,|g' | sed -r 's|,([a-zA-Z0-9_]+)$|,orig.\1|')

    # Where we source data from depends on both the TABLE_NAME and whether pre_load_large_dc_tables is TRUE
    if ([ $TABLE_NAME = "execution_engine_profiles" ] && [ $pre_load_large_dc_tables = TRUE ]) ; then
      # Will read from the pre-loaded ROS-based version of this table for speed
      time $VSQL_ADMIN_COMMAND -a -c "insert into $TARGET_SCHEMA.$TABLE_NAME select $QUALIFIED_COLS, cinfo.user_query_label from public.execution_engine_profiles as orig join $TARGET_SCHEMA.collection_info as cinfo on orig.transaction_id=cinfo.transaction_id and orig.statement_id=cinfo.statement_id; commit;"

    elif ([ $TABLE_NAME = "query_plan_profiles" ] && [ $pre_load_large_dc_tables = TRUE ]); then
      time $VSQL_ADMIN_COMMAND -a -c "insert into $TARGET_SCHEMA.$TABLE_NAME select $QUALIFIED_COLS, cinfo.user_query_label from public.query_plan_profiles as orig join $TARGET_SCHEMA.collection_info as cinfo on orig.transaction_id=cinfo.transaction_id and orig.statement_id=cinfo.statement_id; commit;"

    elif [ $TABLE_NAME = "dc_lock_attempts" ]; then
      # In this case, there is no statement_id - lock attempts are only recorded at the transaction level
      time $VSQL_ADMIN_COMMAND -a -c "insert into $TARGET_SCHEMA.$TABLE_NAME select $QUALIFIED_COLS, cinfo.user_query_label from $ORIGINAL_SCHEMA.$TABLE_NAME as orig join $TARGET_SCHEMA.collection_info as cinfo on orig.transaction_id=cinfo.transaction_id; commit;"

    else
      time $VSQL_ADMIN_COMMAND -a -c "insert into $TARGET_SCHEMA.$TABLE_NAME select $QUALIFIED_COLS, cinfo.user_query_label from $ORIGINAL_SCHEMA.$TABLE_NAME as orig join $TARGET_SCHEMA.collection_info as cinfo on orig.transaction_id=cinfo.transaction_id and orig.statement_id=cinfo.statement_id; commit;"
    fi
done




echo "Building up verification tables"


for t in $SOURCE_TABLES $SNAPSHOT_TABLES
do
    echo "<<<< Checking load info >>>>"
    echo "Source Table is $t"
    ORIGINAL_SCHEMA="${t%%.*}"
    TABLE_NAME="${t##*.}"
    echo "Target is $TABLE_NAME"
    echo "---------------------------------------------"

    if [ $TABLE_NAME = "dc_lock_attempts" ]; then
      # In this case, there is no statement_id - lock attempts are only recorded at the transaction level so we hard-code a statement_id of ZERO to keep the receiving table happy
      time $VSQL_ADMIN_COMMAND -a -c "insert into $TARGET_SCHEMA.collection_events select cinfo.transaction_id, 0, '$TABLE_NAME', 'collect', count (*) as row_count from $TARGET_SCHEMA.$TABLE_NAME dupe join $TARGET_SCHEMA.collection_info cinfo on dupe.transaction_id=cinfo.transaction_id group by 1, 2 order by 1,2; commit;"
    else
      time $VSQL_ADMIN_COMMAND -a -c "insert into $TARGET_SCHEMA.collection_events select cinfo.transaction_id, cinfo.statement_id, '$TABLE_NAME', 'collect', count (*) as row_count from $TARGET_SCHEMA.$TABLE_NAME dupe join $TARGET_SCHEMA.collection_info cinfo on dupe.transaction_id=cinfo.transaction_id and dupe.statement_id=cinfo.statement_id group by 1, 2 order by 1,2; commit;"
    fi
done

# Show a summary table AND as it is likely to paginate (requiring user input), prevent that by redirecting to a file that is immediately catted
$VSQL_ADMIN_COMMAND -a -c "select transaction_id, statement_id, table_name, sum(row_count) from $TARGET_SCHEMA.collection_events group by 1, 2, 3 order by 1, 2, 3" > temp; cat temp

rm -rf $SCRATCH_DIR

echo "Done with script collecting into $TARGET_SCHEMA. Profiled ${PROF_COUNT} queries"

# Check to see if any critical tables failed to get populated. Failures to populate any of these is a reason to NOT go ahead
# and export to a parquet tarball as it won't work or provide worthwhile data
CRITICAL_TABLES="dc_explain_plans dc_plan_steps dc_plan_step_properties dc_query_executions query_plan_profiles dc_plan_activities query_events"

FAILED_CRITICAL_TABLES=""
for t in $CRITICAL_TABLES
do
    echo
    echo "<<<< Checking for critical table zero-row-count issues >>>>"
    echo "Critical Table is $t"
    echo "---------------------------------------------"
    critical_table_row_count=$($VSQL_ADMIN_COMMAND -Atc "select count(*) from $TARGET_SCHEMA.$t;")
    if [ $critical_table_row_count -eq 0 ]; then
      # add the current (failed) critical table to a vertical list of failed tables
      FAILED_CRITICAL_TABLES="${FAILED_CRITICAL_TABLES}${t}\n"
    fi
done

if [ ${#FAILED_CRITICAL_TABLES} -gt 0 ]; then
  # We have at least one critical table with zero rows in $TARGET_SCHEMA - pointless to export to a parquet tarball
  echo; echo "The following critical tables were found with zero rows in schema $TARGET_SCHEMA. There is no point in creating a parquet tarball for import to qprof. Try profiling again."
  echo -e $FAILED_CRITICAL_TABLES
  exit
fi

echo "Next - Now running the export script with Target schema: $TARGET_SCHEMA and Local Directory: $LOCAL_DIRECTORY"

./export.sh "$TARGET_SCHEMA" "$LOCAL_DIRECTORY" $odb
