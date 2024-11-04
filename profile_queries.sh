#!/bin/bash
# Based off Jason Slaunwhite code for exporting query profiling information
# MODIFIED BY UMAR FAROOQ GHUMMAN - 7/24/2024
# This script profiles queries and optionally creates a schema if not provided

##############################
# USER CONFIGURATION SECTION #
##############################

# Database credentials for query execution
QUERY_USER="ughumman"
QUERY_USER_PASSWORD='""'

# Database credentials for administrative operations
ADMIN_USER="ughumman"
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
$0 accepts inputs as follow:

Usage: $0

    Options:
    -j, --job_file          [Required] eg. foo.txt
    -s, --target_schema     [Optional] eg. test_01 (The schema name should be unused)
    -t, --transactions      [Optional] txn_id,stmt_id (Profile an existing transaction)

    Help:
    -h, --help              [Help] Show this help info

For example:
    $0 \
-j foo.txt \
-s test_01 \
-t 12345,67890"
}
# end of function usage()

# List of parameter flags
TEMP=$(getopt -o j:s:t:h --long job_file:,target_schema:,transactions:,help -n 'ERROR: $0' -- "$@")

if [ $? != 0 ]; then usage; exit; fi
eval set -- "$TEMP"

# Bind each parameter NAME
job_file=""
target_schema=""
transactions=""

while true; do
    case "$1" in
        -j|--job_file)         job_file=$2; shift 2;;
        -s|--target_schema)    target_schema=$2; shift 2;;
        -t|--transactions)     transactions=$2; shift 2;;
        -h|--help)             usage; exit 0;;
        --)                    shift; break;;
        *)                     break;;
    esac
done

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

# Extract txn_id and stmt_id from transactions input
TXN_IDS=()
STMT_IDS=()

if [ -n "$transactions" ]; then
    # Remove any surrounding quotes if they exist
    transactions="${transactions#\"}"
    transactions="${transactions%\"}"
    transactions="${transactions#\'}"
    transactions="${transactions%\'}"
    
    # Remove surrounding parentheses and split pairs by the format (txn_id,stmt_id)
    transactions="${transactions#\(\(}"
    transactions="${transactions%\)\)}"
    
    # Split by ),( to handle multiple pairs
    IFS=")," read -ra PAIRS <<< "$transactions"
    for pair in "${PAIRS[@]}"; do
        # Remove any extra spaces and split by comma
        IFS=',' read -r txn_id stmt_id <<< "${pair// /}"
        
        # Validate each pair
        if [ -z "$txn_id" ] || [ -z "$stmt_id" ]; then
            echo "ERROR: Each pair in --transactions option must provide both txn_id and stmt_id in the format (txn_id,stmt_id)."
            exit 1
        fi

        # Append values to the respective arrays
        TXN_IDS+=("$txn_id")
        STMT_IDS+=("$stmt_id")
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
SOURCE_TABLES="v_internal.dc_requests_issued v_internal.dc_query_executions v_internal.dc_explain_plans  v_monitor.query_profiles v_monitor.execution_engine_profiles v_monitor.resource_acquisitions v_monitor.query_consumption v_monitor.query_plan_profiles v_internal.dc_slow_events v_monitor.query_events"
SNAPSHOT_TABLES="v_monitor.host_resources v_monitor.resource_pool_status"

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
    # Transcation id
    # Statemet id
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

PROF_COUNT=0
LINE_COUNT=0
# Use provided txn_id and stmt_id if available, otherwise execute the profiling as usual
if [ ${#TXN_IDS[@]} -gt 0 ]; then
    for (( i=0; i<${#TXN_IDS[@]}; i++ )); do
        TXN_ID="${TXN_IDS[i]}"
        STMT_ID="${STMT_IDS[i]}"
        
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
        sed -i -r -e "0,/(^\(?[Ww][Ii][Tt][Hh]|^\(?[Ss][Ee][Ll][Ee][Cc][Tt])/{s#(^\(?WITH|^\(?SELECT)#PROFILE \1 #i}" ${SCRATCH_QUERY_FILE}

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

    time $VSQL_ADMIN_COMMAND -a -c "insert into $TARGET_SCHEMA.$TABLE_NAME select $QUALIFIED_COLS, cinfo.user_query_label from $ORIGINAL_SCHEMA.$TABLE_NAME as orig join $TARGET_SCHEMA.collection_info as cinfo on orig.transaction_id=cinfo.transaction_id and orig.statement_id=cinfo.statement_id; commit;"

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
    time $VSQL_ADMIN_COMMAND -a -c "insert into $TARGET_SCHEMA.collection_events select cinfo.transaction_id, cinfo.statement_id, '$TABLE_NAME', 'collect', count (*) as row_count from $TARGET_SCHEMA.$TABLE_NAME dupe join $TARGET_SCHEMA.collection_info cinfo on dupe.transaction_id=cinfo.transaction_id and dupe.statement_id=cinfo.statement_id group by 1, 2 order by 1,2; commit;"
done

# Show a summary table
$VSQL_ADMIN_COMMAND -a -c "select transaction_id, statement_id, table_name, sum(row_count) from $TARGET_SCHEMA.collection_events group by 1, 2, 3 order by 1, 2, 3"

rm -rf $SCRATCH_DIR

echo "Done with script collecting into $TARGET_SCHEMA. Profiled ${PROF_COUNT} queries" 

echo "Next - Now running the export script with Target schema: $TARGET_SCHEMA and Local Directory: $LOCAL_DIRECTORY"

./export.sh "$TARGET_SCHEMA" "$LOCAL_DIRECTORY"
