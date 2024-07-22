
/* Create a staging table here, real table is below*/
CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_query_plan_profiles_stage_key
(
    transaction_id int,
    statement_id int,
    path_id int,
    path_line_index int,
    path_is_started boolean,
    path_is_completed boolean,
    is_executing boolean,
    /* We hack running time to be a float ... parquet doesn't like intervals
     * Hack applies to stage table only.
     */
    running_time float,
    memory_allocated_bytes int,
    read_from_disk_bytes int,
    received_bytes int,
    sent_bytes int,
    path_line varchar(64000),
    query_name varchar(128)
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_query_plan_profiles_stage_key_super /*+basename(qprof_query_plan_profiles_stage_key),createtype(A)*/ 
(
 transaction_id,
 statement_id,
 path_id,
 path_line_index,
 path_is_started,
 path_is_completed,
 is_executing,
 running_time,
 memory_allocated_bytes,
 read_from_disk_bytes,
 received_bytes,
 sent_bytes,
 path_line,
 query_name
)
AS
 SELECT qprof_query_plan_profiles_stage_key.transaction_id,
        qprof_query_plan_profiles_stage_key.statement_id,
        qprof_query_plan_profiles_stage_key.path_id,
        qprof_query_plan_profiles_stage_key.path_line_index,
        qprof_query_plan_profiles_stage_key.path_is_started,
        qprof_query_plan_profiles_stage_key.path_is_completed,
        qprof_query_plan_profiles_stage_key.is_executing,
        qprof_query_plan_profiles_stage_key.running_time,
        qprof_query_plan_profiles_stage_key.memory_allocated_bytes,
        qprof_query_plan_profiles_stage_key.read_from_disk_bytes,
        qprof_query_plan_profiles_stage_key.received_bytes,
        qprof_query_plan_profiles_stage_key.sent_bytes,
        qprof_query_plan_profiles_stage_key.path_line,
        qprof_query_plan_profiles_stage_key.query_name
 FROM IMPORT_SCHEMA.qprof_query_plan_profiles_stage_key
 ORDER BY qprof_query_plan_profiles_stage_key.transaction_id,
          qprof_query_plan_profiles_stage_key.statement_id,
          qprof_query_plan_profiles_stage_key.path_id,
          qprof_query_plan_profiles_stage_key.path_line_index,
          qprof_query_plan_profiles_stage_key.path_is_started,
          qprof_query_plan_profiles_stage_key.path_is_completed,
          qprof_query_plan_profiles_stage_key.is_executing,
          qprof_query_plan_profiles_stage_key.running_time
SEGMENTED BY hash(qprof_query_plan_profiles_stage_key.transaction_id, qprof_query_plan_profiles_stage_key.statement_id, qprof_query_plan_profiles_stage_key.path_id, qprof_query_plan_profiles_stage_key.path_line_index, qprof_query_plan_profiles_stage_key.path_is_started, qprof_query_plan_profiles_stage_key.path_is_completed, qprof_query_plan_profiles_stage_key.is_executing, qprof_query_plan_profiles_stage_key.running_time) ALL NODES;

/* create the real table */

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_query_plan_profiles_key
(
    transaction_id int,
    statement_id int,
    path_id int,
    path_line_index int,
    path_is_started boolean,
    path_is_completed boolean,
    is_executing boolean,
    /* Now running_time is a interval, which is what we expect*/
    running_time interval,
    memory_allocated_bytes int,
    read_from_disk_bytes int,
    received_bytes int,
    sent_bytes int,
    path_line varchar(64000),
    query_name varchar(128)
);

CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_query_plan_profiles_key_super /*+basename(qprof_query_plan_profiles_key),createtype(A)*/ 
(
 transaction_id,
 statement_id,
 path_id,
 path_line_index,
 path_is_started,
 path_is_completed,
 is_executing,
 running_time,
 memory_allocated_bytes,
 read_from_disk_bytes,
 received_bytes,
 sent_bytes,
 path_line,
 query_name
)
AS
 SELECT qprof_query_plan_profiles_key.transaction_id,
        qprof_query_plan_profiles_key.statement_id,
        qprof_query_plan_profiles_key.path_id,
        qprof_query_plan_profiles_key.path_line_index,
        qprof_query_plan_profiles_key.path_is_started,
        qprof_query_plan_profiles_key.path_is_completed,
        qprof_query_plan_profiles_key.is_executing,
        qprof_query_plan_profiles_key.running_time,
        qprof_query_plan_profiles_key.memory_allocated_bytes,
        qprof_query_plan_profiles_key.read_from_disk_bytes,
        qprof_query_plan_profiles_key.received_bytes,
        qprof_query_plan_profiles_key.sent_bytes,
        qprof_query_plan_profiles_key.path_line,
        qprof_query_plan_profiles_key.query_name
 FROM IMPORT_SCHEMA.qprof_query_plan_profiles_key
 ORDER BY qprof_query_plan_profiles_key.transaction_id,
          qprof_query_plan_profiles_key.statement_id,
          qprof_query_plan_profiles_key.path_id,
          qprof_query_plan_profiles_key.path_line_index,
          qprof_query_plan_profiles_key.path_is_started,
          qprof_query_plan_profiles_key.path_is_completed,
          qprof_query_plan_profiles_key.is_executing,
          qprof_query_plan_profiles_key.running_time
SEGMENTED BY hash(qprof_query_plan_profiles_key.transaction_id, qprof_query_plan_profiles_key.statement_id, qprof_query_plan_profiles_key.path_id, qprof_query_plan_profiles_key.path_line_index, qprof_query_plan_profiles_key.path_is_started, qprof_query_plan_profiles_key.path_is_completed, qprof_query_plan_profiles_key.is_executing, qprof_query_plan_profiles_key.running_time) ALL NODES;



