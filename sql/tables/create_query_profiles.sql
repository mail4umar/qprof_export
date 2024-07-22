

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_query_profiles_key
(
    session_id varchar(128),
    transaction_id int,
    statement_id int,
    identifier varchar(128),
    node_name varchar(128),
    query varchar(64000),
    query_search_path varchar(64000),
    schema_name varchar(128),
    table_name varchar(128),
    query_duration_us numeric(36,6),
    query_start_epoch int,
    query_start varchar(63),
    query_type varchar(128),
    error_code int,
    user_name varchar(128),
    processed_row_count int,
    reserved_extra_memory_b int,
    is_executing boolean,
    query_name varchar(128)
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_query_profiles_key_super /*+basename(qprof_query_profiles_key),createtype(A)*/ 
(
 session_id,
 transaction_id,
 statement_id,
 identifier,
 node_name,
 query,
 query_search_path,
 schema_name,
 table_name,
 query_duration_us,
 query_start_epoch,
 query_start,
 query_type,
 error_code,
 user_name,
 processed_row_count,
 reserved_extra_memory_b,
 is_executing,
 query_name
)
AS
 SELECT qprof_query_profiles_key.session_id,
        qprof_query_profiles_key.transaction_id,
        qprof_query_profiles_key.statement_id,
        qprof_query_profiles_key.identifier,
        qprof_query_profiles_key.node_name,
        qprof_query_profiles_key.query,
        qprof_query_profiles_key.query_search_path,
        qprof_query_profiles_key.schema_name,
        qprof_query_profiles_key.table_name,
        qprof_query_profiles_key.query_duration_us,
        qprof_query_profiles_key.query_start_epoch,
        qprof_query_profiles_key.query_start,
        qprof_query_profiles_key.query_type,
        qprof_query_profiles_key.error_code,
        qprof_query_profiles_key.user_name,
        qprof_query_profiles_key.processed_row_count,
        qprof_query_profiles_key.reserved_extra_memory_b,
        qprof_query_profiles_key.is_executing,
        qprof_query_profiles_key.query_name
 FROM IMPORT_SCHEMA.qprof_query_profiles_key
 ORDER BY qprof_query_profiles_key.transaction_id,
          qprof_query_profiles_key.statement_id,
          qprof_query_profiles_key.node_name,
          qprof_query_profiles_key.session_id,
          qprof_query_profiles_key.identifier,
          qprof_query_profiles_key.query,
          qprof_query_profiles_key.query_search_path,
          qprof_query_profiles_key.schema_name
SEGMENTED BY hash(qprof_query_profiles_key.transaction_id, qprof_query_profiles_key.statement_id, qprof_query_profiles_key.query_start_epoch, qprof_query_profiles_key.error_code, qprof_query_profiles_key.processed_row_count, qprof_query_profiles_key.reserved_extra_memory_b, qprof_query_profiles_key.is_executing, qprof_query_profiles_key.query_duration_us) ALL NODES;


