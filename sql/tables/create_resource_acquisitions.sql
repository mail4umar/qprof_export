

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_resource_acquisitions_key
(
    node_name varchar(128),
    transaction_id int,
    statement_id int,
    request_type varchar(7),
    pool_id int,
    pool_name varchar(128),
    thread_count int,
    open_file_handle_count int,
    memory_inuse_kb int,
    queue_entry_timestamp timestamptz,
    acquisition_timestamp timestamptz,
    release_timestamp timestamptz,
    duration_ms int,
    is_executing boolean,
    query_name varchar(128)
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_resource_acquisitions_key_super /*+basename(qprof_resource_acquisitions_key),createtype(A)*/ 
(
 node_name,
 transaction_id,
 statement_id,
 request_type,
 pool_id,
 pool_name,
 thread_count,
 open_file_handle_count,
 memory_inuse_kb,
 queue_entry_timestamp,
 acquisition_timestamp,
 release_timestamp,
 duration_ms,
 is_executing,
 query_name
)
AS
 SELECT qprof_resource_acquisitions_key.node_name,
        qprof_resource_acquisitions_key.transaction_id,
        qprof_resource_acquisitions_key.statement_id,
        qprof_resource_acquisitions_key.request_type,
        qprof_resource_acquisitions_key.pool_id,
        qprof_resource_acquisitions_key.pool_name,
        qprof_resource_acquisitions_key.thread_count,
        qprof_resource_acquisitions_key.open_file_handle_count,
        qprof_resource_acquisitions_key.memory_inuse_kb,
        qprof_resource_acquisitions_key.queue_entry_timestamp,
        qprof_resource_acquisitions_key.acquisition_timestamp,
        qprof_resource_acquisitions_key.release_timestamp,
        qprof_resource_acquisitions_key.duration_ms,
        qprof_resource_acquisitions_key.is_executing,
        qprof_resource_acquisitions_key.query_name
 FROM IMPORT_SCHEMA.qprof_resource_acquisitions_key
 ORDER BY qprof_resource_acquisitions_key.transaction_id,
          qprof_resource_acquisitions_key.statement_id,
          qprof_resource_acquisitions_key.node_name,
          qprof_resource_acquisitions_key.request_type,
          qprof_resource_acquisitions_key.pool_name,
          qprof_resource_acquisitions_key.duration_ms
SEGMENTED BY hash(qprof_resource_acquisitions_key.transaction_id, qprof_resource_acquisitions_key.statement_id, qprof_resource_acquisitions_key.request_type, qprof_resource_acquisitions_key.pool_id, qprof_resource_acquisitions_key.thread_count, qprof_resource_acquisitions_key.open_file_handle_count, qprof_resource_acquisitions_key.memory_inuse_kb, qprof_resource_acquisitions_key.queue_entry_timestamp) ALL NODES;


