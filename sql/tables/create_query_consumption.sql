

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_query_consumption_key
(
    start_time timestamptz,
    end_time timestamptz,
    session_id varchar(128),
    user_id int,
    user_name varchar(128),
    transaction_id int,
    statement_id int,
    cpu_cycles_us int,
    network_bytes_received int,
    network_bytes_sent int,
    data_bytes_read int,
    data_bytes_written int,
    data_bytes_loaded int,
    bytes_spilled int,
    input_rows int,
    input_rows_processed int,
    peak_memory_kb int,
    thread_count int,
    duration_ms int,
    resource_pool varchar(128),
    output_rows int,
    request_type varchar(128),
    label varchar(128),
    is_retry boolean,
    success boolean,
    query_name varchar(128)
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_query_consumption_key_super /*+basename(qprof_query_consumption_key),createtype(A)*/ 
(
 start_time,
 end_time,
 session_id,
 user_id,
 user_name,
 transaction_id,
 statement_id,
 cpu_cycles_us,
 network_bytes_received,
 network_bytes_sent,
 data_bytes_read,
 data_bytes_written,
 data_bytes_loaded,
 bytes_spilled,
 input_rows,
 input_rows_processed,
 peak_memory_kb,
 thread_count,
 duration_ms,
 resource_pool,
 output_rows,
 request_type,
 label,
 is_retry,
 success,
 query_name
)
AS
 SELECT qprof_query_consumption_key.start_time,
        qprof_query_consumption_key.end_time,
        qprof_query_consumption_key.session_id,
        qprof_query_consumption_key.user_id,
        qprof_query_consumption_key.user_name,
        qprof_query_consumption_key.transaction_id,
        qprof_query_consumption_key.statement_id,
        qprof_query_consumption_key.cpu_cycles_us,
        qprof_query_consumption_key.network_bytes_received,
        qprof_query_consumption_key.network_bytes_sent,
        qprof_query_consumption_key.data_bytes_read,
        qprof_query_consumption_key.data_bytes_written,
        qprof_query_consumption_key.data_bytes_loaded,
        qprof_query_consumption_key.bytes_spilled,
        qprof_query_consumption_key.input_rows,
        qprof_query_consumption_key.input_rows_processed,
        qprof_query_consumption_key.peak_memory_kb,
        qprof_query_consumption_key.thread_count,
        qprof_query_consumption_key.duration_ms,
        qprof_query_consumption_key.resource_pool,
        qprof_query_consumption_key.output_rows,
        qprof_query_consumption_key.request_type,
        qprof_query_consumption_key.label,
        qprof_query_consumption_key.is_retry,
        qprof_query_consumption_key.success,
        qprof_query_consumption_key.query_name
 FROM IMPORT_SCHEMA.qprof_query_consumption_key
 ORDER BY qprof_query_consumption_key.transaction_id,
          qprof_query_consumption_key.statement_id,
          qprof_query_consumption_key.label,
          qprof_query_consumption_key.start_time,
          qprof_query_consumption_key.end_time
SEGMENTED BY hash(qprof_query_consumption_key.start_time, qprof_query_consumption_key.end_time, qprof_query_consumption_key.user_id, qprof_query_consumption_key.transaction_id, qprof_query_consumption_key.statement_id, qprof_query_consumption_key.cpu_cycles_us, qprof_query_consumption_key.network_bytes_received, qprof_query_consumption_key.network_bytes_sent) ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);

