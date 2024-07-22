

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_dc_query_executions_key
(
    "time" timestamptz,
    node_name varchar(128),
    session_id varchar(128),
    user_id int,
    user_name varchar(128),
    transaction_id int,
    statement_id int,
    request_id int,
    execution_step varchar(128),
    completion_time timestamptz,
    query_name varchar(128)
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_dc_query_executions_key_super /*+basename(qprof_dc_query_executions_key),createtype(A)*/ 
(
 "time",
 node_name,
 session_id,
 user_id,
 user_name,
 transaction_id,
 statement_id,
 request_id,
 execution_step,
 completion_time,
 query_name
)
AS
 SELECT qprof_dc_query_executions_key."time",
        qprof_dc_query_executions_key.node_name,
        qprof_dc_query_executions_key.session_id,
        qprof_dc_query_executions_key.user_id,
        qprof_dc_query_executions_key.user_name,
        qprof_dc_query_executions_key.transaction_id,
        qprof_dc_query_executions_key.statement_id,
        qprof_dc_query_executions_key.request_id,
        qprof_dc_query_executions_key.execution_step,
        qprof_dc_query_executions_key.completion_time,
        qprof_dc_query_executions_key.query_name
 FROM IMPORT_SCHEMA.qprof_dc_query_executions_key
 ORDER BY qprof_dc_query_executions_key.transaction_id,
          qprof_dc_query_executions_key.statement_id,
          qprof_dc_query_executions_key.node_name,
          qprof_dc_query_executions_key."time",
          qprof_dc_query_executions_key.request_id,
          qprof_dc_query_executions_key.session_id,
          qprof_dc_query_executions_key.user_id,
          qprof_dc_query_executions_key.user_name
SEGMENTED BY hash(qprof_dc_query_executions_key."time", qprof_dc_query_executions_key.user_id, qprof_dc_query_executions_key.transaction_id, qprof_dc_query_executions_key.statement_id, qprof_dc_query_executions_key.request_id, qprof_dc_query_executions_key.completion_time, qprof_dc_query_executions_key.node_name, qprof_dc_query_executions_key.session_id) ALL NODES;

