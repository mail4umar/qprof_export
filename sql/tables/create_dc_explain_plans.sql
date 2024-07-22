

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_dc_explain_plans_key
(
    "time" timestamptz,
    node_name varchar(128),
    session_id varchar(128),
    user_id int,
    user_name varchar(128),
    transaction_id int,
    statement_id int,
    request_id int,
    path_id int,
    path_line_index int,
    path_line varchar(64000),
    query_name varchar(128)
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_dc_explain_plans_key_super /*+basename(qprof_dc_explain_plans_key),createtype(A)*/ 
(
 "time",
 node_name,
 session_id,
 user_id,
 user_name,
 transaction_id,
 statement_id,
 request_id,
 path_id,
 path_line_index,
 path_line,
 query_name
)
AS
 SELECT qprof_dc_explain_plans_key."time",
        qprof_dc_explain_plans_key.node_name,
        qprof_dc_explain_plans_key.session_id,
        qprof_dc_explain_plans_key.user_id,
        qprof_dc_explain_plans_key.user_name,
        qprof_dc_explain_plans_key.transaction_id,
        qprof_dc_explain_plans_key.statement_id,
        qprof_dc_explain_plans_key.request_id,
        qprof_dc_explain_plans_key.path_id,
        qprof_dc_explain_plans_key.path_line_index,
        qprof_dc_explain_plans_key.path_line,
        qprof_dc_explain_plans_key.query_name
 FROM IMPORT_SCHEMA.qprof_dc_explain_plans_key
 ORDER BY qprof_dc_explain_plans_key.transaction_id,
          qprof_dc_explain_plans_key.statement_id,
          qprof_dc_explain_plans_key.node_name,
          qprof_dc_explain_plans_key."time",
          qprof_dc_explain_plans_key.session_id,
          qprof_dc_explain_plans_key.user_id,
          qprof_dc_explain_plans_key.user_name,
          qprof_dc_explain_plans_key.request_id
SEGMENTED BY hash(qprof_dc_explain_plans_key."time", qprof_dc_explain_plans_key.user_id, qprof_dc_explain_plans_key.transaction_id, qprof_dc_explain_plans_key.statement_id, qprof_dc_explain_plans_key.request_id, qprof_dc_explain_plans_key.path_id, qprof_dc_explain_plans_key.path_line_index, qprof_dc_explain_plans_key.node_name) ALL NODES;


