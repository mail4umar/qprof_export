

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_dc_requests_issued_key
(
    "time" timestamptz,
    node_name varchar(128),
    session_id varchar(128),
    user_id int,
    user_name varchar(128),
    transaction_id int,
    statement_id int,
    request_id int,
    request_type varchar(128),
    label varchar(128),
    client_label varchar(64000),
    search_path varchar(64000),
    query_start_epoch int,
    request varchar(64000),
    is_retry boolean,
    digest int,
    query_name varchar(128)
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_dc_requests_issued_key_super /*+basename(qprof_dc_requests_issued_key),createtype(A)*/ 
(
 "time",
 node_name,
 session_id,
 user_id,
 user_name,
 transaction_id,
 statement_id,
 request_id,
 request_type,
 label,
 client_label,
 search_path,
 query_start_epoch,
 request,
 is_retry,
 digest,
 query_name
)
AS
 SELECT qprof_dc_requests_issued_key."time",
        qprof_dc_requests_issued_key.node_name,
        qprof_dc_requests_issued_key.session_id,
        qprof_dc_requests_issued_key.user_id,
        qprof_dc_requests_issued_key.user_name,
        qprof_dc_requests_issued_key.transaction_id,
        qprof_dc_requests_issued_key.statement_id,
        qprof_dc_requests_issued_key.request_id,
        qprof_dc_requests_issued_key.request_type,
        qprof_dc_requests_issued_key.label,
        qprof_dc_requests_issued_key.client_label,
        qprof_dc_requests_issued_key.search_path,
        qprof_dc_requests_issued_key.query_start_epoch,
        qprof_dc_requests_issued_key.request,
        qprof_dc_requests_issued_key.is_retry,
        qprof_dc_requests_issued_key.digest,
        qprof_dc_requests_issued_key.query_name
 FROM IMPORT_SCHEMA.qprof_dc_requests_issued_key
 ORDER BY qprof_dc_requests_issued_key.transaction_id,
          qprof_dc_requests_issued_key.statement_id,
          qprof_dc_requests_issued_key.node_name,
          qprof_dc_requests_issued_key.label,
          qprof_dc_requests_issued_key.request_id
SEGMENTED BY hash(qprof_dc_requests_issued_key."time", qprof_dc_requests_issued_key.user_id, qprof_dc_requests_issued_key.transaction_id, qprof_dc_requests_issued_key.statement_id, qprof_dc_requests_issued_key.request_id, qprof_dc_requests_issued_key.query_start_epoch, qprof_dc_requests_issued_key.is_retry, qprof_dc_requests_issued_key.digest) ALL NODES;
