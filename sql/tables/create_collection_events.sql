 

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_collection_events_key
(
    transaction_id int,
    statement_id int,
    table_name varchar(256),
    operation varchar(128),
    row_count int
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_collection_events_key_super /*+basename(qprof_collection_events_key),createtype(L)*/ 
(
 transaction_id,
 statement_id,
 table_name,
 operation,
 row_count
)
AS
 SELECT qprof_collection_events_key.transaction_id,
        qprof_collection_events_key.statement_id,
        qprof_collection_events_key.table_name,
        qprof_collection_events_key.operation,
        qprof_collection_events_key.row_count
 FROM IMPORT_SCHEMA.qprof_collection_events_key
 ORDER BY qprof_collection_events_key.transaction_id,
          qprof_collection_events_key.statement_id
SEGMENTED BY hash(qprof_collection_events_key.transaction_id, qprof_collection_events_key.statement_id) ALL NODES;




