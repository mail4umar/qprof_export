 

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_export_events_key
(
    table_name varchar(256),
    operation varchar(128),
    row_count int
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_export_events_key_super /*+basename(qprof_export_events_key),createtype(L)*/ 
(
 table_name,
 operation,
 row_count
)
AS
 SELECT qprof_export_events_key.table_name,
        qprof_export_events_key.operation,
        qprof_export_events_key.row_count
 FROM IMPORT_SCHEMA.qprof_export_events_key
 ORDER BY qprof_export_events_key.table_name,
          qprof_export_events_key.operation,
          qprof_export_events_key.row_count
SEGMENTED BY hash(qprof_export_events_key.row_count, qprof_export_events_key.operation, qprof_export_events_key.table_name) ALL NODES;




