

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_collection_info_key
(
    transaction_id int,
    statement_id int,
    user_query_label varchar(256),
    user_query_comment varchar(512),
    project_name varchar(128),
    customer_name varchar(128),
    version varchar(512) DEFAULT version()
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_collection_info_key_super /*+basename(qprof_collection_info_key),createtype(L)*/ 
(
 transaction_id encoding rle,
 statement_id encoding rle,
 user_query_label encoding rle,
 user_query_comment encoding rle,
 project_name encoding rle,
 customer_name encoding rle,
 version encoding rle
)
AS
 SELECT qprof_collection_info_key.transaction_id,
        qprof_collection_info_key.statement_id,
        qprof_collection_info_key.user_query_label,
        qprof_collection_info_key.user_query_comment,
        qprof_collection_info_key.project_name,
        qprof_collection_info_key.customer_name,
        qprof_collection_info_key.version
 FROM IMPORT_SCHEMA.qprof_collection_info_key
 ORDER BY qprof_collection_info_key.transaction_id,
          qprof_collection_info_key.statement_id,
          qprof_collection_info_key.user_query_label,
          qprof_collection_info_key.user_query_comment,
          qprof_collection_info_key.project_name,
          qprof_collection_info_key.customer_name
SEGMENTED BY hash(qprof_collection_info_key.transaction_id, qprof_collection_info_key.statement_id, qprof_collection_info_key.user_query_label) ALL NODES;

