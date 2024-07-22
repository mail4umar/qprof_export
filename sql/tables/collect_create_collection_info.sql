

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.collection_info
(
    transaction_id int,
    statement_id int,
    user_query_label varchar(256),
    user_query_comment varchar(512),
    project_name varchar(128),
    customer_name varchar(128),
    version varchar(512) DEFAULT version()
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.collection_info_super /*+basename(collection_info),createtype(L)*/ 
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
 SELECT collection_info.transaction_id,
        collection_info.statement_id,
        collection_info.user_query_label,
        collection_info.user_query_comment,
        collection_info.project_name,
        collection_info.customer_name,
        collection_info.version
 FROM IMPORT_SCHEMA.collection_info
 ORDER BY collection_info.transaction_id,
          collection_info.statement_id,
          collection_info.user_query_label,
          collection_info.user_query_comment,
          collection_info.project_name,
          collection_info.customer_name
SEGMENTED BY hash(collection_info.transaction_id, collection_info.statement_id, collection_info.user_query_label) ALL NODES;
