 

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_host_resources_key
(
    host_name varchar(128),
    open_files_limit int,
    threads_limit int,
    core_file_limit_max_size_bytes int,
    processor_count int,
    processor_core_count int,
    processor_description varchar(8192),
    opened_file_count int,
    opened_socket_count int,
    opened_nonfile_nonsocket_count int,
    total_memory_bytes int,
    total_memory_free_bytes int,
    total_buffer_memory_bytes int,
    total_memory_cache_bytes int,
    total_swap_memory_bytes int,
    total_swap_memory_free_bytes int,
    disk_space_free_mb int,
    disk_space_used_mb int,
    disk_space_total_mb int,
    system_open_files int,
    system_max_files int,
    transaction_id int,
    statement_id int,
    query_name varchar(128)
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_host_resources_key_super /*+basename(qprof_host_resources_key),createtype(A)*/ 
(
 host_name,
 open_files_limit,
 threads_limit,
 core_file_limit_max_size_bytes,
 processor_count,
 processor_core_count,
 processor_description,
 opened_file_count,
 opened_socket_count,
 opened_nonfile_nonsocket_count,
 total_memory_bytes,
 total_memory_free_bytes,
 total_buffer_memory_bytes,
 total_memory_cache_bytes,
 total_swap_memory_bytes,
 total_swap_memory_free_bytes,
 disk_space_free_mb,
 disk_space_used_mb,
 disk_space_total_mb,
 system_open_files,
 system_max_files,
 transaction_id,
 statement_id,
 query_name
)
AS
 SELECT qprof_host_resources_key.host_name,
        qprof_host_resources_key.open_files_limit,
        qprof_host_resources_key.threads_limit,
        qprof_host_resources_key.core_file_limit_max_size_bytes,
        qprof_host_resources_key.processor_count,
        qprof_host_resources_key.processor_core_count,
        qprof_host_resources_key.processor_description,
        qprof_host_resources_key.opened_file_count,
        qprof_host_resources_key.opened_socket_count,
        qprof_host_resources_key.opened_nonfile_nonsocket_count,
        qprof_host_resources_key.total_memory_bytes,
        qprof_host_resources_key.total_memory_free_bytes,
        qprof_host_resources_key.total_buffer_memory_bytes,
        qprof_host_resources_key.total_memory_cache_bytes,
        qprof_host_resources_key.total_swap_memory_bytes,
        qprof_host_resources_key.total_swap_memory_free_bytes,
        qprof_host_resources_key.disk_space_free_mb,
        qprof_host_resources_key.disk_space_used_mb,
        qprof_host_resources_key.disk_space_total_mb,
        qprof_host_resources_key.system_open_files,
        qprof_host_resources_key.system_max_files,
        qprof_host_resources_key.transaction_id,
        qprof_host_resources_key.statement_id,
        qprof_host_resources_key.query_name
 FROM IMPORT_SCHEMA.qprof_host_resources_key
 ORDER BY qprof_host_resources_key.transaction_id,
          qprof_host_resources_key.statement_id,
          qprof_host_resources_key.host_name
SEGMENTED BY hash(qprof_host_resources_key.open_files_limit, qprof_host_resources_key.threads_limit, qprof_host_resources_key.core_file_limit_max_size_bytes, qprof_host_resources_key.processor_count, qprof_host_resources_key.processor_core_count, qprof_host_resources_key.opened_file_count, qprof_host_resources_key.opened_socket_count, qprof_host_resources_key.opened_nonfile_nonsocket_count) ALL NODES;




