

CREATE TABLE IF NOT EXISTS IMPORT_SCHEMA.qprof_execution_engine_profiles_key
(
    node_name varchar(128),
    user_id int,
    user_name varchar(128),
    session_id varchar(128),
    transaction_id int,
    statement_id int,
    plan_id int,
    operator_name varchar(128),
    operator_id int,
    baseplan_id int,
    path_id int,
    localplan_id int,
    activity_id int,
    resource_id int,
    counter_name varchar(128),
    counter_tag varchar(128),
    counter_value int,
    is_executing boolean,
    query_name varchar(128)
);


CREATE PROJECTION IF NOT EXISTS IMPORT_SCHEMA.qprof_execution_engine_profiles_key_super /*+basename(qprof_execution_engine_profiles_key),createtype(A)*/ 
(
 node_name,
 user_id,
 user_name,
 session_id,
 transaction_id,
 statement_id,
 plan_id,
 operator_name,
 operator_id,
 baseplan_id,
 path_id,
 localplan_id,
 activity_id,
 resource_id,
 counter_name,
 counter_tag,
 counter_value,
 is_executing,
 query_name
)
AS
 SELECT qprof_execution_engine_profiles_key.node_name,
        qprof_execution_engine_profiles_key.user_id,
        qprof_execution_engine_profiles_key.user_name,
        qprof_execution_engine_profiles_key.session_id,
        qprof_execution_engine_profiles_key.transaction_id,
        qprof_execution_engine_profiles_key.statement_id,
        qprof_execution_engine_profiles_key.plan_id,
        qprof_execution_engine_profiles_key.operator_name,
        qprof_execution_engine_profiles_key.operator_id,
        qprof_execution_engine_profiles_key.baseplan_id,
        qprof_execution_engine_profiles_key.path_id,
        qprof_execution_engine_profiles_key.localplan_id,
        qprof_execution_engine_profiles_key.activity_id,
        qprof_execution_engine_profiles_key.resource_id,
        qprof_execution_engine_profiles_key.counter_name,
        qprof_execution_engine_profiles_key.counter_tag,
        qprof_execution_engine_profiles_key.counter_value,
        qprof_execution_engine_profiles_key.is_executing,
        qprof_execution_engine_profiles_key.query_name
 FROM IMPORT_SCHEMA.qprof_execution_engine_profiles_key
 ORDER BY qprof_execution_engine_profiles_key.transaction_id,
          qprof_execution_engine_profiles_key.statement_id,
          qprof_execution_engine_profiles_key.node_name,
          qprof_execution_engine_profiles_key.plan_id,
          qprof_execution_engine_profiles_key.path_id,
          qprof_execution_engine_profiles_key.operator_id
SEGMENTED BY hash(qprof_execution_engine_profiles_key.user_id, qprof_execution_engine_profiles_key.transaction_id, qprof_execution_engine_profiles_key.statement_id, qprof_execution_engine_profiles_key.plan_id, qprof_execution_engine_profiles_key.operator_id, qprof_execution_engine_profiles_key.baseplan_id, qprof_execution_engine_profiles_key.path_id, qprof_execution_engine_profiles_key.localplan_id) ALL NODES;


SELECT MARK_DESIGN_KSAFE(1);

