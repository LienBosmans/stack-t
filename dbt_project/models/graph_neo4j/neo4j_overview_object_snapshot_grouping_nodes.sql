{{ config(materialized='external',location='../neo4j/import/overview_object_snapshot_grouping_nodes.csv') }}

select
    'x' || object_snapshot_grouping_id as 'object_snapshot_grouping_id:ID',  -- needs prefix letter, because neo4j does not accept id's that start with number
    object_type as object_type,
    prev_event_type as prev_event_type,
    object_attribute_group as object_attribute_group,
    total_node_count as total_node_count,
    '(' || total_node_count || ') ' || object_type as label,
    'OBJECT_SNAPSHOT_GROUPING' || ';' || replace(object_type,' ','_') as ':LABEL'
from
    {{ ref('overview_object_snapshot_grouping_nodes')}}
