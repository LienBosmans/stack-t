{{ config(materialized='external',location='../neo4j/import/overview_event_type_nodes.csv') }}

select
    'x' || event_type_id as 'event_type_id:ID',  -- needs prefix letter, because neo4j does not accept id's that start with number
    event_type as event_type,
    event_count as total_node_count,
    '(' || event_count || ') ' || event_type as label,
    'EVENT_TYPE' || ';' || replace(event_type,' ','_') as ':LABEL'
from
    {{ ref('overview_event_type_nodes')}}
