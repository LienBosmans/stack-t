{{ config(materialized='external',location='../neo4j/import/overview_object_to_object_edges.csv') }}

select
    'x' || start_id as ':START_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    relation as relation,
    relation_qualifier_value as relation_qualifier_value,
    'x' || end_id as ':END_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    total_edge_count as total_edge_count,
    '(' || total_edge_count || ') ' || relation as label,
    'object_to_object' as ':TYPE'
from
    {{ ref('overview_object_to_object_edges') }}
