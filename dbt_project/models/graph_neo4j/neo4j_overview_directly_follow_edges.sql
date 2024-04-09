{{ config(materialized='external',location='../neo4j/import/overview_directly_follow_edges.csv') }}

select
    'x' || start_id as ':START_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    relation as relation,
    relation_qualifier_value as relation_qualifier_value,
    'x' || end_id as ':END_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    object_type as object_type,
    total_edge_count as total_edge_count,
    case
        when relation is null
            then concat('(',total_edge_count,') ',edge_type,':',object_type)
        else concat('(',total_edge_count,') ',relation,':',object_type)
    end as label,
    edge_type as ':TYPE'
from
    {{ ref('overview_directly_follow_edges') }}
