{{ config(materialized='external',location='../neo4j/import/traces_directly_follow_edges.csv') }}

select
    'x' || start_id as ':START_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    relation as relation,
    relation_qualifier_value as relation_qualifier_value,
    'x' || end_id as ':END_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    object_description as description,
    case
        when relation is null
            then concat(edge_type,':',object_description)
        else concat(relation,':',object_description)
    end as label,
    edge_type as ':TYPE'
from
    {{ ref('traces_directly_follow_edges') }}
