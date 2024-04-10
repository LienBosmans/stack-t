{{ config(materialized='external',location='../neo4j/import/traces_object_to_object_edges.csv') }}

select
    'x' || start_id as ':START_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    relation as relation,
    relation_qualifier_value as relation_qualifier_value,
    'x' || end_id as ':END_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    relation as label,
    'object_to_object' as ':TYPE'
from
    {{ ref('traces_object_to_object_edges') }}
