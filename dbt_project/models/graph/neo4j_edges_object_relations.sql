{{ config(materialized='external',location='../neo4j/import/object_relations.csv',options={'force_quote':'relation'}) }}

with edges_object_to_object as (
    select * from {{ ref('object_relations') }}
)
select 
    'X' || start_id as ':START_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    relation as 'relation', -- force_quote
    'X' || end_id as ':END_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    type as ':TYPE'
from edges_object_to_object
