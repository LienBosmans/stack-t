{{ config(materialized='external',location='../neo4j/import/object_type_relations.csv',options={'force_quote':'relation'}) }}

with edges_object_type_to_object_type as (
    select * from {{ ref('object_type_relations') }}
)
select 
    'X' || start_id as ':START_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    relation as 'relation', -- force_quote
    'X' || end_id as ':END_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    type as ':TYPE',
    count_relationships,
    count_distinct_source_objects,
    count_distinct_target_objects,
    count_distinct_source_objects || ' - ' || relation || ' (' || count_relationships || ') - ' || count_distinct_target_objects as label
from edges_object_type_to_object_type
