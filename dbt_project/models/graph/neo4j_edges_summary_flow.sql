{{ config(materialized='external',location='../neo4j/import/summary_flow.csv',options={'force_quote':"('relation','object_type','label')"}) }}

with edges_summary_flow as (
    select * from {{ ref('summary_flow') }}
)

select 
    'X' || start_id as ':START_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    relation as 'relation', -- force_quote
    object_type_description as 'object_type', -- force_quote
    label as 'label', -- force_quote
    'X' || end_id as ':END_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    type as ':TYPE',
    count_edges as 'count_edges'
from edges_summary_flow
