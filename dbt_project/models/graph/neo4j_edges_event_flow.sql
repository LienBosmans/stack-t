{{ config(materialized='external',location='../neo4j/import/event_flow.csv',options={'force_quote':"('relation','object_description','label')"}) }}

with edges_event_flow as (
    select * from {{ ref('event_flow') }}
)

select 
    'X' || start_id as ':START_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    relation as 'relation', -- force_quote
    object_description as 'object_description', -- force_quote
    label as 'label', -- force_quote
    'X' || end_id as ':END_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    type as ':TYPE'
from edges_event_flow
