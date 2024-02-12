{{ config(materialized='external',location='../neo4j/import/events.csv',options={'force_quote':'description'}) }}

with nodes_events as (
    select * from {{ ref('event_nodes') }}
)

select 
    'X' || event_id as 'event_id:ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    event_timestamp as 'timestamp',
    event_description as 'description', -- force_quote
    replace(event_type,' ','_') as 'event_type',
    'EVENT' || ';' || replace(event_type,' ','_') as ':LABEL'
from nodes_events
