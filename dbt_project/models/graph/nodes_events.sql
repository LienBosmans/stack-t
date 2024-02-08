{{ config(materialized='external',location='../neo4j/import/events.csv',options={'force_quote':'description'}) }}

with events as (
    select * from {{ ref('events') }}
),
event_types as (
    select * from {{ ref('event_types') }}
),
nodes_events as (
    select
        events.id as event_id,
        events.description as event_description,
        event_types.description as event_type,
        events.timestamp as event_timestamp
    from
        events
        inner join event_types
            on events.event_type_id = event_types.id
)

select 
    'X' || event_id as 'event_id:ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    event_timestamp as 'timestamp',
    event_description as 'description', -- force_quote
    replace(event_type,' ','_') as 'event_type',
    'EVENT' || ';' || replace(event_type,' ','_') as ':LABEL'
from nodes_events
