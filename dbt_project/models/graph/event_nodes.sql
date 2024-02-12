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

select * from nodes_events
