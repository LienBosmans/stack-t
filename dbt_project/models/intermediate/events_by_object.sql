with events_by_object as (
    select
        objects.id as object_id,
        object_types.id as object_type_id,
        events.id as event_id,
        event_types.id as event_type_id,
        qualifiers.id as qualifier_id,
        objects.description as object_description,
        object_types.description as object_type_description,
        qualifiers.description as qualifier_description,
        events.description as event_description,
        event_types.description as event_type_description,
        events.timestamp as event_timestamp
    from
        {{ ref('objects') }}
        inner join {{ ref('event_to_object') }}
            on event_to_object.object_id = objects.id
        left join {{ ref('qualifiers') }}
            on event_to_object.qualifier_id = qualifiers.id
        inner join {{ ref('events') }}
            on event_to_object.event_id = events.id
        inner join {{ ref('object_types') }}
            on objects.object_type_id = object_types.id
        inner join {{ ref('event_types') }}
            on events.event_type_id = event_types.id
),
first_object_type_ts as (
    select
        object_type_id,
        min(event_timestamp) as first_event_timestamp
    from events_by_object
    group by object_type_id
),
first_object_ts as (
    select
        object_id,
        min(event_timestamp) as first_event_timestamp
    from events_by_object
    group by object_id
)

select 
    events_by_object.*
from
    events_by_object
    left join first_object_type_ts
        on events_by_object.object_type_id = first_object_type_ts.object_type_id
    left join first_object_ts
        on events_by_object.object_id = first_object_ts.object_id
order by
    first_object_type_ts.first_event_timestamp asc,
    first_object_ts.first_event_timestamp asc