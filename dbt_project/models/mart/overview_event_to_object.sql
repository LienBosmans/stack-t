with overview as (
    select
        object_types.id as object_type_id,
        event_types.id as event_type_id,
        qualifiers.id as qualifier_id,
        qualifiers.description as relation_qualifier,
        object_types.description as object_type_description,
        event_types.description as event_type_description,
        count(distinct event_to_object.id) as event_to_object_count,
        count(distinct events.id) as event_count,
        count(distinct objects.id) as object_count,
        min(events.timestamp) as first_event_timestamp,
        max(events.timestamp) as last_event_timestamp
    from
        {{ ref('event_to_object') }}
        left join {{ ref('qualifiers') }}
            on event_to_object.qualifier_id = qualifiers.id
        inner join {{ ref('events') }}
            on events.id = event_to_object.event_id
        inner join {{ ref('event_types') }}
            on events.event_type_id = event_types.id
        inner join {{ ref('objects') }}
            on objects.id = event_to_object.object_id
        inner join {{ ref('object_types') }}
            on objects.object_type_id = object_types.id
    group by
        object_types.id,
        event_types.id,
        qualifiers.id,
        qualifiers.description,
        object_types.description,
        event_types.description,
    order by
        event_type_description asc,
        event_to_object_count desc
)

select
    event_to_object_count as relation_count,
    first_event_timestamp,
    last_event_timestamp,
    event_count,
    event_type_description as event_type,
    relation_qualifier,
    object_type_description as object_type,
    object_count
from overview