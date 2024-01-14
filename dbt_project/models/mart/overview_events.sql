with overview as (
    select
        event_types.id as event_type_id,
        event_types.description as event_type_description,
        min(events.timestamp) as first_event_timestamp,
        max(events.timestamp) as last_event_timestamp,
        count(distinct events.id) as event_count,
        count(distinct event_attributes.id) as event_attribute_count,
        count(distinct event_attribute_values.id) as event_attribute_value_update_count
    from
        {{ ref('event_types') }}
        left join {{ ref('events') }}
            on events.event_type_id = event_types.id
        left join {{ ref('event_attributes') }}
            on event_attributes.event_type_id = event_types.id
        left join {{ ref('event_attribute_values') }}
            on event_attribute_values.event_id = events.id
    group by
        event_types.id,
        event_types.description
    order by
        event_count desc
)

select
    event_type_description as event_type,
    first_event_timestamp,
    last_event_timestamp,
    event_count,
    event_attribute_count,
    event_attribute_value_update_count
from overview