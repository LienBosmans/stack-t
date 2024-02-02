with prev_overview as (
    select
        object_type_id,
        event_type_id,
        prev_event_type_id as linked_event_type_id,
        object_type_id  as linked_object_type_id,
        'prev_event_this_object' as event_relationship,
        count(*) as count_events,
        to_seconds(avg(date_diff('second', event_timestamp, prev_event_timestamp))::bigint)::interval as avg_interval_between_linked_events
    from
        {{ ref('prev_next_events') }}
    group by
        object_type_id,
        event_type_id,
        prev_event_type_id
),
next_overview as (
    select
        object_type_id,
        event_type_id,
        next_event_type_id as linked_event_type_id,
        object_type_id  as linked_object_type_id,
        'next_event_this_object' as event_relationship,
        count(*) as count_events,
        to_seconds(avg(date_diff('second', event_timestamp, next_event_timestamp))::bigint)::interval as avg_interval_between_linked_events
    from
        {{ ref('prev_next_events') }}
    group by
        object_type_id,
        event_type_id,
        next_event_type_id
),
-- prev_linked_overview as (
--     select
--         object_type_id,
--         event_type_id,
--         prev_linked_event_type_id as linked_event_type_id,
--         prev_linked_object_type_id as linked_object_type_id,
--         'prev_event_linked_objects' as event_relationship,
--         count(*) as count_events,
--         to_seconds(avg(date_diff('second', event_timestamp, prev_linked_event_timestamp))::bigint)::interval as avg_interval_between_linked_events
--     from
--         ref('prev_next_events_linked_objects')
--     group by
--         object_type_id,
--         event_type_id,
--         prev_linked_event_type_id,
--         prev_linked_object_type_id
-- ),
-- next_linked_overview as (
--     select
--         object_type_id,
--         event_type_id,
--         next_linked_event_type_id as linked_event_type_id,
--         next_linked_object_type_id as linked_object_type_id,
--         'next_event_linked_objects' as event_relationship,
--         count(*) as count_events,
--         to_seconds(avg(date_diff('second', event_timestamp, next_linked_event_timestamp))::bigint)::interval as avg_interval_between_linked_events
--     from
--         ref('prev_next_events_linked_objects')
--     group by
--         object_type_id,
--         event_type_id,
--         next_linked_event_type_id,
--         next_linked_object_type_id
-- ),
overview as (
    select * from prev_overview
    UNION ALL
    select * from next_overview
    -- UNION ALL
    -- select * from prev_linked_overview
    -- UNION ALL
    -- select * from next_linked_overview
),
added_type_descriptions as (
    select
        main_object_types.description as object_type,
        main_event_types.description as event_type,
        overview.event_relationship,
        linked_object_types.description as linked_object_type,
        linked_event_types.description as linked_event_type,
        overview.count_events as event_count,
        overview.avg_interval_between_linked_events as avg_interval_between_events
    from
        overview
        left join {{ ref('object_types') }} as main_object_types
            on main_object_types.id = overview.object_type_id
        left join {{ ref('object_types') }} as linked_object_types
            on linked_object_types.id = overview.linked_object_type_id
        left join {{ ref('event_types') }} as main_event_types
            on main_event_types.id = overview.event_type_id
        left join {{ ref('event_types') }} as linked_event_types
            on linked_event_types.id = overview.linked_event_type_id
)

select * 
from added_type_descriptions 
order by
    object_type asc,
    event_type asc,
    event_relationship asc,
    linked_object_type asc,
    linked_event_type asc
