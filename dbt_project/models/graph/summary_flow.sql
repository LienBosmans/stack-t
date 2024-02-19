with event_flow as (
    select
        start_id,
        end_id,
        relation,
        type -- 'OBJECT_ATTRIBUTE_UPDATE', 'NEXT_EVENT', 'EVENT_TO_OBJECT'
    from {{ ref('event_flow') }}
),
object_snapshots as (
    select
        object_snapshot_id,
        object_id
    from {{ ref('object_snapshots') }}
),
next_events as (
    select
        object_id,
        object_type_id,
        event_id,
        event_type_id,
        event_timestamp,
        next_event_timestamp
    from {{ ref('prev_next_events') }}
),
prev_events as (
    select
        object_id,
        object_type_id,
        event_id,
        event_type_id,
        prev_event_id,
        prev_event_type_id
    from {{ ref('prev_next_events') }}
),
events as (
    select * from {{ ref('events') }}
),
event_types as (
    select * from {{ ref('event_types') }}
),
objects as (
    select * from {{ ref('objects') }}
),
object_types as (
    select * from {{ ref('object_types') }}
),
object_attribute_values as (
    select * from {{ ref('object_attribute_values') }}
),
event_outgoing_edges as (
    select
        event_types.id as start_id,
        md5(object_types.id || '-' || event_types.id) as end_id, -- object_type_snapshot_id
        event_flow.relation as relation,
        object_types.description as object_type_description,
        '(' || count(event_flow) || ') ' || event_flow.relation || ': ' || object_types.description as label,
        'EVENT_TYPE_TO_OBJECT_TYPE' as type,
        count(event_flow) as count_edges
    from
        event_flow
        inner join events
            on event_flow.start_id = events.id
        inner join event_types
            on events.event_type_id = event_types.id
        inner join object_snapshots
            on event_flow.end_id = object_snapshots.object_snapshot_id
        inner join objects
            on object_snapshots.object_id = objects.id
        inner join object_types
            on objects.object_type_id = object_types.id
    where
        event_flow.type = 'EVENT_TO_OBJECT'
    group by
        event_types.id,
        object_types.id,
        event_flow.relation,
        object_types.description
),
next_event_edges as (
    select
        md5(object_types.id || '-' || prev_events.prev_event_type_id) as start_id, -- object_type_snapshot_id
        event_types.id as end_id,
        '-' as relation,
        object_types.description as object_type_description,
        '(' || count(event_flow) || ') ' || object_types.description  as label,
        'NEXT_EVENT_TYPE' as type,
        count(event_flow) as count_edges
    from
        event_flow
        inner join events
            on event_flow.end_id = events.id
        inner join event_types
            on events.event_type_id = event_types.id
        inner join object_snapshots
            on event_flow.start_id = object_snapshots.object_snapshot_id
        inner join objects
            on object_snapshots.object_id = objects.id
        inner join object_types
            on objects.object_type_id = object_types.id
        inner join prev_events
            on (
                prev_events.object_id = objects.id
                and prev_events.event_id = events.id
            )
    where
        event_flow.type = 'NEXT_EVENT'
    group by
        event_types.id,
        prev_event_type_id,
        object_types.id,
        object_types.description
),
object_attribute_update_edges as (
    select
        md5(next_events.object_type_id || '-' || next_events.event_type_id) as start_id, -- object_type_snapshot_id
        md5(next_events.object_type_id || '-' || next_events.event_type_id) as end_id, -- object_type_snapshot_id
        '-' as relation,
        object_types.description as object_type_description,
        '(' || count(object_attribute_values) || ') ' || object_types.description as label,
        'OBJECT_ATTRIBUTE_UPDATE' as type,
        count(object_attribute_values) as count_edges
    from
        next_events
        inner join object_attribute_values
            on (
                next_events.object_id = object_attribute_values.object_id
                and next_events.event_timestamp < object_attribute_values.timestamp
                and (
                    next_events.next_event_timestamp > object_attribute_values.timestamp
                    or next_events.next_event_timestamp is null        
                )
            )
        inner join object_types
            on next_events.object_type_id = object_types.id
    group by
        next_events.object_type_id,
        next_events.event_type_id,
        object_types.description        
)

select * from event_outgoing_edges
UNION ALL
select * from next_event_edges
UNION ALL
select * from object_attribute_update_edges
