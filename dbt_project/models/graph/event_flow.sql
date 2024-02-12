with object_snapshots as (
    select
        object_snapshot_id,
        object_id,
        object_description,
        snapshot_timestamp
    from {{ ref('object_snapshots') }}
),
event_to_object as (
    select * from {{ ref('event_to_object') }}
),
events as (
    select * from {{ ref('events') }}
),
qualifiers as (
    select * from {{ ref('qualifiers') }}
),
event_incoming_edges as (
    select
        object_snapshots.object_snapshot_id as start_id,
        events.id as end_id,
        qualifiers.description as relation,
        object_snapshots.object_description as object_description,
        relation || ': ' || object_description as label,
        'NEXT_EVENT' as type
    from
        event_to_object
        inner join events
            on event_to_object.event_id = events.id
        inner join object_snapshots
            on (
                object_snapshots.object_id = event_to_object.object_id
                and object_snapshots.snapshot_timestamp = events.timestamp
            )
        inner join qualifiers
            on event_to_object.qualifier_id = qualifiers.id
        
),
event_nodes as (
    select
        events.id as node_id,
        events.timestamp as node_timestamp,
        event_to_object.object_id as object_id,
        'EVENT' as node_type
    from
        events
        inner join event_to_object
            on event_to_object.event_id = events.id
),
object_update_nodes as (
    select
        object_snapshots.object_snapshot_id as node_id,
        object_snapshots.snapshot_timestamp as node_timestamp,
        object_snapshots.object_id as object_id,
        'OBJECT' as node_type
    from
        object_snapshots
        left join event_nodes
            on (
                event_nodes.object_id = object_snapshots.object_id
                and event_nodes.node_timestamp = object_snapshots.snapshot_timestamp
            )
    where
        event_nodes.node_id is null     
),
prev_nodes as (
    select * from event_nodes
    UNION ALL
    select * from object_update_nodes
),
object_snapshot_incoming_edges as (
    select
        prev_nodes.node_id as start_id,
        object_snapshots.object_snapshot_id as end_id,
        case
            when prev_nodes.node_type = 'OBJECT' then 'attribute update'
            else '-'
        end as relation,
        object_snapshots.object_description as object_description,
        relation || ': ' || object_description as label,
        case
            when prev_nodes.node_type = 'OBJECT' then 'OBJECT_ATTRIBUTE_UPDATE'
            else 'NEXT_OBJECT_SNAPSHOT'
        end as type,
    from
        object_snapshots
        ASOF inner join prev_nodes
            on (
                object_snapshots.object_id = prev_nodes.object_id
                and object_snapshots.snapshot_timestamp > prev_nodes.node_timestamp
            )
),
edges_event_flow as (
    select * from event_incoming_edges
    UNION ALL
    select * from object_snapshot_incoming_edges
)

select * from edges_event_flow
