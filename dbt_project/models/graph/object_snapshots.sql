with objects as ( 
    select * from {{ ref('objects') }}
),
event_to_object as (
    select * from {{ ref('event_to_object') }}
),
events as (
    select * from {{ ref('events') }}
),
object_attribute_values as (
    select * from {{ ref('object_attribute_values') }}
),
object_types as (
    select * from {{ ref('object_types') }}
),
object_snapshots_for_events as (
    select distinct
        objects.id as object_id,
        events.timestamp as snapshot_timestamp
    from
        objects
        inner join event_to_object
            on event_to_object.object_id = objects.id
        inner join events
            on event_to_object.event_id = events.id
),
object_snapshots_for_attribute_updates as (
    select distinct
        object_id,
        timestamp as snapshot_timestamp
    from
        object_attribute_values
),
object_snapshots_id_timestamp as (
    select distinct *
    from (
        select * from object_snapshots_for_events
        UNION ALL
        select * from object_snapshots_for_attribute_updates
    )
),
object_snapshots as (
    select
        md5(object_snapshots.object_id || '-' || object_snapshots.snapshot_timestamp) as object_snapshot_id,
        object_snapshots.object_id as object_id,
        object_snapshots.snapshot_timestamp as snapshot_timestamp,
        objects.description as object_description,
        object_types.description as object_type
    from 
        object_snapshots_id_timestamp as object_snapshots
        inner join objects
            on object_snapshots.object_id = objects.id
        inner join object_types
            on objects.object_type_id = object_types.id
)

select * from object_snapshots
