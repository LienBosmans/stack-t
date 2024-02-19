with events as (
    select * from {{ ref('events') }}
),
objects as (
    select * from {{ ref('objects') }}
),
event_types as (
    select * from {{ ref('event_types') }}
),
object_types as (
    select * from {{ ref('object_types') }}
),
event_to_object as (
    select * from {{ ref('event_to_object') }}
),
object_types_linked_to_event_types as (
    select distinct
        object_types.id as object_type_id,
        object_types.description as object_type_description,
        event_types.id as event_type_id
    from
        objects
        inner join event_to_object
            on event_to_object.object_id = objects.id
        inner join events
            on event_to_object.event_id = events.id
        inner join object_types
            on objects.object_type_id = object_types.id
        inner join event_types
            on events.event_type_id = event_types.id
),
object_type_snapshots as (
    select
        md5(object_type_id || '-' || event_type_id) as object_type_snapshot_id,
        object_type_id,
        event_type_id,
        object_type_description
    from
        object_types_linked_to_event_types
)

select * from object_type_snapshots
