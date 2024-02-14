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
object_attributes as (
    select * from {{ ref('object_attributes') }}
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
),
object_snapshot_attributes as (
    select
        object_snapshots.object_snapshot_id,
        object_attributes.description as attribute_name,
        object_attribute_values.attribute_value as attribute_value
    from
        object_snapshots
        inner join objects
            on object_snapshots.object_id = objects.id
        inner join object_types
            on objects.object_type_id = object_types.id
        inner join object_attributes
            on object_attributes.object_type_id = object_types.id
        ASOF inner join object_attribute_values
            on (
                (
                    object_attribute_values.object_attribute_id = object_attributes.id
                    and object_attribute_values.object_id = object_snapshots.object_id
                )
                and object_snapshots.snapshot_timestamp >= object_attribute_values.timestamp
            )
),
object_snapshots_join_attributes as (
    select
        object_snapshots.object_snapshot_id as object_snapshot_id,
        object_snapshots.object_id as object_id,
        object_snapshots.snapshot_timestamp as snapshot_timestamp,
        object_snapshots.object_description as object_description,
        object_snapshots.object_type as object_type,
        object_snapshot_attributes.attribute_name as attribute_name,
        object_snapshot_attributes.attribute_value as attribute_value
    from
        object_snapshots
        left join object_snapshot_attributes
            on object_snapshots.object_snapshot_id = object_snapshot_attributes.object_snapshot_id
),
object_snapshots_with_attributes as (
    select
        object_snapshot_id,
        object_id,
        snapshot_timestamp,
        object_description,
        object_type,
        {% if dbt_utils.get_column_values(ref('object_attributes'),'description') != None %}
        {{ dbt_utils.pivot(
                'attribute_name',
                dbt_utils.get_column_values(ref('object_attributes'),'description'),
                agg='max',
                then_value='attribute_value',
                else_value='null'
            )}}
        {% endif %}
    from
        object_snapshots_join_attributes
    group by
        object_snapshot_id,
        object_id,
        snapshot_timestamp,
        object_description,
        object_type
)

select * from object_snapshots_with_attributes
