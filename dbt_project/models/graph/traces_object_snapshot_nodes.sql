with object_snapshot_nodes as (
    select
        object_snapshots.object_snapshot_id as snapshot_id,
        object_snapshots.object_id as object_id,
        objects.description as object_description,
        object_snapshots.object_type_id as object_type_id,
        object_types.description as object_type,
        object_snapshots.snapshot_timestamp  as snapshot_timestamp,
        false as is_dummy
    from
        {{ ref('object_snapshots') }}
        inner join {{ ref('object_types') }}
            on object_snapshots.object_type_id = object_types.id
        inner join {{ ref('objects') }}
            on object_snapshots.object_id = objects.id
),
dummy_first_nodes as (
    select
        md5(objects.id || 'first') as snapshot_id,
        objects.id as object_id,
        objects.description as object_description,
        objects.object_type_id as object_type_id,
        object_types.description as object_type,
        prev_next_object_snapshots.snapshot_timestamp - (INTERVAL 1 seconds) as snapshot_timestamp,
        true as is_dummy
    from
        {{ ref('objects') }}
        inner join {{ ref('object_types') }}
            on objects.object_type_id = object_types.id
        inner join {{ ref('prev_next_object_snapshots') }}
            on (
                objects.id = prev_next_object_snapshots.object_id
                and prev_object_snapshot_id is null
            )
),
dummy_last_nodes as (
    select
        md5(objects.id || 'last') as snapshot_id,
        objects.id as object_id,
        objects.description as object_description,
        objects.object_type_id as object_type_id,
        object_types.description as object_type,
        prev_next_object_snapshots.snapshot_timestamp + (INTERVAL 1 seconds) as snapshot_timestamp,
        true as is_dummy
    from
        {{ ref('objects') }}
        inner join {{ ref('object_types') }}
            on objects.object_type_id = object_types.id
        inner join {{ ref('prev_next_object_snapshots') }}
            on (
                objects.id = prev_next_object_snapshots.object_id
                and next_object_snapshot_id is null
            )
),
all_nodes as (
    select * from object_snapshot_nodes
    UNION ALL
    select * from dummy_first_nodes
    UNION ALL
    select * from dummy_last_nodes
),
object_snapshot_node_attributes as (
    select
        object_attribute_values.object_id as object_id,
        object_attributes.description as attribute_name,
        object_attribute_values.attribute_value as attribute_value,
        object_attribute_values.timestamp as valid_from_timestamp
    from
        {{ ref('object_attribute_values') }}
        inner join {{ ref('object_attributes') }}
            on object_attribute_values.object_attribute_id = object_attributes.id
),
object_snapshot_nodes_join_attributes as (
    select
        object_snapshot_nodes.snapshot_id as snapshot_id,
        object_snapshot_nodes.object_id as object_id,
        object_snapshot_nodes.object_description as object_description,
        object_snapshot_nodes.object_type as object_type,
        object_snapshot_nodes.snapshot_timestamp as snapshot_timestamp,
        object_snapshot_nodes.is_dummy as is_dummy_node,
        object_snapshot_node_attributes.attribute_name as attribute_name,
        object_snapshot_node_attributes.attribute_value as attribute_value
    from
        all_nodes as object_snapshot_nodes
        ASOF left join object_snapshot_node_attributes
            on (
                object_snapshot_nodes.object_id = object_snapshot_node_attributes.object_id
                and object_snapshot_nodes.snapshot_timestamp >= object_snapshot_node_attributes.valid_from_timestamp
            )
),
object_snapshot_nodes_with_attributes as (
    select
        snapshot_id,
        object_id,
        object_description,
        object_type,
        snapshot_timestamp,
        is_dummy_node
        {% if dbt_utils.get_column_values(ref('object_attributes'),'description') != None %}
        ,{{ dbt_utils.pivot(
                'attribute_name',
                dbt_utils.get_column_values(ref('object_attributes'),'description'),
                agg='max',
                then_value='attribute_value',
                else_value='null'
            )}}
        {% endif %}
    from
        object_snapshot_nodes_join_attributes
    group by
        snapshot_id,
        object_id,
        object_description,
        object_type,
        snapshot_timestamp,
        is_dummy_node
)

select * from object_snapshot_nodes_with_attributes
