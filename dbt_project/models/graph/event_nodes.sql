with events as (
    select * from {{ ref('events') }}
),
event_types as (
    select * from {{ ref('event_types') }}
),
event_attributes as (
    select * from {{ ref('event_attributes') }}
),
event_attribute_values as (
    select * from {{ ref('event_attribute_values') }}
),
event_nodes as (
    select
        events.id as event_id,
        events.description as event_description,
        event_types.description as event_type,
        events.timestamp as event_timestamp
    from
        events
        inner join event_types
            on events.event_type_id = event_types.id
),
event_node_attributes as (
    select
        event_attribute_values.event_id as event_id,
        event_attributes.description as attribute_name,
        event_attribute_values.attribute_value as attribute_value
    from
        event_attribute_values
        inner join event_attributes
            on event_attribute_values.event_attribute_id = event_attributes.id
),
event_nodes_join_attributes as (
    select
        event_nodes.event_id as event_id,
        event_nodes.event_description as event_description,
        event_nodes.event_type as event_type,
        event_nodes.event_timestamp as event_timestamp,
        event_node_attributes.attribute_name as attribute_name,
        event_node_attributes.attribute_value as attribute_value
    from
        event_nodes
        left join event_node_attributes
            on event_nodes.event_id = event_node_attributes.event_id
),
event_nodes_with_attributes as (
    select
        event_id,
        event_description,
        event_type,
        event_timestamp,
        {{ dbt_utils.pivot(
                'attribute_name',
                dbt_utils.get_column_values(ref('event_attributes'),'description'),
                agg='max',
                then_value='attribute_value'
            )}}
    from
        event_nodes_join_attributes
    group by
        event_id,
        event_description,
        event_type,
        event_timestamp
)

select * from event_nodes_with_attributes
