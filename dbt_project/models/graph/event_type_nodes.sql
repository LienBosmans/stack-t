with event_types as (
    select * from {{ ref('event_types') }}
),
events as (
    select * from {{ ref('events') }}
),
event_attributes as (
    select * from {{ ref('event_attributes') }}
),
event_attribute_values as (
    select * from {{ ref('event_attribute_values') }}
),
event_type_nodes as (
    select
        event_types.id as event_type_id,
        event_types.description as event_type,
        count(event_attributes.id) as count_event_attributes
    from
        event_types
        left join event_attributes
            on event_types.id = event_attributes.event_type_id
    group by
        event_types.id,
        event_types.description
),
event_attribute_updates as (
    select
        events.id as event_id,
        events.event_type_id as event_type_id,
        event_attribute_values.event_attribute_id as event_attribute_id,
        count(event_attribute_values.id) as count_attribute_updates
    from 
        events
        inner join event_attribute_values
            on event_attribute_values.event_id = events.id
    group by
        events.id,
        events.event_type_id,
        event_attribute_values.event_attribute_id
),
event_attribute_stats as (
    select
        event_type_id,
        event_attribute_id,
        count(event_attribute_id) as count_events_used_attributes,
        min(count_attribute_updates) as min_count_attribute_updates,
        max(count_attribute_updates) as max_count_attribute_updates,
        avg(count_attribute_updates) as avg_count_attribute_updates,
        stddev_samp(count_attribute_updates) as stdv_count_attribute_updates
    from
        event_attribute_updates
    group by
        event_type_id,
        event_attribute_id
),
event_counts as (
    select
        events.event_type_id as event_type_id,
        count(events.id) as count_events
    from
        events
    group by
        events.event_type_id
),
event_type_attributes as (
    select 
        event_types.id as event_type_id,
        event_attributes.id as event_attribute_id,
        event_attributes.description as attribute_name,
        event_counts.count_events as count_events,
        event_attribute_stats.count_events_used_attributes as count_events_using_this_attribute,
        count_events_using_this_attribute::float / count_events::float as perc_events_using_this_attribute,
        event_attribute_stats.min_count_attribute_updates as min_count_attribute_updates,
        event_attribute_stats.max_count_attribute_updates as max_count_attribute_updates,
        event_attribute_stats.avg_count_attribute_updates as avg_count_attribute_updates,
        event_attribute_stats.stdv_count_attribute_updates as stdv_count_attribute_updates
    from
        event_types
        left join event_counts
            on event_counts.event_type_id = event_types.id
        left join event_attributes
            on event_attributes.event_type_id = event_types.id
        left join  event_attribute_stats
            on (
                event_attribute_stats.event_type_id = event_types.id
                and event_attribute_stats.event_attribute_id = event_attributes.id
            )        
),
event_type_attributes_pivoted as (
    select
        event_type_id,
        count_events,
        {% if dbt_utils.get_column_values(ref('event_attributes'),'description') != None %}
        {{ dbt_utils.pivot(
                'attribute_name',
                dbt_utils.get_column_values(ref('event_attributes'),'description'),
                agg='max',
                then_value='perc_events_using_this_attribute',
                else_value='null',
                prefix='attribute_',
                suffix='_usage_perc'
            )}},
        {{ dbt_utils.pivot(
                'attribute_name',
                dbt_utils.get_column_values(ref('event_attributes'),'description'),
                agg='max',
                then_value='avg_count_attribute_updates',
                else_value='null',
                prefix='attribute_',
                suffix='_avg_update_count'
            )}},
        {{ dbt_utils.pivot(
                'attribute_name',
                dbt_utils.get_column_values(ref('event_attributes'),'description'),
                agg='max',
                then_value='stdv_count_attribute_updates',
                else_value='null',
                prefix='attribute_',
                suffix='_stdv_update_count'
            )}}
        {% endif %}
    from
        event_type_attributes
    group by
        event_type_id,
        count_events
),
event_type_nodes_with_attributes as (
    select
        event_type_nodes.event_type_id as event_type_id,
        event_type_nodes.event_type as event_type,
        event_type_nodes.count_event_attributes as count_event_attributes,
        event_type_attributes_pivoted.* EXCLUDE (event_type_id)
    from
        event_type_nodes
        inner join event_type_attributes_pivoted
            on event_type_nodes.event_type_id = event_type_attributes_pivoted.event_type_id
)


select * from event_type_nodes_with_attributes 
