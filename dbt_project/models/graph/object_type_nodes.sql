with object_types as (
    select * from {{ ref('object_types') }}
),
objects as (
    select * from {{ ref('objects') }}
),
object_attributes as (
    select * from {{ ref('object_attributes') }}
),
object_attribute_values as (
    select * from {{ ref('object_attribute_values') }}
),
object_type_nodes as (
    select
        object_types.id as object_type_id,
        object_types.description as object_type,
        count(object_attributes.id) as count_object_attributes
    from
        object_types
        left join object_attributes
            on object_types.id = object_attributes.object_type_id
    group by
        object_types.id,
        object_types.description
),
object_attribute_updates as (
    select
        objects.id as object_id,
        objects.object_type_id as object_type_id,
        object_attribute_values.object_attribute_id as object_attribute_id,
        count(object_attribute_values.id) as count_attribute_updates
    from 
        objects
        inner join object_attribute_values
            on object_attribute_values.object_id = objects.id
    group by
        objects.id,
        objects.object_type_id,
        object_attribute_values.object_attribute_id
),
object_attribute_stats as (
    select
        object_type_id,
        object_attribute_id,
        count(object_attribute_id) as count_objects_used_attributes,
        min(count_attribute_updates) as min_count_attribute_updates,
        max(count_attribute_updates) as max_count_attribute_updates,
        avg(count_attribute_updates) as avg_count_attribute_updates,
        stddev_samp(count_attribute_updates) as stdv_count_attribute_updates
    from
        object_attribute_updates
    group by
        object_type_id,
        object_attribute_id
),
object_counts as (
    select
        objects.object_type_id as object_type_id,
        count(objects.id) as count_objects
    from
        objects
    group by
        objects.object_type_id
),
object_type_attributes as (
    select 
        object_types.id as object_type_id,
        object_attributes.id as object_attribute_id,
        object_attributes.description as attribute_name,
        object_counts.count_objects as count_objects,
        object_attribute_stats.count_objects_used_attributes as count_objects_using_this_attribute,
        count_objects_using_this_attribute::float / count_objects::float as perc_objects_using_this_attribute,
        object_attribute_stats.min_count_attribute_updates as min_count_attribute_updates,
        object_attribute_stats.max_count_attribute_updates as max_count_attribute_updates,
        object_attribute_stats.avg_count_attribute_updates as avg_count_attribute_updates,
        object_attribute_stats.stdv_count_attribute_updates as stdv_count_attribute_updates
    from
        object_types
        left join object_counts
            on object_counts.object_type_id = object_types.id
        left join object_attributes
            on object_attributes.object_type_id = object_types.id
        left join  object_attribute_stats
            on (
                object_attribute_stats.object_type_id = object_types.id
                and object_attribute_stats.object_attribute_id = object_attributes.id
            )        
),
object_type_attributes_pivoted as (
    select
        object_type_id,
        count_objects,
        {% if dbt_utils.get_column_values(ref('object_attributes'),'description') != None %}
        {{ dbt_utils.pivot(
                'attribute_name',
                dbt_utils.get_column_values(ref('object_attributes'),'description'),
                agg='max',
                then_value='perc_objects_using_this_attribute',
                else_value='null',
                prefix='attribute_',
                suffix='_usage_perc'
            )}},
        {{ dbt_utils.pivot(
                'attribute_name',
                dbt_utils.get_column_values(ref('object_attributes'),'description'),
                agg='max',
                then_value='avg_count_attribute_updates',
                else_value='null',
                prefix='attribute_',
                suffix='_avg_update_count'
            )}},
        {{ dbt_utils.pivot(
                'attribute_name',
                dbt_utils.get_column_values(ref('object_attributes'),'description'),
                agg='max',
                then_value='stdv_count_attribute_updates',
                else_value='null',
                prefix='attribute_',
                suffix='_stdv_update_count'
            )}}
        {% endif %}
    from
        object_type_attributes
    group by
        object_type_id,
        count_objects
),
object_type_nodes_with_attributes as (
    select
        object_type_nodes.object_type_id as object_type_id,
        object_type_nodes.object_type as object_type,
        object_type_nodes.count_object_attributes as count_object_attributes,
        object_type_attributes_pivoted.* EXCLUDE (object_type_id)
    from
        object_type_nodes
        inner join object_type_attributes_pivoted
            on object_type_nodes.object_type_id = object_type_attributes_pivoted.object_type_id
)


select * from object_type_nodes_with_attributes 
