with overview as(
    select
        object_types.id as object_type_id,
        object_types.description as object_type_description,
        min(object_attribute_values.timestamp) as first_object_update_timestamp,
        max(object_attribute_values.timestamp) as last_object_update_timestamp,
        count(distinct objects.id) as object_count,
        count(distinct object_attributes.id) as object_attribute_count,
        count(distinct object_attribute_values.id) as object_attribute_value_update_count
    from
        {{ ref('object_types') }}
        left join {{ ref('objects') }}
            on objects.object_type_id = object_types.id
        left join {{ ref('object_attributes') }}
            on object_attributes.object_type_id = object_types.id
        left join {{ ref('object_attribute_values') }}
            on object_attribute_values.object_id = objects.id
    group by
        object_types.id,
        object_types.description
    order by
        object_count desc
)

select
    object_type_description as object_type,
    first_object_update_timestamp,
    last_object_update_timestamp,
    object_count,
    object_attribute_count,
    object_attribute_value_update_count
from overview