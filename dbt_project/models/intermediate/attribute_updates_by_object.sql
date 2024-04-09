with attribute_value_updates as (
    select
        objects.id as object_id,
        object_types.id as object_type_id,
        md5(string_agg(object_attributes.id,'|' ORDER BY object_attributes.description ASC)) as object_attribute_group_id,
        objects.description as object_description,
        object_types.description as object_type_description,
        string_agg(object_attributes.description,'|' ORDER BY object_attributes.description ASC) as object_attribute_group_description,
        object_attribute_values.timestamp as update_timestamp
    from
        {{ ref('objects') }} as objects
        inner join {{ ref('object_types') }} as object_types
            on objects.object_type_id = object_types.id
        inner join {{ ref('object_attribute_values') }} as object_attribute_values
            on object_attribute_values.object_id = objects.id
        inner join {{ ref('object_attributes') }} as object_attributes
            on object_attribute_values.object_attribute_id = object_attributes.id
    group by
        objects.id,
        object_types.id,
        objects.description,
        object_types.description,
        update_timestamp
    order by
        object_types.description asc,
        objects.description asc,
        object_attribute_values.timestamp asc
)

select * from attribute_value_updates
