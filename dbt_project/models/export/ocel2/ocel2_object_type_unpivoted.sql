with unpivoted_table as (
    select
        objects.id as ocel_id,
        case 
            when object_attribute_values.timestamp is null then make_date(1970,1,1)::datetime -- default NULL date in ocel2
            else object_attribute_values.timestamp
        end as ocel_time,
        object_attributes.description as attribute_column_name,
        object_attributes.description as ocel_changed_field,
        object_attribute_values.attribute_value as attribute_value,
        object_attributes.datatype as attribute_datatype,
        lower(replace(object_types.description,' ','_')) as ocel_type_map
    from
        {{ ref('objects') }}
        left join {{ ref('object_attribute_values') }}
            on objects.id = object_attribute_values.object_id
        left join {{ ref('object_attributes') }}
            on object_attributes.id = object_attribute_values.object_attribute_id
        inner join {{ ref('object_types') }}
            on objects.object_type_id = object_types.id
)

select * from unpivoted_table
