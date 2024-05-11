with unpivoted_table as (
    select
        events.id as ocel_id,
        events.timestamp as ocel_time,
        event_attributes.description as attribute_column_name,
        event_attribute_values.attribute_value as attribute_value,
        event_attributes.datatype as attribute_datatype,
        lower(replace(event_types.description,' ','_')) as ocel_type_map
    from
        {{ ref('events') }}
        left join {{ ref('event_attribute_values') }}
            on events.id = event_attribute_values.event_id
        left join {{ ref('event_attributes') }}
            on event_attributes.id = event_attribute_values.event_attribute_id
        inner join {{ ref('event_types') }}
            on events.event_type_id = event_types.id
)

select * from unpivoted_table
