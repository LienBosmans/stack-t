import duckdb

# Change object_type for which to export here!

object_type_description = 'customer'
export_csv_file_path = '../exports/csv/customer_event_log.csv'

# ATTACH DuckDB database

duck_db = 'dev.duckdb'
sql_query = '''
ATTACH ''' + "'" + duck_db + "'" + ''' as duck_db;
'''
duckdb.sql(sql_query)

# todo: write a query that gets a simple event log and writes it to csv

sql_query = '''
with filtered_objects as (
    select
        objects.id as case_id,
        objects.description as object_description
    from
        duck_db.main_stackt.object_types
        inner join duck_db.main_stackt.objects
            on (
                object_types.description = ''' + "'" + object_type_description + "'" + '''
                and object_types.id = objects.object_type_id
            )
),
filtered_events_via_event_to_object as (
    select
        filtered_objects.case_id,
        filtered_objects.object_description,
        events.id as event_id,
        events.timestamp as event_timestamp,
        events.event_type_id
    from
        filtered_objects
        left join duck_db.main_stackt.event_to_object
            on filtered_objects.case_id = event_to_object.object_id
        inner join duck_db.main_stackt.events
            on event_to_object.event_id = events.id
),
filtered_events_via_event_to_object_attribute_value as (
    select
        filtered_objects.case_id,
        filtered_objects.object_description,
        events.id as event_id,
        events.timestamp as event_timestamp,
        events.event_type_id
    from
        filtered_objects
        inner join duck_db.main_stackt.object_attribute_values
            on filtered_objects.case_id = object_attribute_values.object_id
        inner join duck_db.main_stackt.event_to_object_attribute_value
            on object_attribute_values.id = event_to_object_attribute_value.object_attribute_value_id
        inner join duck_db.main_stackt.events
            on event_to_object_attribute_value.event_id = events.id
),
filtered_events as (
    select distinct 
        case_id,
        object_description,
        event_id,
        event_timestamp,
        event_type_id
    from
        (
            select * from filtered_events_via_event_to_object
            UNION ALL select * from filtered_events_via_event_to_object_attribute_value
        )
),
unpivoted_event_attributes as (
    select
        filtered_events.event_id,
        filtered_events.case_id,
        event_attributes.description as attribute,
        event_attribute_values.attribute_value
    from
        filtered_events
        inner join duck_db.main_stackt.event_attribute_values
            on filtered_events.event_id = event_attribute_values.event_id
        inner join duck_db.main_stackt.event_attributes
            on event_attribute_values.event_attribute_id = event_attributes.id
),
unpivoted_object_attributes as (
    select
        filtered_events.event_id,
        filtered_events.case_id,
       object_attributes.description as attribute,
        object_attribute_values.attribute_value
    from
        filtered_events
        ASOF inner join duck_db.main_stackt.object_attribute_values
            on (
                filtered_events.case_id = object_attribute_values.object_id
                and filtered_events.event_timestamp >= object_attribute_values.timestamp
            )
        inner join duck_db.main_stackt.object_attributes
            on object_attribute_values.object_attribute_id = object_attributes.id
),
unpivoted_attributes as (
    select * from unpivoted_event_attributes
    UNION ALL select * from unpivoted_object_attributes
),
pivoted_attributes as (
    pivot 
        unpivoted_attributes
    on 
        attribute
    using 
        first(attribute_value)
    group by
        case_id,
        event_id
),
event_log_table as (
    select
        filtered_events.case_id,
        filtered_events.event_id,
        filtered_events.object_description as case,
        filtered_events.event_timestamp,
        event_types.description as event_type,
        pivoted_attributes.* EXCLUDE (case_id,event_id)
    from 
        filtered_events
        left join duck_db.main_stackt.event_types
            on filtered_events.event_type_id = event_types.id
        left join pivoted_attributes
            on (
                filtered_events.case_id = pivoted_attributes.case_id
                and filtered_events.event_id = pivoted_attributes.event_id
            )
)

select * from event_log_table
order by event_timestamp asc
'''

duckdb.sql(sql_query).write_csv(export_csv_file_path)
