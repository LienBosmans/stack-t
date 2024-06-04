import duckdb
import pandas as pd

duck_db = 'dev.duckdb'
export_folder = '../exports/docel/'

sql_query = '''
ATTACH ''' + "'" + duck_db + "'" + ''' as duck_db;
'''
duckdb.sql(sql_query)


## EVENTS TABLE

sql_query = '''
with events as (
    select
        events.id as EID,
        event_types.description as Activity,
        events.timestamp as Timestamp
    from
        duck_db.main_stackt.events
        inner join duck_db.main_stackt.event_types
            on events.event_type_id = event_types.id
),
event_to_object as (
    select
        events.id as EID,
        string_agg(objects.id,',') as object_ids,
        object_types.description as object_type_description
    from
        duck_db.main_stackt.events
        inner join duck_db.main_stackt.event_to_object
            on events.id = event_to_object.event_id
        inner join duck_db.main_stackt.objects
            on event_to_object.object_id = objects.id
        inner join duck_db.main_stackt.object_types
            on objects.object_type_id = object_types.id
    group by
        events.id,
        object_types.description
),
event_to_object_unpivoted as (
    select
        EID,
        case 
            when object_ids is null then null
            else concat('{',object_ids,'}')
        end as object_ids,
        object_type_description
    from
        event_to_object
),
event_to_object_pivoted as (
    PIVOT event_to_object_unpivoted
    ON object_type_description
    USING first(object_ids)
    GROUP BY EID
),
event_attributes_unpivoted as (
    select
        events.id as EID,
        event_attributes.description as event_attribute_description,
        event_attribute_values.attribute_value as attribute_value
    from
        duck_db.main_stackt.events
        inner join duck_db.main_stackt.event_attribute_values
            on events.id = event_attribute_values.event_id
        inner join duck_db.main_stackt.event_attributes
            on event_attribute_values.event_attribute_id = event_attributes.id
),
event_attributes_pivoted as (
    PIVOT event_attributes_unpivoted
    ON event_attribute_description
    USING first(attribute_value)
    GROUP BY EID
),
events_table as (
    select
        events.EID,
        events.Activity,
        events.Timestamp,
        event_to_object_pivoted.* EXCLUDE (EID),
        event_attributes_pivoted.* EXCLUDE (EID)
    from
        events
        left join event_to_object_pivoted
            on events.EID = event_to_object_pivoted.EID
        left join event_attributes_pivoted
            on events.EID = event_attributes_pivoted.EID
)

select * from events_table
order by Timestamp
'''
duckdb.sql(sql_query).write_csv(export_folder + 'events.csv')


## (DYNAMIC) OBJECT ATTRIBUTE TABLES

sql_query = '''
select distinct
    object_attributes.id,
    object_attributes.description
from
    duck_db.main_stackt.event_to_object_attribute_value
    inner join duck_db.main_stackt.object_attribute_values
        on event_to_object_attribute_value.object_attribute_value_id = object_attribute_values.id
    inner join duck_db.main_stackt.object_attributes
        on object_attribute_values.object_attribute_id = object_attributes.id
'''

object_types = duckdb.sql(sql_query).df()

for index,row in object_types.iterrows():
    object_attribute_id = row['id']
    object_attribute_description = row['description']

    sql_query = '''
    select
        event_to_object_attribute_value.id as RID,
        event_to_object_attribute_value.event_id as EID,
        object_attribute_values.object_id as OID,
        ''' + "'" +  object_attribute_description + "'" + ''' as attribute_name,
        object_attribute_values.attribute_value as attribute_value
    from
        duck_db.main_stackt.events
        inner join duck_db.main_stackt.event_to_object_attribute_value
            on events.id = event_to_object_attribute_value.event_id
        inner join duck_db.main_stackt.qualifiers
            on event_to_object_attribute_value.qualifier_id = qualifiers.id
        inner join duck_db.main_stackt.object_attribute_values
            on (
                event_to_object_attribute_value.object_attribute_value_id = object_attribute_values.id
                and events.timestamp = object_attribute_values.timestamp
                and object_attribute_values.object_attribute_id = ''' + "'" + object_attribute_id + "'" + '''
            )
    order by
        events.timestamp
    '''
    duckdb.sql(sql_query).write_csv(export_folder + 'object_attribute_' + object_attribute_description + '.csv')


## OBJECT TABLES

sql_query = '''
select
    object_types.id,
    object_types.description
from
    duck_db.main_stackt.object_types
'''

object_types = duckdb.sql(sql_query).df()

for index,row in object_types.iterrows():
    object_type_id = row['id']
    object_type_description = row['description']

    sql_query = '''
    with static_object_attributes as (
        select
            object_id,
            object_attribute_id,
            count(*) as value_update_count
        from
            duck_db.main_stackt.object_attribute_values
        group by
            object_id,
            object_attribute_id
    ),
    object_attributes_unpivoted as (
        select
            objects.id as object_id,
            objects.description as object_description,
            object_attributes.description as attribute_name,
            object_attribute_values.attribute_value as attribute_value
        from
            duck_db.main_stackt.objects
            left join static_object_attributes
                on objects.id = static_object_attributes.object_id
            left join duck_db.main_stackt.object_attributes
                on (
                    static_object_attributes.object_attribute_id = object_attributes.id
                    and static_object_attributes.value_update_count = 1
                ) 
            left join duck_db.main_stackt.object_attribute_values
                on (
                    object_attributes.id = object_attribute_values.object_attribute_id
                    and static_object_attributes.object_id = object_attribute_values.object_id
                )
        where 
            objects.object_type_id = ''' + "'" + object_type_id + "'" + '''
    ),
    object_attributes_pivoted as (
        pivot
            object_attributes_unpivoted
        on
            attribute_name
        using
            first(attribute_value)
        group by
            object_id,
            object_description
    )

    select * from object_attributes_pivoted
    '''

    duckdb.sql(sql_query).write_csv(export_folder + 'objects_' + object_type_description + '.csv')
