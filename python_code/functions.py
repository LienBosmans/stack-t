import duckdb
import pandas as pd
import os
import shutil


def empty_folder(folder_path):
    """A function that deleted all files inside the given folder."""

    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        shutil.rmtree(folder_path)

    return None


def create_file(file_name,file_contents):
    """A function that uses the file_name (including path & file extension) 
    to create a new file (or overwrite an existing one) with the file_contents."""

    new_file = open(file_name, 'w')
    new_file.write(file_contents)
    new_file.close()

    return None



def get_tables(sqlite_db_path):
    """A function that returns an overview of all table names inside the SQLite database located at sqlite_db_path."""

    sql_query = get_query_sqlite_schema(sqlite_db_path)
    df_schema_info = duckdb.sql(sql_query).df()
    tables = list(df_schema_info['tbl_name'])

    return tables


def get_query_sqlite_schema(sqlite_db_path):
    """A function that returns a SQL query (as string) to get the database schema of a SQLite database."""

    sql_query = '''with schema_info as (
            select
                tbl_name,
                sql
            from sqlite_scan(\'''' + sqlite_db_path + '''\', 'sqlite_schema')
            where type = 'table'
        )

        select * from schema_info'''
    
    return sql_query



def get_event_tables(all_tables,mapping_tables):
    """A function that returns a list of all event tables in the 'all tables' list. 
    The list of mapping tables is used to filter out any non-event tables that start with 'event_'."""

    event_tables = get_type_tables('event',mapping_tables,all_tables)

    return event_tables


def get_object_tables(all_tables,mapping_tables):
    """A function that returns a list of all object tables in the 'all tables' list. 
    The list of mapping tables is used to filter out any non-object tables that start with 'object_'."""

    object_tables = get_type_tables('object',mapping_tables,all_tables)

    return object_tables


def get_type_tables(type,mapping_tables,all_tables):
    """A helper function used by get_event_tables and get_object_tables (to avoid code duplication)."""

    event_tables = []
    object_tables = []

    for table in all_tables:
        if not(table in mapping_tables):
            if table[:6] == 'event_':
                event_tables.append(table)
            elif table[:7] == 'object_':
                object_tables.append(table)

    if type == 'object':
        return object_tables
    elif type == 'event':
        return event_tables
    else:
        return []
  


def create_dbt_sources_yml(source_name,sqlite_db_path,tables,event_tables,object_tables,is_staging,models_folder='models'):
    """A function that creates a dbt sources file (as .yml file) inside the models folder."""
    if is_staging:
        staging_models_yml = generate_dbt_sources_yml(source_name,sqlite_db_path,tables,event_tables,object_tables,is_staging)
        create_file(models_folder + '/staging_models.yml',staging_models_yml)
    else:
        sources_yml = generate_dbt_sources_yml(source_name,sqlite_db_path,tables,event_tables,object_tables)
        create_file(models_folder + '/sources.yml',sources_yml)

    return None


def generate_dbt_sources_yml(source_name,sqlite_db_path,tables,event_tables,object_tables,is_staging=False):
    """A function that uses the database schema of a SQLite input file 
    to return the content of a dbt sources file (as string) that can be used in a dbt project."""
    if is_staging:
        sources_yml = \
'''version: 2

models:'''
    else:
        sources_yml = \
'''version: 2

sources:
  - name: ''' + source_name + '''
    tables:'''

    for table in tables:
        sources_yml = '\n'.join([sources_yml,generate_dbt_sources_entry(table,sqlite_db_path,event_tables,object_tables,is_staging)])

    return sources_yml


def generate_dbt_sources_entry(table_name,sqlite_db_path,event_tables,object_tables,is_staging=False):
    """A helper function that is used by get_sources_yml to generate the individual table entries."""
    if is_staging:
        name_line   = '      - name: stg_' + table_name
    else:
        name_line   = '      - name: ' + table_name

    description_line = get_dbt_source_description(table_name,event_tables,object_tables)

    if not(is_staging):
        location_line1  = '        meta:'
        location_line2   = '          external_location: "sqlite_scan(\'' + sqlite_db_path + '\', ' + table_name + ')"'

    if is_staging:
        columns_block = get_dbt_source_columns(table_name,event_tables,object_tables)
    
    if is_staging:
        dbt_source_entry = '\n'.join([name_line,description_line,columns_block])
    else:
        dbt_source_entry = '\n'.join([name_line,description_line,location_line1,location_line2])

    return dbt_source_entry


def get_dbt_source_description(table_name,event_tables,object_tables):
    """Helper function used to get description of table."""

    if table_name in event_tables:
        description = 'Event type table (OCEL2).'
    elif table_name in object_tables:
        description = 'Object type table (OCEL2).'
    elif table_name == 'event_map_type':
        description = 'Distinct event types (OCEL2).'
    elif table_name == 'object_map_type':
        description = 'Distinct object types (OCEL2).'
    elif table_name == 'event':
        description = 'Contains the event type for each event (OCEL2).'
    elif table_name == 'object':
        description = 'Contains the object type for each object (OCEL2).'
    elif table_name == 'event_object':
        description = 'Contains the event-to-object (E2O) relationships (OCEL2).'
    elif table_name == 'object_object':
        description = 'Contains the object-to-object (O2O) relationships (OCEL2).'
    else:
        description = 'unexpected table'

    description_line = '        description: ' + description

    return description_line


def get_dbt_source_columns(table_name,event_tables,object_tables):
    """Helper function used to get columns definition of source table."""

    if table_name in event_tables:
        columns_block = \
'''        columns:
          - name: ocel_id
            description: Primary key (PK) and foreign key (FK) of events (event table).
            tests:
              - unique
              - not_null
              - relationships:
                  to: ref('stg_event')
                  field: ocel_id
          - name: ocel_time
            tests:
              - not_null
'''
    elif table_name in object_tables:
        columns_block = \
'''        columns:
          - name: ocel_id
            description: Foreign key (FK) of objects (object table).
            tests:
              - not_null
              - relationships:
                  to: ref('stg_object')
                  field: ocel_id
          - name: ocel_time
            tests:
              - not_null
'''
    elif table_name == 'event_map_type':
        columns_block = \
'''        columns:
          - name: ocel_type
            description: Primary key (PK) of event types.
            tests:
              - unique
              - not_null
          - name: ocel_type_map
            description: Unique identifier used to link the event type to the corresponding 'event_' table.
            tests:
              - unique
              - not_null
'''
    elif table_name == 'object_map_type':
        columns_block = \
'''        columns:
          - name: ocel_type
            description: Primary key (PK) of object types.
            tests:
              - unique
              - not_null
          - name: ocel_type_map
            description: Unique identifier used to link the object type to the corresponding 'object_' table.
            tests:
              - unique
              - not_null
'''
    elif table_name == 'event':
        columns_block = \
'''        columns:
          - name: ocel_id
            description: Primary key (PK) of events.
            tests:
              - unique
              - not_null
          - name: ocel_type
            description: Foreign key (FK) of event types (event_map_type table).
            tests:
              - not_null
              - relationships:
                  to: ref('stg_event_map_type')
                  field: ocel_type
'''
    elif table_name == 'object':
        columns_block = \
'''        columns:
          - name: ocel_id
            description: Primary key (PK) of objects.
            tests:
              - unique
              - not_null
          - name: ocel_type
            description: Foreign key (FK) of object types (object_map_type table).
            tests:
              - not_null
              - relationships:
                  to: ref('stg_object_map_type')
                  field: ocel_type
'''
    elif table_name == 'event_object':
        columns_block = \
'''        columns:
          - name: ocel_event_id
            description: Foreign key (FK) of events (event table).
            tests:
              - not_null
              - relationships:
                  to: ref('stg_event')
                  field: ocel_id
          - name: ocel_object_id
            description: Foreign key (FK) of object (object table).
            tests:
              - not_null
              - relationships:
                  to: ref('stg_object')
                  field: ocel_id
          - name: ocel_qualifier
            description: Describes the relationship between event and object.
            tests:
              - not_null
'''
    elif table_name == 'object_object':
        columns_block = \
'''        columns:
          - name: ocel_source_id
            description: Foreign key (FK) of objects (object table).
            tests:
              - not_null
              - relationships:
                  to: ref('stg_object')
                  field: ocel_id
          - name: ocel_target_id
            description: Foreign key (FK) of object (object table).
            tests:
              - not_null
              - relationships:
                  to: ref('stg_object')
                  field: ocel_id
          - name: ocel_qualifier
            description: Describes the relationship between source object and target object.
            tests:
              - not_null
'''
    else:
        columns_block = ''


    return columns_block


def create_dbt_staging_model(source_name,source_table,staging_folder='models/staging'):
    """A function that creates a dbt staging model (as .sql file) inside the models/staging folder."""

    dbt_model = generate_dbt_staging_model(source_name,source_table)
    create_file(staging_folder + '/stg_' + source_table + '.sql',dbt_model)

    return None


def generate_dbt_staging_model(source_name,source_table):
    """A function that generates a dbt staging model (as string) for a given source table."""

    dbt_model = "select distinct * from {{ source('" + source_name + "','" + source_table + "') }}" + '\n'

    return dbt_model



def create_object_attributes_csv(sqlite_db_path,object_tables,transform_folder='models/transform'):
    """A function that generates the 'object_attribues' csv file (as string)."""
    csv_header = 'ocel_type_map,attribute,datatype'
    csv_body = []

    other_headers = ['ocel_id','ocel_time','ocel_changed_field']
    for table in object_tables:
        sql_query = "select * from sqlite_scan('" + sqlite_db_path + "', '" + table + "') where 1 != 1"
        df_table = duckdb.sql(sql_query).df()
        column_headers = df_table.columns.values
        attribute_headers = [header for header in column_headers if header not in other_headers]

        ocel_type_map = table[7:]    # removes 'object_' from table name
        for attribute in attribute_headers:
            datatype = str(df_table.dtypes[attribute])
            csv_body.append(','.join([ocel_type_map,attribute,datatype]))

    csv_contents =  csv_header + '\n' + '\n'.join(csv_body)

    create_file(transform_folder + '/object_attributes.csv',csv_contents)

    return None


def create_event_attributes_csv(sqlite_db_path,event_tables,transform_folder='models/transform'):
    """A function that generates the 'event_attribues' csv file (as string)."""

    csv_header = 'ocel_type_map,attribute,datatype'
    csv_body = []

    other_headers = ['ocel_id','ocel_time','ocel_changed_field']
    for table in event_tables:
        sql_query = "select * from sqlite_scan('" + sqlite_db_path + "', '" + table + "') where 1 != 1"
        df_table = duckdb.sql(sql_query).df()
        column_headers = df_table.columns.values
        attribute_headers = [header for header in column_headers if header not in other_headers]

        ocel_type_map = table[6:]    # removes 'event_' from table name
        for attribute in attribute_headers:
            datatype = str(df_table.dtypes[attribute])
            csv_body.append(','.join([ocel_type_map,attribute,datatype]))

    csv_contents = csv_header + '\n' + '\n'.join(csv_body)

    create_file(transform_folder + '/event_attributes.csv',csv_contents)

    return None



def create_dbt_transform_model(model_name,dbt_model,transform_folder='models/transform'):
    """A function that creates a dbt intermediate model (as .sql file) inside the models/transform folder."""

    create_file(transform_folder + '/' + model_name + '.sql',dbt_model)

    return None


def generate_events_model(event_tables,stg_event='stg_event',stg_event_map_type='stg_event_map_type'):
    """A function that generates the 'events' dbt model (as string)."""

    dbt_model = \
'''with all_event_tables as (
    '''

    sql_lines = []
    for table in event_tables:
        temp_sql = "select distinct ocel_id, ocel_time from {{ ref('stg_" + table + "') }}"
        sql_lines.append(temp_sql)

    dbt_model = dbt_model + '\n    UNION ALL '.join(sql_lines)

    temp_sql = \
'''
),
events as (
    select
        md5(all_event_tables.ocel_id::text) as id,
        md5(ocel2_event_map_type.ocel_type_map::text) as event_type_id,
        all_event_tables.ocel_id as description,
        all_event_tables.ocel_time as timestamp
    from
        all_event_tables
        inner join {{ ref(\'''' + stg_event + '''\') }} as ocel2_event
            on all_event_tables.ocel_id = ocel2_event.ocel_id
        inner join {{ ref(\'''' + stg_event_map_type + '''\') }} as ocel2_event_map_type
            on ocel2_event.ocel_type = ocel2_event_map_type.ocel_type
)

select * from events
'''

    dbt_model = dbt_model + temp_sql

    return dbt_model


def generate_objects_model(stg_object='stg_object',stg_object_map_type='stg_object_map_type'):
    """A function that generates the 'objects' dbt model (as string)."""

    dbt_model = \
'''with objects as (
    select
        md5(ocel2_object.ocel_id::text) as id,
        md5(ocel2_object_map_type.ocel_type_map::text) as object_type_id,
        ocel2_object.ocel_id as description
    from
        {{ ref(\'''' + stg_object + '''\') }} as ocel2_object
        inner join {{ ref(\'''' + stg_object_map_type + '''\') }} as ocel2_object_map_type
            on ocel2_object.ocel_type = ocel2_object_map_type.ocel_type
)

select * from objects
'''

    return dbt_model


def generate_event_types_model(stg_event_map_type='stg_event_map_type'):
    """A function that generates the 'event_types' dbt model (as string)."""

    dbt_model = \
'''select
    md5(ocel_type_map::text) as id,
    ocel_type as description
from {{ ref(\'''' + stg_event_map_type + '''\') }}
'''

    return dbt_model


def generate_object_types_model(stg_object_map_type='stg_object_map_type'):
    """A function that generates the 'object_types' dbt model (as string)."""

    dbt_model = \
'''select
    md5(ocel_type_map::text) as id,
    ocel_type as description
from {{ ref(\'''' + stg_object_map_type + '''\') }}
'''

    return dbt_model


def generate_event_to_object_model(stg_event_object='stg_event_object'):
    """A function that generates the 'event_to_object' dbt model (as string)."""

    dbt_model = \
'''select
    md5(ocel_event_id::text || '-' || ocel_object_id::text || '-' || ocel_qualifier::text) as id,
    md5(ocel_event_id::text) as event_id,
    md5(ocel_object_id::text) as object_id,
    md5(ocel_qualifier::text) as qualifier_id,
    ocel_qualifier as qualifier_value
from {{ ref(\'''' + stg_event_object + '''\') }}
'''

    return dbt_model


def generate_object_to_object_model(stg_object_object='stg_object_object'):
    """A function that generates the 'object_to_object' dbt model (as string)."""

    dbt_model = \
'''select
    md5(ocel_source_id::text || '-' || ocel_target_id::text || '-' || ocel_qualifier::text) as id,
    md5(ocel_source_id::text) as source_object_id,
    md5(ocel_target_id::text) as target_object_id,
    make_date(1900,1,1)::datetime as timestamp, -- not defined in ocel2
    md5(ocel_qualifier::text) as qualifier_id,
    ocel_qualifier as qualifier_value
from {{ ref(\'''' + stg_object_object + '''\') }}
'''

    return dbt_model


def generate_qualifiers_model(stg_event_object='stg_event_object',stg_object_object='stg_object_object'):
    """A function that generates the 'qualifiers' dbt model (as string)."""

    dbt_model = \
'''with all_qualifiers as (
    select distinct ocel_qualifier from {{ ref(\'''' + stg_event_object + '''\') }}
    UNION ALL
    select distinct ocel_qualifier from {{ ref(\'''' + stg_object_object + '''\') }}
),
unique_qualifiers as (
    select distinct ocel_qualifier from all_qualifiers
),
qualifiers as (
    select
        md5(ocel_qualifier::text) as id,
        ocel_qualifier as description,
        'varchar' as qualifier_datatype
    from unique_qualifiers
)

select * from qualifiers
'''

    return dbt_model


def generate_object_attributes_model(transform_folder='models/transform'):
    """A function that generates the 'object_attributes' dbt model (as string)."""

    dbt_model = \
'''select
    md5(ocel_type_map::text || '-' || attribute::text) as id,
    md5(ocel_type_map::text) as object_type_id,
    attribute as description,
    datatype as attribute_datatype
from read_csv(\'''' + transform_folder + '''/object_attributes.csv',delim=',',header=true,auto_detect=true)
'''

    df = pd.read_csv(transform_folder + '/object_attributes.csv')
    if df.empty:
        dbt_model = \
'''with empty_table as ( -- no object attributes in input data source
    select
        null as id,
        null as object_type_id,
        null as description,
        null as attribute_datatype
    where 1!=1
)

select * from empty_table
'''
    return dbt_model


def generate_event_attributes_model(transform_folder='models/transform'):
    """A function that generates the 'object_attributes' dbt model (as string)."""

    dbt_model = \
'''select
    md5(ocel_type_map::text || '-' || attribute::text) as id,
    md5(ocel_type_map::text) as event_type_id,
    attribute as description,
    datatype as attribute_datatype
from read_csv(\'''' + transform_folder + '''/event_attributes.csv',delim=',',header=true,auto_detect=true)
'''

    df = pd.read_csv(transform_folder + '/event_attributes.csv')
    if df.empty:
        dbt_model = \
'''with empty_table as ( -- no event attributes in input data source
    select
        null as id,
        null as event_type_id,
        null as description,
        null as attribute_datatype
    where 1!=1
)

select * from empty_table
'''
    return dbt_model


def generate_object_attribute_values_model(transform_folder='models/transform'):
    """A function that generates the 'object_attribute_values' dbt model (as string)."""

    dbt_model = 'with '

    df = pd.read_csv(transform_folder + '/object_attributes.csv')
    sql_blocks = []
    sql_lines = []

    for index, row in df.iterrows():
        sql_blocks.append(generate_object_attributes_values_entry(row['ocel_type_map'],row['attribute']))

        temp_sql = "select * from attribute_" + row['ocel_type_map'] + '_' + row['attribute']
        sql_lines.append(temp_sql)

    dbt_model = dbt_model + '\n'.join(sql_blocks)

    temp_sql = \
'''all_attributes as (
    '''

    temp_sql = temp_sql + '\n    UNION ALL '.join(sql_lines) + '\n)'
    dbt_model = dbt_model + '\n' + temp_sql

    temp_sql = \
'''
select * from all_attributes
'''

    dbt_model = dbt_model + '\n' + temp_sql

    if df.empty:
        dbt_model = '''with empty_table as ( -- no object attributes in input data source
        select
            null as id,
            null as object_id,
            null as object_attribute_id,
            null as attribute_value
        where 1!=1
    )

    select * from empty_table
    '''

    return dbt_model


def generate_object_attributes_values_entry(ocel_type_map,attribute,stg_object_prefix='stg_object_'):
    """A helper function that is used by generate_object_attribute_values_model to generate the individual object attributes."""
    
    prefix = 'attribute_' + ocel_type_map + '_' + attribute + ' as ('
    suffix = '),'

    concat = ocel_type_map + '-' + attribute
    body = \
'''
    select
        md5(\'''' + concat + '''\' || '-' || ocel_id::text || '-' || ocel_time::text) as id,
        md5(ocel_id::text) as object_id,
        ocel_time as timestamp,
        md5(\'''' + concat + '''\') as object_attribute_id,
        ''' + attribute + '''::varchar as attribute_value
    from {{ ref(\'''' + stg_object_prefix + ocel_type_map + '''\') }}
    where
        ocel_changed_field = \'''' + attribute + '''\'
        or ocel_changed_field is null
'''

    return ''.join([prefix,body,suffix]) 


def generate_event_attribute_values_model(transform_folder='models/transform'):
    """A function that generates the 'event_attribute_values' dbt model (as string)."""

    dbt_model = 'with '

    df = pd.read_csv(transform_folder + '/event_attributes.csv')
    sql_blocks = []
    sql_lines = []

    for index, row in df.iterrows():
        sql_blocks.append(generate_event_attributes_values_entry(row['ocel_type_map'],row['attribute']))

        temp_sql = "select * from attribute_" + row['ocel_type_map'] + '_' + row['attribute']
        sql_lines.append(temp_sql)

    dbt_model = dbt_model + '\n'.join(sql_blocks)

    temp_sql = \
'''all_attributes as (
    '''
    
    temp_sql = temp_sql + '\n    UNION ALL '.join(sql_lines) + '\n)'
    dbt_model = dbt_model + '\n' + temp_sql

    temp_sql = \
'''
select * from all_attributes
'''

    dbt_model = dbt_model + '\n' + temp_sql

    if df.empty:
        dbt_model = '''with empty_table as ( -- no event attributes in input data source
        select
            null as id,
            null as event_id,
            null as event_attribute_id,
            null as attribute_value
        where 1!=1
    )

    select * from empty_table
    '''

    return dbt_model


def generate_event_attributes_values_entry(ocel_type_map,attribute,stg_event_prefix='stg_event_'):
    """A helper function that is used by generate_event_attribute_values_model to generate the individual event attributes."""
    
    prefix = 'attribute_' + ocel_type_map + '_' + attribute + ' as ('
    suffix = '),'

    concat = ocel_type_map + '-' + attribute
    body = \
'''
    select
        md5(\'''' + concat + '''\' || '-' || ocel_id::text || '-' || ocel_time::text) as id,
        md5(ocel_id::text) as event_id,
        ocel_time as timestamp,
        md5(\'''' + concat + '''\') as event_attribute_id,
        ''' + attribute + '''::varchar as attribute_value
    from {{ ref(\'''' + stg_event_prefix + ocel_type_map + '''\') }}
'''

    return ''.join([prefix,body,suffix]) 
