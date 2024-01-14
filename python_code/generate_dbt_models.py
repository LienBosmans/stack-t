import duckdb
import pandas as pd
import os


## Change source_name and sqlite_db_name below!

source_name = 'ocel2_source_name'
sqlite_db_name = 'ocel2_source_name.sqlite'


## Create folders

sqlite_db_path = '../event_log_datasets/' + sqlite_db_name

staging_folder = 'models/staging'
transform_folder = 'models/transform'

os.makedirs(staging_folder,exist_ok=True)
os.makedirs(transform_folder,exist_ok=True)


## Read sqlite database schema with duckdb, and save in pandas dataframe

sql_query = '''with schema_info as (
    select
        tbl_name,
        sql
    from sqlite_scan(\'''' + sqlite_db_path + '''\', 'sqlite_schema')
    where type = 'table'
)

select * from schema_info'''

df_schema_info = duckdb.sql(sql_query).df()


## Create sources.yml and staging models (.sql)

sources_yml = '''version: 2

sources:
  - name: ''' + source_name + '''
    tables:'''

def generate_sources_entry(tbl_name, sqlite_db_path):
    first_row   = '      - name: ' + tbl_name
    second_row  = '        meta:'
    third_row   = '          external_location: "sqlite_scan(\'' + sqlite_db_path + '\', ' + tbl_name + ')"'

    return '\n'.join([first_row,second_row,third_row])

event_tables = []
object_tables = []
mapping_tables = ['object_object','object_map_type','event_map_type','event_object']

for index, row in df_schema_info.iterrows():
    tbl_name = row['tbl_name']

    if not(tbl_name in mapping_tables):
        if tbl_name[:6] == 'event_':
            event_tables.append(tbl_name)
        elif tbl_name[:7] == 'object_':
            object_tables.append(tbl_name)

    sources_yml = '\n'.join([sources_yml,generate_sources_entry(tbl_name,sqlite_db_path)])

    dbt_model = "select * from {{ source('" + source_name + "','" + tbl_name + "') }}"
    file_model_sql = open(staging_folder + '/' + tbl_name + '.sql', 'w')
    file_model_sql.write(dbt_model)
    file_model_sql.close()

file_sources_yml = open('models/sources.yml', 'w')
file_sources_yml.write(sources_yml)
file_sources_yml.close()


## Create transform models

### events.sql
sql_statement = '''with all_event_tables as (
    '''
dbt_model = sql_statement

sql_rows = []
for table in event_tables:
    sql_statement = "select distinct ocel_id, ocel_time from {{ ref('" + table + "') }}"
    sql_rows.append(sql_statement)

sql_statement = '\n    UNION ALL '.join(sql_rows)

dbt_model = dbt_model + sql_statement

sql_statement = '''
),
events as (
    select
        md5(all_event_tables.ocel_id::text) as id,
        md5(ocel2_event_map_type.ocel_type_map::text) as event_type_id,
        all_event_tables.ocel_id as description,
        all_event_tables.ocel_time as timestamp
    from
        all_event_tables
        inner join {{ ref('event') }} as ocel2_event
            on all_event_tables.ocel_id = ocel2_event.ocel_id
        inner join {{ ref('event_map_type') }} as ocel2_event_map_type
            on ocel2_event.ocel_type = ocel2_event_map_type.ocel_type
)

select * from events
'''

dbt_model = dbt_model + sql_statement

file_model_sql = open(transform_folder + '/events.sql', 'w')
file_model_sql.write(dbt_model)
file_model_sql.close()

### objects.sql
sql_statement = '''with all_object_tables as (
    '''
dbt_model = sql_statement

sql_rows = []
for table in object_tables:
    sql_statement = "select distinct ocel_id from {{ ref('" + table + "') }}"
    sql_rows.append(sql_statement)

sql_statement = '\n    UNION ALL '.join(sql_rows)

dbt_model = dbt_model + sql_statement

sql_statement = '''
),
objects as (
    select
        md5(all_object_tables.ocel_id::text) as id,
        md5(ocel2_object_map_type.ocel_type_map::text) as object_type_id,
        all_object_tables.ocel_id as description
    from
        all_object_tables
        inner join {{ ref('object') }} as ocel2_object
            on all_object_tables.ocel_id = ocel2_object.ocel_id
        inner join {{ ref('object_map_type') }} as ocel2_object_map_type
            on ocel2_object.ocel_type = ocel2_object_map_type.ocel_type
)

select * from objects
'''

dbt_model = dbt_model + sql_statement

file_model_sql = open(transform_folder + '/objects.sql', 'w')
file_model_sql.write(dbt_model)
file_model_sql.close()


### event_types.sql
sql_statement = '''select
    md5(ocel_type_map::text) as id,
    ocel_type as description
from {{ ref('event_map_type') }}
'''

dbt_model = sql_statement
file_model_sql = open(transform_folder + '/event_types.sql', 'w')
file_model_sql.write(dbt_model)
file_model_sql.close()


### object_types.sql
sql_statement = '''select
    md5(ocel_type_map::text) as id,
    ocel_type as description
from {{ ref('object_map_type') }}
'''

dbt_model = sql_statement
file_model_sql = open(transform_folder + '/object_types.sql', 'w')
file_model_sql.write(dbt_model)
file_model_sql.close()


### event_to_object.sql
sql_statement = '''select
    md5(ocel_event_id::text || '-' || ocel_object_id::text || '-' || ocel_qualifier::text) as id,
    md5(ocel_event_id::text) as event_id,
    md5(ocel_object_id::text) as object_id,
    md5(ocel_qualifier::text) as qualifier_id,
    ocel_qualifier as qualifier_value
from {{ ref('event_object') }}
'''

dbt_model = sql_statement
file_model_sql = open(transform_folder + '/event_to_object.sql', 'w')
file_model_sql.write(dbt_model)
file_model_sql.close()


### object_to_object.sql
sql_statement = '''select
    md5(ocel_source_id::text || '-' || ocel_target_id::text || '-' || ocel_qualifier::text) as id,
    md5(ocel_source_id::text) as source_object_id,
    md5(ocel_target_id::text) as target_object_id,
    null::datetime as timestamp, -- not defined in ocel2
    md5(ocel_qualifier::text) as qualifier_id,
    ocel_qualifier as qualifier_value
from {{ ref('object_object') }}
'''

dbt_model = sql_statement
file_model_sql = open(transform_folder + '/object_to_object.sql', 'w')
file_model_sql.write(dbt_model)
file_model_sql.close()


### qualifiers.sql
sql_statement = '''with all_qualifiers as (
    select distinct ocel_qualifier from {{ ref('event_object') }}
    UNION ALL
    select distinct ocel_qualifier from {{ ref('object_object') }}
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

dbt_model = sql_statement
file_model_sql = open(transform_folder + '/qualifiers.sql', 'w')
file_model_sql.write(dbt_model)
file_model_sql.close()


### object_attributes.csv
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

file_csv = open(transform_folder + '/object_attributes.csv', 'w')
file_csv.write(csv_header + '\n' + '\n'.join(csv_body))
file_csv.close()


### object_attributes.sql
sql_statement = '''select
    md5(ocel_type_map::text || '-' || attribute::text) as id,
    md5(ocel_type_map::text) as object_type_id,
    attribute as description,
    datatype as attribute_datatype
from read_csv('models/transform/object_attributes.csv',delim=',',header=true,auto_detect=true)
'''

dbt_model = sql_statement

df = pd.read_csv(transform_folder + '/object_attributes.csv')
if df.empty:
    dbt_model = '''with empty_table as ( -- no object attributes in dataset
    select
        null as id,
        null as object_type_id,
        null as description,
        null as attribute_datetype
    where 1!=1
)

select * from empty_table
'''

file_model_sql = open(transform_folder + '/object_attributes.sql', 'w')
file_model_sql.write(dbt_model)
file_model_sql.close()


### object_attribute_values.sql
def generate_sql_block(ocel_type_map,attribute):
    prefix = 'attribute_' + ocel_type_map + '_' + attribute + ' as ('
    suffix = '),'

    concat = ocel_type_map + '-' + attribute
    body = '''
    select
        md5(\'''' + concat + '''\' || '-' || ocel_id::text || '-' || ocel_time::text) as id,
        md5(ocel_id::text) as object_id,
        ocel_time as timestamp,
        md5(\'''' + concat + '''\') as object_attribute_id,
        \'''' + attribute + '''\'::varchar as attribute_value
    from {{ ref('object_''' + ocel_type_map + '''\') }}
    where
        ocel_changed_field = \'''' + attribute + '''\'
'''
    return ''.join([prefix,body,suffix])

sql_statement = "with "
dbt_model = sql_statement

df = pd.read_csv(transform_folder + '/object_attributes.csv')
sql_blocks = []
sql_rows = []
attribute
for index, row in df.iterrows():
    sql_blocks.append(generate_sql_block(row['ocel_type_map'],row['attribute']))

    sql_statement = "select * from attribute_" + row['ocel_type_map'] + '_' + row['attribute']
    sql_rows.append(sql_statement)

dbt_model = dbt_model + '\n'.join(sql_blocks)

sql_statement = '''all_attributes as (
    '''
sql_statement = sql_statement + '\n    UNION ALL '.join(sql_rows) + '\n)'
dbt_model = dbt_model + '\n' + sql_statement

sql_statement = '''
select * from all_attributes
'''
dbt_model = dbt_model + '\n' + sql_statement

if df.empty:
    dbt_model = '''with empty_table as ( -- no object attributes in dataset
    select
        null as id,
        null as object_id,
        null as object_attribute_id,
        null as attribute_value
     where 1!=1
)

select * from empty_table
'''

file_model_sql = open(transform_folder + '/object_attribute_values.sql', 'w')
file_model_sql.write(dbt_model)
file_model_sql.close()


### event_attributes.csv
csv_header = 'ocel_type_map,attribute,datatype'
csv_body = []

other_headers = ['ocel_id','ocel_time','ocel_changed_field']
for table in event_tables:
    sql_query = "select * from sqlite_scan('" + sqlite_db_path + "', '" + table + "') where 1 != 1"
    df_table = duckdb.sql(sql_query).df()
    column_headers = df_table.columns.values
    attribute_headers = [header for header in column_headers if header not in other_headers]

    ocel_type_map = table[7:]    # removes 'event_' from table name
    for attribute in attribute_headers:
        datatype = str(df_table.dtypes[attribute])
        csv_body.append(','.join([ocel_type_map,attribute,datatype]))

file_csv = open(transform_folder + '/event_attributes.csv', 'w')
file_csv.write(csv_header + '\n' + '\n'.join(csv_body))
file_csv.close()


### event_attributes.sql
sql_statement = '''select
    md5(ocel_type_map::text || '-' || attribute::text) as id,
    md5(ocel_type_map::text) as event_type_id,
    attribute as description,
    datatype as attribute_datatype
from read_csv('models/transform/event_attributes.csv',delim=',',header=true,auto_detect=true)
'''

dbt_model = sql_statement

df = pd.read_csv(transform_folder + '/event_attributes.csv')
if df.empty:
    dbt_model = '''with empty_table as ( -- no event attributes in dataset
    select
        null as id,
        null as event_type_id,
        null as description,
        null as attribute_datetype
    where 1!=1
)

select * from empty_table
'''

file_model_sql = open(transform_folder + '/event_attributes.sql', 'w')
file_model_sql.write(dbt_model)
file_model_sql.close()


### event_attribute_values.sql
def generate_sql_block(ocel_type_map,attribute):
    prefix = 'attribute_' + ocel_type_map + '_' + attribute + ' as ('
    suffix = '),'

    concat = ocel_type_map + '-' + attribute
    body = '''
    select
        md5(\'''' + concat + '''\' || '-' || ocel_id::text || '-' || ocel_time::text) as id,
        md5(ocel_id::text) as event_id,
        ocel_time as timestamp,
        md5(\'''' + concat + '''\') as event_attribute_id,
        \'''' + attribute + '''\'::varchar as attribute_value
    from {{ ref('event_''' + ocel_type_map + '''\') }}
    where
        ocel_changed_field = \'''' + attribute + '''\'
'''
    return ''.join([prefix,body,suffix])

sql_statement = "with "
dbt_model = sql_statement

df = pd.read_csv(transform_folder + '/event_attributes.csv')
sql_blocks = []
sql_rows = []
attribute
for index, row in df.iterrows():
    sql_blocks.append(generate_sql_block(row['ocel_type_map'],row['attribute']))

    sql_statement = "select * from attribute_" + row['ocel_type_map'] + '_' + row['attribute']
    sql_rows.append(sql_statement)

dbt_model = dbt_model + '\n'.join(sql_blocks)

sql_statement = '''all_attributes as (
    '''
sql_statement = sql_statement + '\n    UNION ALL '.join(sql_rows) + '\n)'
dbt_model = dbt_model + '\n' + sql_statement

sql_statement = '''
select * from all_attributes
'''
dbt_model = dbt_model + '\n' + sql_statement

if df.empty:
    dbt_model = '''with empty_table as ( -- no event attributes in dataset
    select
        null as id,
        null as event_id,
        null as event_attribute_id,
        null as attribute_value
     where 1!=1
)

select * from empty_table
'''

file_model_sql = open(transform_folder + '/event_attribute_values.sql', 'w')
file_model_sql.write(dbt_model)
file_model_sql.close()
