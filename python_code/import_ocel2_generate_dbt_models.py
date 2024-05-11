import os
from functions import *

## Change source_name and sqlite_db_name below!

source_name = 'ocel2_logistics'
sqlite_db_name = 'ocel2_logistics.sqlite'


## Create folders

sqlite_db_path = '../event_data/event_log_datasets/' + sqlite_db_name

models_folder = 'models'
staging_folder = models_folder + '/staging'
transform_folder = models_folder + '/transform'

empty_folder(staging_folder)
os.makedirs(staging_folder,exist_ok=True)

empty_folder(transform_folder)
os.makedirs(transform_folder,exist_ok=True)


## Get overview of tables inside sqlite_db

mapping_tables = ['object_object','object_map_type','event_map_type','event_object']
all_tables = get_tables(sqlite_db_path)
event_tables = get_event_tables(all_tables,mapping_tables)
object_tables = get_object_tables(all_tables,mapping_tables)


## Generate dbt sources.yml file & dbt staging_models.yml file

create_dbt_sources_yml(source_name,sqlite_db_path,all_tables,event_tables,object_tables,False)
create_dbt_sources_yml(source_name,sqlite_db_path,all_tables,event_tables,object_tables,True)


## Generate dbt staging models (.sql files)

for table in all_tables:
    create_dbt_staging_model(source_name,table)



## Generate attribute (.csv) files
    
create_object_attributes_csv(sqlite_db_path,object_tables)
create_event_attributes_csv(sqlite_db_path,event_tables)


# Generate dbt transform models (.sql files)

create_dbt_transform_model('events',generate_events_model(event_tables))
create_dbt_transform_model('objects',generate_objects_model())
create_dbt_transform_model('event_types',generate_event_types_model())
create_dbt_transform_model('object_types',generate_object_types_model())
create_dbt_transform_model('event_to_object',generate_event_to_object_model())
create_dbt_transform_model('object_to_object',generate_object_to_object_model())
create_dbt_transform_model('qualifiers',generate_qualifiers_model())
create_dbt_transform_model('object_attributes',generate_object_attributes_model())
create_dbt_transform_model('object_attribute_values',generate_object_attribute_values_model())
create_dbt_transform_model('event_attributes',generate_event_attributes_model())
create_dbt_transform_model('event_attribute_values',generate_event_attribute_values_model())
