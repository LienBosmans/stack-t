import os
from functions import *

## Change source_name and duckdb_db_name below!

source_name = 'dbt-core'
duckdb_db_name = 'dbt-core.duckdb'


## List of tables

tables = ['event_types', 'events', 'event_attributes', 'event_attribute_values',
          'object_types', 'objects', 'object_attributes', 'object_attribute_values',
          'event_to_object', 'event_to_object_attribute_value', 'object_to_object', 'qualifiers',
          ]


## Create folders

dukdb_db_path = '../event_data/github_logs/' + duckdb_db_name

models_folder = 'models'
staging_folder = models_folder + '/staging'
transform_folder = models_folder + '/transform'

empty_folder(staging_folder)
os.makedirs(staging_folder,exist_ok=True)

empty_folder(transform_folder)
os.makedirs(transform_folder,exist_ok=True)


## Overwrite profiles.yml, to attach the DuckDB database with alias `github_log`

profiles_yml = """
stackt_user:
  outputs:
    dev:
      type: duckdb
      path: dev.duckdb
      attach:
        - path: """ + dukdb_db_path + """
          alias: github_log

  target: dev
"""

create_file("profiles.yml",profiles_yml)


## Generate dbt sources.yml file

sources_yml = """
version: 2

sources:
  - name: github_log
    description: GitHub log in Stack't format stored in the attached database with alias github_log (defined in profiles.yml)
    schema: main
    database: github_log
    tables: 
"""

for table in tables:
    if table == 'qualifiers':
        sources_yml = sources_yml + "      - name: relation_qualifiers\n"
    else:
        sources_yml = sources_yml + f"      - name: {table}\n"

create_file(models_folder + "/sources.yml",sources_yml)

## Delete dbt staging_models.yml file (if exists)

if os.path.exists(models_folder + "/staging_models.yml"):
    os.remove(models_folder + "/staging_models.yml")


# Generate dbt transform models (.sql files)

for table in tables:
    if table == 'qualifiers':
        sql_model = "select * from {{ source('github_log','relation_qualifiers') }}\n"
    else:
        sql_model = f"select * from {{{{ source('github_log','{table}')}}}}\n" # quadruple curlies to escape {} in f string
    
    create_file(transform_folder + f"/{table}.sql",sql_model)
