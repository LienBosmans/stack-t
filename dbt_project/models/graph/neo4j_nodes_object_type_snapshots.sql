{{ config(materialized='external',location='../neo4j/import/object_type_snapshots.csv') }}

select 
    'X' || object_type_snapshot_id as 'object_type_snapshot_id:ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    replace(object_type_description,' ','_') as 'object_type',
    'OBJECT_TYPE_SNAPSHOT' || ';' || replace(object_type_description,' ','_') as ':LABEL'
from {{ ref('object_type_snapshots') }}
