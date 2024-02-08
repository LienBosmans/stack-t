{{ config(materialized='external',location='../neo4j/import/object_relations.csv',options={'force_quote':'relation'}) }}

with object_snapshots as (
    select
        object_snapshot_id,
        object_id,
        object_description,
        snapshot_timestamp
    from {{ ref('int_object_snapshots') }}
),
object_to_object as (
    select * from {{ ref('object_to_object') }}
),
qualifiers as (
    select * from {{ ref('qualifiers') }}
),
edges_object_to_object as (
    select
        source_objects.object_snapshot_id as start_id,
        target_objects.object_snapshot_id as end_id,
        'OBJECT_TO_OBJECT' as type,
        qualifiers.description as relation
    from
        object_snapshots as source_objects
        inner join object_to_object
            on (
                object_to_object.source_object_id = source_objects.object_id
            )
        inner join object_snapshots as target_objects
            on (
                object_to_object.target_object_id = target_objects.object_id
                and source_objects.snapshot_timestamp = target_objects.snapshot_timestamp
            )
        inner join qualifiers
            on object_to_object.qualifier_id = qualifiers.id
    where
        object_to_object.qualifier_value is not null           
)

select 
    'X' || start_id as ':START_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    relation as 'relation', -- force_quote
    'X' || end_id as ':END_ID', -- needs prefix letter, because neo4j does not accept id's that start with number
    type as ':TYPE'
from edges_object_to_object
