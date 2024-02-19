with object_to_object as (
    select * from {{ ref('object_to_object') }}
),
objects as (
    select * from {{ ref('objects') }}
),
qualifiers as (
    select * from {{ ref('qualifiers') }}
),
object_types as (
    select * from {{ ref('object_types') }}
),
edges_object_type_to_object_type as (
    select
        source_object_types.id as start_id,
        target_object_types.id as end_id,
        'OBJECT_TYPE_TO_OBJECT_TYPE' as type,
        qualifiers.description as relation,
        count(distinct object_to_object.id) as count_relationships,
        count(distinct source_objects.id) as count_distinct_source_objects,
        count(distinct target_objects.id) as count_distinct_target_objects
    from
        objects as source_objects
        inner join object_to_object
            on object_to_object.source_object_id = source_objects.id
        inner join objects as target_objects
            on object_to_object.target_object_id = target_objects.id
        inner join object_types as source_object_types
            on source_objects.object_type_id = source_object_types.id
        inner join object_types as target_object_types
            on target_objects.object_type_id = target_object_types.id
        inner join qualifiers
            on object_to_object.qualifier_id = qualifiers.id
    group by
        source_object_types.id,
        target_object_types.id,
        qualifiers.description
)

select * from edges_object_type_to_object_type
