with overview as (
    select
        source_object_types.id as source_object_type_id,
        target_object_types.id as target_object_type_id,
        qualifiers.id as qualifier_id,
        qualifiers.description as relation_qualifier,
        source_object_types.description as source_object_type_description,
        target_object_types.description as target_object_type_description,
        count(distinct object_to_object.id) as object_to_object_count,
        count(distinct source_objects.id) as source_object_count,
        count(distinct target_objects.id) as target_object_count
    from
        {{ ref('object_to_object') }}
        left join {{ ref('qualifiers') }}
            on object_to_object.qualifier_id = qualifiers.id
        inner join {{ ref('objects') }} as source_objects
            on source_objects.id = object_to_object.source_object_id
        inner join {{ ref('object_types') }} as source_object_types
            on source_objects.object_type_id = source_object_types.id
        inner join {{ ref('objects') }} as target_objects
            on target_objects.id = object_to_object.target_object_id
        inner join {{ ref('object_types') }} as target_object_types
            on target_objects.object_type_id = target_object_types.id
    group by
        source_object_types.id,
        target_object_types.id,
        qualifiers.id,
        qualifiers.description, 
        source_object_types.description,
        target_object_types.description
    order by
        object_to_object_count desc
)

select
    source_object_count,
    source_object_type_description as source_object_type,
    -- object_to_object_count as relation_count,
    relation_qualifier,
    target_object_type_description as target_object_type,
    target_object_count
from overview