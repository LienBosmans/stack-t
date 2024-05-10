with first_object_to_object as (
    select
        source_object_id,
        target_object_id,
        qualifier_id
    from
        {{ ref('object_to_object') }}
    group by
        source_object_id,
        target_object_id,
        qualifier_id
)

select
    first_object_to_object.source_object_id as ocel_source_id,
    first_object_to_object.target_object_id as ocel_target_id,
    qualifiers.description as ocel_qualifier
from
    first_object_to_object
    inner join {{ ref('qualifiers') }}
        on first_object_to_object.qualifier_id = qualifiers.id
