select
    valid_from.id,
    valid_from.source_object_id,
    valid_from.target_object_id,
    valid_from.qualifier_id,
    valid_from.qualifier_value,
    valid_from.timestamp as valid_from_timestamp,
    valid_til.timestamp as valid_til_timestamp
from
    {{ ref('object_to_object') }} as valid_from
    ASOF left join {{ ref('object_to_object') }} as valid_til
        on (
            (
                valid_from.source_object_id = valid_til.source_object_id
                and valid_from.target_object_id = valid_til.target_object_id
                and valid_from.qualifier_id = valid_til.qualifier_id
            )
            and valid_from.timestamp < valid_til.timestamp
        )
where
    valid_from.qualifier_value is not null
