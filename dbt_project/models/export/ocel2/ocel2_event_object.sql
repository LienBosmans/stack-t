select
    event_to_object.event_id as ocel_event_id,
    event_to_object.object_id as ocel_object_id,
    qualifiers.description as ocel_qualifier
from
    {{ ref('event_to_object') }}
    inner join {{ ref('qualifiers') }}
        on event_to_object.qualifier_id = qualifiers.id
