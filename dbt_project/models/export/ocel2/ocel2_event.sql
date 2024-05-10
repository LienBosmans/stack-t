select
    id as ocel_id,
    event_type_id as ocel_type
from
    {{ ref('events') }}
