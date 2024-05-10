select
    id as ocel_id,
    object_type_id as ocel_type
from
    {{ ref('objects') }}
