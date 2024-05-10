select
    id as ocel_type,
    lower(replace(description,' ','_')) as ocel_type_map
from
    {{ ref('object_types') }}
