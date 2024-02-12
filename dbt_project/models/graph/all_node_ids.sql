select object_snapshot_id as id from {{ ref('object_snapshots') }}
UNION ALL
select event_id as id from {{ ref('event_nodes') }}