with object_to_object_edges as (
    select
        source_object_snapshots.object_snapshot_id as start_id,
        source_object_snapshots.object_snapshot_grouping_id as start_grouping_id,
        target_object_snapshots.object_snapshot_id as end_id,
        target_object_snapshots.object_snapshot_grouping_id as end_grouping_id,
        qualifiers.description as relation,
        object_to_object.qualifier_value as relation_qualifier_value,
        source_object_snapshots.object_id as source_object_id,
        target_object_snapshots.object_id as target_object_id
    from
        {{ ref('object_snapshots') }} as source_object_snapshots
        ASOF inner join {{ ref('object_to_object') }}
            on (
                source_object_snapshots.object_id = object_to_object.source_object_id
                and source_object_snapshots.snapshot_timestamp >= object_to_object.timestamp
            )
        inner join {{ ref('object_snapshots') }} as target_object_snapshots
            on (
                target_object_snapshots.object_id = object_to_object.target_object_id
                and target_object_snapshots.snapshot_timestamp = source_object_snapshots.snapshot_timestamp
                -- object-to-object relations are only visualized for snapshots that share the same timestamp
                -- can be changed to bigger time window by changing above join condition
            )
        inner join {{ ref('qualifiers') }}
            on object_to_object.qualifier_id = qualifiers.id
)

select * from object_to_object_edges
