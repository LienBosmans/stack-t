with prev_next_snapshots as (
    select
        main_object_snapshots.object_id as object_id,
        main_object_snapshots.object_type_id as object_type_id,
        main_object_snapshots.object_snapshot_id as object_snapshot_id,
        main_object_snapshots.snapshot_timestamp as snapshot_timestamp,
        main_object_snapshots.snapshot_index_number as snapshot_index_number,
        main_object_snapshots.event_index_number as event_index_number,
        main_object_snapshots.event_id as event_id,
        main_object_snapshots.event_type_id as event_type_id,
        main_object_snapshots.object_attribute_group_id as object_attribute_group_id,
        prev_object_snapshots.object_snapshot_id as prev_object_snapshot_id,
        prev_object_snapshots.snapshot_timestamp as prev_snapshot_timestamp,
        next_object_snapshots.object_snapshot_id as next_object_snapshot_id,
        next_object_snapshots.snapshot_timestamp as next_snapshot_timestamp
    from
        {{ ref('object_snapshots') }} as main_object_snapshots
        left join {{ ref('object_snapshots') }} as prev_object_snapshots
            on (
                main_object_snapshots.object_id = prev_object_snapshots.object_id
                and main_object_snapshots.snapshot_index_number - 1 =  prev_object_snapshots.snapshot_index_number
            )
        left join {{ ref('object_snapshots') }} as next_object_snapshots
            on (
                main_object_snapshots.object_id = next_object_snapshots.object_id
                and main_object_snapshots.snapshot_index_number + 1 = next_object_snapshots.snapshot_index_number
            )  
)

select * from prev_next_snapshots
