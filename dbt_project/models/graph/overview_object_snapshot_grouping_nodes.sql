with start_nodes as (
    select
        start_id as node_id
    from
        {{ ref('overview_directly_follow_edges')}}
),
end_nodes as (
    select
        end_id as node_id
    from
        {{ ref('overview_directly_follow_edges')}}
),
all_nodes as (
    select * from start_nodes
    UNION ALL
    select * from end_nodes
),
without_events as (
    select
        all_nodes.node_id
    from
        all_nodes
        ANTI join {{ ref('overview_event_type_nodes') }} as event_nodes 
            on all_nodes.node_id = event_nodes.event_type_id::varchar
    group by
        all_nodes.node_id
),
with_foreign_keys as (
    select
        all_nodes.node_id as object_snapshot_grouping_id,
        object_snapshots.object_type_id,
        object_snapshots.object_attribute_group_id,
        object_snapshots.prev_event_type_id,
        count(*) as total_node_count
    from
        without_events as all_nodes
        left join {{ ref('object_snapshots') }}
            on (
                all_nodes.node_id = object_snapshots.object_snapshot_grouping_id
                or all_nodes.node_id = md5(object_snapshots.object_snapshot_grouping_id || 'first')
                or all_nodes.node_id = md5(object_snapshots.object_snapshot_grouping_id || 'last')
            ) 
    group by
        all_nodes.node_id,
        object_snapshots.object_type_id,
        object_snapshots.object_attribute_group_id,
        object_snapshots.prev_event_type_id
),
with_details as (
    select 
        with_foreign_keys.*,
        event_types.description as prev_event_type,
        object_types.description as object_type,
        attribute_updates_by_object.object_attribute_group_description as object_attribute_group
    from
        with_foreign_keys
        left join {{ ref('event_types') }}
            on with_foreign_keys.prev_event_type_id = event_types.id
        left join {{ ref('object_types') }}
             on with_foreign_keys.object_type_id = object_types.id
        left join {{ ref('attribute_updates_by_object') }}
            on (
                with_foreign_keys.object_type_id = attribute_updates_by_object.object_type_id
                and with_foreign_keys.object_attribute_group_id = attribute_updates_by_object.object_attribute_group_id
            )
    group by *
)

select * from with_details
