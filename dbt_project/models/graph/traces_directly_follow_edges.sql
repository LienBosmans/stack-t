with event_outgoing_edges as ( -- events point to their corresponding object snapshot
    select
        object_snapshots.event_id as start_id,
        object_snapshots.event_type_id as start_grouping_id,
        object_snapshots.object_snapshot_id as end_id,
        object_snapshots.object_snapshot_grouping_id as end_grouping_id,
        qualifiers.description as relation,
        event_to_object.qualifier_value as relation_qualifier_value,
        object_snapshots.object_id as object_id,
        'event_to_object' as edge_type
    from
        {{ ref('object_snapshots') }}
        inner join {{ ref('event_to_object') }}
            on (
                object_snapshots.event_id = event_to_object.event_id
                and object_snapshots.object_id = event_to_object.object_id
            )
        inner join {{ ref('qualifiers') }}
            on event_to_object.qualifier_id = qualifiers.id
    where 
        object_snapshots.event_id is not null
),
object_snapshot_outgoing_edges as ( -- object snapshots point to the (corresponding event of the) next object snapshot
    select
        object_snapshots.object_snapshot_id as start_id,
        object_snapshots.object_snapshot_grouping_id as start_grouping_id,
        case
            when next_snapshots.event_id is null 
                then next_snapshots.object_snapshot_id
            else next_snapshots.event_id
        end as end_id,
        case
            when next_snapshots.event_id is null 
                then next_snapshots.object_snapshot_grouping_id
            else next_snapshots.event_type_id
        end as end_grouping_id,
        null as relation,
        null as relation_qualifier_value,
        object_snapshots.object_id as object_id,
        case
            when next_snapshots.event_id is null 
                then 'attribute_update'
            else 'next_event' 
        end as edge_type
    from
        {{ ref('object_snapshots') }} as object_snapshots
        inner join {{ ref('object_snapshots' )}} as next_snapshots
            on (
                object_snapshots.object_id = next_snapshots.object_id
                and object_snapshots.snapshot_index_number + 1 = next_snapshots.snapshot_index_number
            )
),
dummy_first_edges as ( -- edge from dummy 'first' node to the (corresponding event of the) first object snapshot
    select
        md5(object_snapshots.object_id || 'first') as start_id,
        md5(object_snapshots.object_snapshot_grouping_id || 'first') as start_grouping_id,
        case
            when object_snapshots.event_id is null 
                then object_snapshots.object_snapshot_id
            else object_snapshots.event_id
        end as end_id,
        case
            when object_snapshots.event_id is null 
                then object_snapshots.object_snapshot_grouping_id
            else object_snapshots.event_type_id
        end as end_grouping_id,
        'first' as relation,
        null as relation_qualifier_value,
        object_snapshots.object_id as object_id,
        'first_dummy' as edge_type
    from
        {{ ref('prev_next_object_snapshots') }}
        inner join {{ ref('object_snapshots') }}
            on prev_next_object_snapshots.object_snapshot_id = object_snapshots.object_snapshot_id
    where
        prev_object_snapshot_id is null
),
dummy_last_edges as ( -- edge from the last object snapshot to the dummy 'last' node
    select
        object_snapshots.object_snapshot_id as start_id,
        object_snapshots.object_snapshot_grouping_id as start_grouping_id,
        md5(object_snapshots.object_id || 'last') as end_id,
        md5(object_snapshots.object_snapshot_grouping_id || 'last') as end_grouping_id,
        'last' as relation,
        null as relation_qualifier_value,
        object_snapshots.object_id as object_id,
        'last_dummy' as edge_type
    from
        {{ ref('prev_next_object_snapshots') }}
        inner join {{ ref('object_snapshots') }}
            on prev_next_object_snapshots.object_snapshot_id = object_snapshots.object_snapshot_id
    where
        next_object_snapshot_id is null
),
all_directly_follow_edges as (
    select * from event_outgoing_edges
    UNION ALL
    select * from object_snapshot_outgoing_edges
    UNION ALL
    select * from dummy_first_edges
    UNION ALL
    select * from dummy_last_edges
),
edges_with_object_description as (
    select
        all_directly_follow_edges.*,
        objects.description as object_description
    from 
        all_directly_follow_edges
        inner join {{ ref('objects') }}
            on all_directly_follow_edges.object_id = objects.id
)

select * from edges_with_object_description 
