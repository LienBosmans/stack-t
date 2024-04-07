with events_with_attribute_updates as (
    select
        events_by_object.object_id,
        events_by_object.object_type_id,
        events_by_object.event_timestamp as snapshot_timestamp,
        max(events_by_object.index_number) as event_index_number,
        arg_max(events_by_object.event_id, events_by_object.index_number) as event_id,
        arg_max(events_by_object.event_type_id, events_by_object.index_number) as event_type_id,
        attribute_updates_by_object.object_attribute_group_id as object_attribute_group_id
    from
        {{ ref('events_by_object') }}
        left join {{ ref('attribute_updates_by_object') }}
            on (
                events_by_object.object_id = attribute_updates_by_object.object_id
                and events_by_object.event_timestamp = attribute_updates_by_object.update_timestamp
            )
    group by
        events_by_object.object_id,
        events_by_object.object_type_id,
        events_by_object.event_timestamp,
        attribute_updates_by_object.object_attribute_group_id
),
events_without_attribute_updates as (
    select
        events_by_object.object_id,
        events_by_object.object_type_id,
        events_by_object.event_timestamp as snapshot_timestamp,
        events_by_object.index_number as event_index_number,
        events_by_object.event_id,
        events_by_object.event_type_id,
        null as object_attribute_group_id
    from 
        {{ ref('events_by_object') }}
        ANTI JOIN events_with_attribute_updates
            on (
                events_by_object.object_id = events_with_attribute_updates.object_id
                and events_by_object.event_id = events_with_attribute_updates.event_id
            )
),
attribute_updates_without_event as (
    select
        attribute_updates_by_object.object_id,
        attribute_updates_by_object.object_type_id,
        attribute_updates_by_object.update_timestamp as snapshot_timestamp,
        null as event_index_number,
        null as event_id,
        null as event_type_id,
        attribute_updates_by_object.object_attribute_group_id
    from
        {{ ref('attribute_updates_by_object') }}
        ANTI JOIN {{ ref('events_by_object') }}
            on (
                attribute_updates_by_object.object_id = events_by_object.object_id
                and attribute_updates_by_object.update_timestamp = events_by_object.event_timestamp
            )
),
object_snapshots as (
    select * from events_with_attribute_updates
    UNION ALL
    select * from events_without_attribute_updates
    UNION ALL
    select * from attribute_updates_without_event
),
object_snapshots_with_index as (
    select
        md5(concat(object_id,snapshot_timestamp,event_id)) as object_snapshot_id,
        object_id,
        object_type_id,
        snapshot_timestamp,
        row_number() OVER (PARTITION BY object_id ORDER BY snapshot_timestamp ASC, event_index_number ASC) as snapshot_index_number,
        event_index_number,
        event_id,
        event_type_id,
        object_attribute_group_id
    from
        object_snapshots
),
object_snapshots_with_prev_event as (
    select
        object_snapshots.*,
        prev_events.event_id as prev_event_id,
        prev_events.event_type_id as prev_event_type_id,
        md5(concat(object_snapshots.object_type_id,prev_events.event_type_id,object_snapshots.object_attribute_group_id)) as object_snapshot_grouping_id
        -- Object snapshots will be grouped based on their object type, the event type of the previous event, and the set of updated attributes.
    from
        object_snapshots_with_index as object_snapshots
        ASOF left join (select * from object_snapshots_with_index where event_id is not null) as prev_events
            on (
                object_snapshots.object_id = prev_events.object_id
                and object_snapshots.snapshot_index_number >= prev_events.snapshot_index_number
            )
)

select * from object_snapshots_with_prev_event
