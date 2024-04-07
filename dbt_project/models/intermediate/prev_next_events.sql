with prev_next_events as (
    select
        main_events_by_object.object_id as object_id,
        main_events_by_object.object_type_id as object_type_id,
        main_events_by_object.index_number as event_index_number,
        main_events_by_object.event_id as event_id,
        main_events_by_object.event_type_id as event_type_id,
        main_events_by_object.event_timestamp as event_timestamp,
        prev_events_by_object.event_id as prev_event_id,
        prev_events_by_object.event_type_id as prev_event_type_id,
        prev_events_by_object.event_timestamp as prev_event_timestamp,
        next_events_by_object.event_id as next_event_id,
        next_events_by_object.event_type_id as next_event_type_id,
        next_events_by_object.event_timestamp as next_event_timestamp
    from
        {{ ref('events_by_object') }} as main_events_by_object
        left join {{ ref('events_by_object') }} as prev_events_by_object
            on (
                main_events_by_object.object_id = prev_events_by_object.object_id
                and main_events_by_object.index_number - 1 =  prev_events_by_object.index_number
            )
        left join {{ ref('events_by_object') }} as next_events_by_object
            on (
                main_events_by_object.object_id = next_events_by_object.object_id
                and main_events_by_object.index_number + 1 = next_events_by_object.index_number
            )  
)

select * from prev_next_events
