with event_nodes as (
    select
        event_type_id,
        event_type,
        count(*) as event_count
    from
        {{ ref('traces_event_nodes') }}
    group by
        event_type_id,
        event_type
)

select * from event_nodes
