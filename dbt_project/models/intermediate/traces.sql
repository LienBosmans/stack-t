with traces as (
    select
        object_id,
        object_type_id,
        md5(string_agg(event_type_id || qualifier_id, '|')) as trace_id,
        object_description,
        object_type_description,
        string_agg(event_type_description, '|') as trace_description,
        min(event_timestamp) as first_event_timestamp,
        max(event_timestamp) as last_event_timestamp,
        count(event_id) as event_count,
        max(event_timestamp) - min(event_timestamp) as trace_duration,
        date_diff('second', min(event_timestamp), max(event_timestamp)) as trace_duration_in_seconds
    from
        (select * from {{ ref('events_by_object') }} order by event_timestamp asc)
    group by
        object_id,
        object_type_id,
        object_description,
        object_type_description,
),
first_object_type_ts as (
    select
        object_type_id,
        min(first_event_timestamp) as first_event_timestamp
    from traces
    group by object_type_id
),
first_object_ts as (
    select
        object_id,
        min(first_event_timestamp) as first_event_timestamp
    from traces
    group by object_id
)

select 
    traces.* 
from
    traces
    left join first_object_type_ts
        on traces.object_type_id = first_object_type_ts.object_type_id
    left join first_object_ts
        on traces.object_id = first_object_ts.object_id
order by
    first_object_type_ts.first_event_timestamp asc,
    first_object_ts.first_event_timestamp asc