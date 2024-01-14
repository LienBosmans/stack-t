with overview as (
    select
        object_type_id,
        trace_id,
        object_type_description,
        trace_description,
        event_count,
        count(object_id) as trace_count,
        to_seconds(avg(trace_duration_in_seconds)::bigint)::interval as avg_trace_duration,
        min(trace_duration) as min_trace_duration,
        max(trace_duration) as max_trace_duration
    from {{ ref('traces') }}
    group by
        object_type_id,
        trace_id,
        object_type_description,
        trace_description,
        event_count
),
max_trace_count_object_type as (
    select
        object_type_id,
        max(trace_count) as max_trace_count
    from overview
    group by object_type_id
),
ordered_overview as (
    select overview.* 
    from 
        overview
        inner join max_trace_count_object_type
            on overview.object_type_id = max_trace_count_object_type.object_type_id
    order by
        max_trace_count_object_type.max_trace_count desc,
        trace_count desc,
        event_count desc
)

select
    object_type_description as object_type,
    trace_description as trace,
    trace_count,
    event_count,
    avg_trace_duration,
    min_trace_duration,
    max_trace_duration
from ordered_overview