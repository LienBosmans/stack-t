with object_to_object_edges as (
    select
        start_grouping_id as start_id,
        end_grouping_id as end_id,
        relation,
        relation_qualifier_value,
        count(distinct source_object_id) as distinct_source_object_count,
        count(distinct target_object_id) as distinct_target_object_count,
        count(*) as total_edge_count
    from
        {{ ref('traces_object_to_object_edges') }}
    group by
        start_grouping_id,
        end_grouping_id,
        relation,
        relation_qualifier_value
)

select * from object_to_object_edges
