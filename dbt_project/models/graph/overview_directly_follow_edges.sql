with directly_follow_edges as (
    select
        start_grouping_id as start_id,
        end_grouping_id as end_id,
        relation,
        relation_qualifier_value,
        object_types.description as object_type,
        edge_type,
        count(distinct object_id) as distinct_object_count,
        count(*) as total_edge_count
    from
        {{ ref('traces_directly_follow_edges') }}
        left join {{ ref('objects') }}
            on traces_directly_follow_edges.object_id = objects.id
        left join {{ ref('object_types') }}
            on objects.object_type_id = object_types.id
    group by
        start_grouping_id,
        end_grouping_id,
        relation,
        relation_qualifier_value,
        object_type,
        edge_type
)

select * from directly_follow_edges
