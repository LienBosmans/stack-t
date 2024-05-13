with first_store_visits as (
    select
        customer,
        store_id,
        md5(concat('e',argmin(id,ordered_at))) as event_id,
        min(ordered_at) as first_visit_at
    from 
        {{ ref('stg_orders' )}}
    group by
        customer,
        store_id
),
customer_count_by_store as ( -- link update of customer count to order events by first-time customers at that timestamp
    select
        md5(concat(first_store_visits.event_id,object_attribute_values.id)) as id,
        first_store_visits.event_id as event_id,
        object_attribute_values.id as object_attribute_value_id,
        md5('new_customer_in_store') as qualifier_id,
        'new_customer_in_store' as qualifier_value
    from
        {{ ref('object_attribute_values') }}
        inner join {{ ref('object_attributes') }}
            on (
                object_attribute_values.object_attribute_id = object_attributes.id
                and object_attributes.description = 'customer_count'
            ) 
        inner join first_store_visits
            on (
                first_store_visits.store_id = object_attribute_values.object_id
                and first_store_visits.first_visit_at = object_attribute_values.timestamp
            )   
)

select * from customer_count_by_store
