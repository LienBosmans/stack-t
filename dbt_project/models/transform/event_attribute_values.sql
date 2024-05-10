with calculated_order_subtotals as (
    select
        stg_orders.*,
        sum(stg_products.price) as calculated_subtotal
    from 
        {{ ref('stg_orders') }}
        left join {{ ref('stg_items') }}
            on stg_orders.id = stg_items.order_id
        left join {{ ref('stg_products') }}
            on stg_items.sku = stg_products.sku
    group by
        stg_orders.*
),
calculated_order_totals as (
    select
        orders.id as order_id,
        orders.order_total as order_total,
        orders.calculated_subtotal * (1 + stores.tax_rate) as calculated_order_total
    from 
        calculated_order_subtotals as orders
        left join {{ ref('stg_stores') }} as stores
            on orders.store_id = stores.id
),
total_is_correct as (
    select
        md5(concat('e',calculated_order_totals.order_id,'total_is_correct')) as id,
        md5(concat('e',calculated_order_totals.order_id)) as event_id,
        md5('total_is_correct') as event_attribute_id,
        case
            when order_total = calculated_order_total then 'True'
            else 'False'
        end as attribute_value
    from
        calculated_order_totals
),
event_attribute_values as (
    select * from total_is_correct
)

select * from event_attribute_values
