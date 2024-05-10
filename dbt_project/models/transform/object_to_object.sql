with order_placed_in_store as ( -- store where order is placed
    select
        md5(concat(id,store_id)) as id,
        id as source_object_id, -- order
        store_id as target_object_id, -- store
        md5('order_placed_in_store') as qualifier_id,
        'order_placed_in_store' as qualifier_value,
        ordered_at as timestamp
    from
        {{ ref('stg_orders') }}
),
order_placed_by_customer as ( -- customer who placed the order
    select
        md5(concat(id,customer)) as id,
        id as source_object_id, -- order
        customer as target_object_id, -- customer
        md5('order_placed_by_customer') as qualifier_id,
        'order_placed_by_customer' as qualifier_value,
        ordered_at as timestamp
    from
        {{ ref('stg_orders') }}
),
order_contains_product as ( -- product on the order
    select
        md5(concat(stg_items.order_id,md5(stg_items.sku))) as id,
        stg_items.order_id as source_object_id, -- order
        md5(stg_items.sku) as target_object_id, -- product
        md5('order_contains_product') as qualifier_id,
        case
            when stg_products.type is null then 'other'
            else stg_products.type
        end as qualifier_value,
        stg_orders.ordered_at as timestamp
    from
        {{ ref('stg_items') }}
        inner join {{ ref('stg_orders') }}
            on stg_items.order_id = stg_orders.id
        left join {{ ref('stg_products') }}
            on stg_items.sku = stg_products.sku
    group by
        stg_items.order_id,
        stg_items.sku,
        stg_products.type,
        stg_orders.ordered_at
),
ingredient_used_for_product as ( -- ingredient needed to make product
    select
        md5(concat(md5(id),md5(sku))) as id,
        md5(id) as source_object_id, -- ingredient
        md5(sku) as target_object_id, -- product
        md5('ingredient_used_for_product') as qualifier_id,
        case
            when perishable is True then 'perishable'
            when perishable is False then 'non-perishable'
            else 'other'
        end as qualifier_value,
        make_date(1900,1,1)::datetime as timestamp -- dummy date
    from
        {{ ref('stg_supplies') }}
),
tweet_by_customer as ( -- customer who sent tweet
    select
        md5(concat(id,user_id)) as id,
        id as source_object_id, -- tweet
        user_id as target_object_id, -- customer
        md5('tweet_by_customer') as qualifier_id,
        'tweet_by_customer' as qualifier_value,
        tweeted_at as timestamp
    from
        {{ ref('stg_tweets') }}
),
orders_with_row_over_customer as (
    select
        *,
        row_number() OVER (PARTITION BY customer ORDER BY ordered_at asc) as customer_order_counter
    from
        {{ ref('stg_orders') }}
),
last_five_orders_by_same_customer as (
    select
        main_order.customer as customer,
        linked_orders.id as linked_order_id,
        main_order.ordered_at as timestamp
    from
        orders_with_row_over_customer as main_order
        left join orders_with_row_over_customer as linked_orders
            on (
                linked_orders.customer = main_order.customer
                and linked_orders.customer_order_counter BETWEEN main_order.customer_order_counter - 4 AND main_order.customer_order_counter
            )
),
customer_product_ranking as (
    select
        last_five_orders_by_same_customer.timestamp as timestamp,
        last_five_orders_by_same_customer.customer as customer,
        stg_items.sku as product_sku,
        count(stg_items.id) as product_count
    from
        last_five_orders_by_same_customer
        inner join {{ ref('stg_items') }}
            on last_five_orders_by_same_customer.linked_order_id = stg_items.order_id
    group by
        last_five_orders_by_same_customer.timestamp,
        last_five_orders_by_same_customer.customer,
        stg_items.sku
    order by
        product_count desc
),
customer_favorite_product as ( -- most ordered product in last 5 orders
    select distinct
        md5(concat(customer,md5(arg_max(product_sku,product_count)),timestamp)) as id,
        customer as source_object_id, -- customer
        md5(arg_max(product_sku,product_count)) as target_object_id, -- product
        md5('customer_favorite_product') as qualifier_id,
        'customer_favorite_product' as qualifier_value,
        timestamp as timestamp
    from
        customer_product_ranking
    group by
        customer,
        timestamp
),
customer_of_store as ( -- customer placed at least one order in store
    select
        md5(concat(store_id,customer)) as id,
        store_id as source_object_id, -- store
        customer as target_object_id, -- customer
        md5('customer_of_store') as qualifier_id,
        'customer_of_store' as qualifier_value,
        min(ordered_at) as timestamp
    from
        {{ ref('stg_orders') }}
    group by
        store_id,
        customer
),
object_to_object_relations as (
    select * from order_placed_in_store
    UNION ALL select * from order_placed_by_customer
    UNION ALL select * from order_contains_product
    UNION ALL select * from ingredient_used_for_product
    UNION ALL select * from tweet_by_customer
    UNION ALL select * from customer_favorite_product
    UNION ALL select * from customer_of_store 
)

select * from object_to_object_relations
