with order_subtotal as (
    select
        md5(concat(id,md5('order_subtotal'),ordered_at)) as id,
        id as object_id,
        md5('order_subtotal') as object_attribute_id,
        ordered_at as timestamp,
        subtotal::varchar as attribute_value
    from
        {{ ref('stg_orders') }}
),
order_tax as (
    select
        md5(concat(id,md5('order_tax'),ordered_at)) as id,
        id as object_id,
        md5('order_tax') as object_attribute_id,
        ordered_at as timestamp,
        tax_paid::varchar as attribute_value
    from
        {{ ref('stg_orders') }}   
),
order_total as (
    select
        md5(concat(id,md5('order_total'),ordered_at)) as id,
        id as object_id,
        md5('order_total') as object_attribute_id,
        ordered_at as timestamp,
        order_total::varchar as attribute_value
    from
        {{ ref('stg_orders') }}   
),
order_item_count as (
    select
        md5(concat(orders.id,md5('order_item_count'),orders.ordered_at)) as id,
        orders.id as object_id,
        md5('order_item_count') as object_attribute_id,
        orders.ordered_at as timestamp,
        count(stg_items.*)::varchar as attribute_value
    from
        {{ ref('stg_orders') }} as orders
        left join {{ ref('stg_items')}}
            on orders.id = stg_items.order_id
    group by
        orders.id,
        orders.ordered_at,
),
product_price as (
    select
        md5(concat(md5(sku),md5('product_price'),make_date(1900,1,1))) as id,
        md5(sku) as object_id,
        md5('product_price') as object_attribute_id,
        make_date(1900,1,1)::datetime as timestamp, -- dummy date
        price::varchar as attribute_value
    from
        {{ ref('stg_products') }}   
),
product_type as (
    select
        md5(concat(md5(sku),md5('product_type'),make_date(1900,1,1))) as id,
        md5(sku) as object_id,
        md5('product_type') as object_attribute_id,
        make_date(1900,1,1)::datetime as timestamp, -- dummy date
        price::varchar as attribute_value
    from
        {{ ref('stg_products') }}   
),
product_description as (
    select
        md5(concat(md5(sku),md5('product_description'),make_date(1900,1,1))) as id,
        md5(sku) as object_id,
        md5('product_description') as object_attribute_id,
        make_date(1900,1,1)::datetime as timestamp, -- dummy date
        description::varchar as attribute_value
    from
        {{ ref('stg_products') }}   
),
product_cost as (
    select
        md5(concat(md5(products.sku),md5('product_cost'),make_date(1900,1,1))) as id,
        md5(products.sku) as object_id,
        md5('product_cost') as object_attribute_id,
        make_date(1900,1,1)::datetime as timestamp, -- dummy date
        sum(ingredients.cost)::varchar as attribute_value
    from
        {{ ref('stg_products') }} as products
        left join {{ ref('stg_supplies') }} as ingredients 
            on products.sku = ingredients.sku
    group by
        products.sku
),
product_margin_perc as (
    select
        md5(concat(product_price.object_id,md5('product_margin_perc'),product_price.timestamp)) as id,
        product_price.object_id as object_id,
        md5('product_margin_perc') as object_attribute_id,
        product_price.timestamp as timestamp,
        ((product_price.attribute_value::decimal - product_cost.attribute_value::decimal) / (product_cost.attribute_value::decimal))::varchar as attribute_value
    from
        product_price
        inner join product_cost
            on (
                product_price.object_id = product_cost.object_id
                and product_price.timestamp = product_cost.timestamp
            )
),
tweet_content as (
    select
        md5(concat(id,md5('tweet_content'),tweeted_at)) as id,
        id as object_id,
        md5('tweet_content') as object_attribute_id,
        tweeted_at as timestamp,
        content::varchar as attribute_value
    from
        {{ ref('stg_tweets') }}
),
store_tax_rate as (
    select
        md5(concat(id,md5('store_tax_rate'),opened_at)) as id,
        id as object_id,
        md5('store_tax_rate') as object_attribute_id,
        opened_at as timestamp,
        tax_rate::varchar as attribute_value
    from
        {{ ref('stg_stores') }}
),
first_store_visits as (
    select
        customer,
        store_id,
        min(ordered_at) as first_visit_at,
    from 
        {{ ref('stg_orders' )}}
    group by
        customer,
        store_id
),
customer_count_with_duplicates as ( -- multiple customers can have first visit at same timestamp
    select
        md5(concat(store_id,md5('customer_count'),first_visit_at)) as id,
        store_id as object_id,
        md5('customer_count') as object_attribute_id,
        first_visit_at as timestamp,
        row_number() OVER (PARTITION BY store_id ORDER BY first_visit_at asc) as attribute_value
    from
        first_store_visits
),
customer_count as (
    select
        id as id,
        object_id as object_id,
        object_attribute_id as object_attribute_id,
        timestamp as timestamp,
        max(attribute_value) as attribute_value
    from
        customer_count_with_duplicates
    group by
        id,
        object_id,
        object_attribute_id,
        timestamp
),
ingredient_cost as (
    select
        md5(concat(md5(id),md5('ingredient_cost'),make_date(1900,1,1))) as id,
        md5(id) as object_id,
        md5('ingredient_cost') as object_attribute_id,
        make_date(1900,1,1)::datetime as timestamp, -- dummy date
        cost::varchar as attribute_value
    from
        {{ ref('stg_supplies') }}
    group by
        id,
        cost
),
ingredient_is_perishable as (
    select
        md5(concat(md5(id),md5('ingredient_is_perishable'),make_date(1900,1,1))) as id,
        md5(id) as object_id,
        md5('ingredient_is_perishable') as object_attribute_id,
        make_date(1900,1,1)::datetime as timestamp, -- dummy date
        perishable::varchar as attribute_value
    from
        {{ ref('stg_supplies') }}
    group by
        id,
        perishable
),
customer_tweet_count as (
    select
        md5(concat(user_id,md5('customer_tweet_count'),tweeted_at)) as id,
        user_id as object_id, -- customer
        md5('customer_tweet_count') as object_attribute_id,
        tweeted_at as timestamp,
        (row_number() OVER (PARTITION BY user_id ORDER BY tweeted_at asc))::varchar as attribute_value
    from
        {{ ref('stg_tweets') }}
),
object_attribute_values as (
    select * from order_subtotal
    UNION ALL select * from order_tax
    UNION ALL select * from order_total
    UNION ALL select * from order_item_count
    UNION ALL select * from product_price
    UNION ALL select * from product_type
    UNION ALL select * from product_description
    UNION ALL select * from product_cost
    UNION ALL select * from product_margin_perc
    UNION ALL select * from tweet_content
    UNION ALL select * from store_tax_rate
    UNION ALL select * from customer_count
    UNION ALL select * from ingredient_cost
    UNION ALL select * from ingredient_is_perishable
    UNION ALL select * from customer_tweet_count 
)

select * from object_attribute_values
