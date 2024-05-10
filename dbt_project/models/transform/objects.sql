with orders as (
    select
        id as id,
        md5('order') as object_type_id,
        concat('order for ',order_total) as description
    from {{ ref('stg_orders') }}
),
products as (
    select
        md5(sku) as id,
        md5('product') as object_type_id,
        concat(sku,'|',name) as description
    from {{ ref('stg_products') }}
),
ingredients as (
    select distinct
        md5(id) as id,
        md5('ingredient') as object_type_id,
        concat(id,'|',name) as description
    from {{ ref('stg_supplies') }}
),
stores as (
    select
        id as id,
        md5('store') as object_type_id,
        name as description
    from {{ ref('stg_stores') }}
),
customers as (
    select
        id as id,
        md5('customer') as object_type_id,
        name as description
    from {{ ref('stg_customers') }}
),
tweets as (
    select
        id as id,
        md5('tweet') as object_type_id,
        concat('...',right(content,21)) as description -- complete tweet is (imo) too long to use as description
    from {{ ref('stg_tweets') }}
),
objects as (
    select * from orders
    UNION ALL select * from products
    UNION ALL select * from ingredients
    UNION ALL select * from stores
    UNION ALL select * from customers
    UNION ALL select * from tweets
)

select * from objects
