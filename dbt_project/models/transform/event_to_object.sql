with new_order as (
    select
        md5(concat('e',id,id)) as id,
        md5(concat('e',id)) as event_id,
        id as object_id,
        md5('new_order') as qualifier_id,
        'new_order' as qualifier_value
    from
        {{ ref('stg_orders') }}
),
order_placed_in as (
    select
        md5(concat('e',id,store_id)) as id,
        md5(concat('e',id)) as event_id,
        store_id as object_id,
        md5('order_placed_in') as qualifier_id,
        'order_placed_in' as qualifier_value
    from
        {{ ref('stg_orders') }}
),
order_placed_by as (
    select
        md5(concat('e',id,customer)) as id,
        md5(concat('e',id)) as event_id,
        customer as object_id,
        md5('order_placed_by') as qualifier_id,
        'order_placed_by' as qualifier_value
    from
        {{ ref('stg_orders') }}
),
new_store as (
    select
        md5(concat('e',id,id)) as id,
        md5(concat('e',id)) as event_id,
        id as object_id,
        md5('new_store') as qualifier_id,
        'new_store' as qualifier_value
    from
        {{ ref('stg_stores') }}
),
new_tweet as (
    select
        md5(concat('e',id,id)) as id,
        md5(concat('e',id)) as event_id,
        id as object_id,
        md5('new_tweet') as qualifier_id,
        'new_tweet' as qualifier_value
    from
        {{ ref('stg_tweets') }}
),
tweet_sent_by as (
    select
        md5(concat('e',id,user_id)) as id,
        md5(concat('e',id)) as event_id,
        user_id as object_id,
        md5('tweet_sent_by') as qualifier_id,
        'tweet_sent_by' as qualifier_value
    from
        {{ ref('stg_tweets') }}
),
event_to_object_relations as (
    select * from new_order
    UNION ALL select * from order_placed_in
    UNION ALL select * from order_placed_by
    UNION ALL select * from new_store
    UNION ALL select * from new_tweet
    UNION ALL select * from tweet_sent_by
)

select * from event_to_object_relations
