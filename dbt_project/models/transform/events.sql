with place_order_events as (
    select
        md5(concat('e',id)) as id,
        md5('place_order') as event_type_id,
        ordered_at as timestamp,
        concat('order for ',order_total) as description
    from {{ ref('stg_orders') }}
),
open_store_events as (
    select
        md5(concat('e',id))  as id,
        md5('open_store') as event_type_id,
        opened_at as timestamp,
        concat('new store in ',name) as description
    from {{ ref('stg_stores') }}
),
send_tweet_events as (
    select
        md5(concat('e',tweets.id))  as id,
        md5('send_tweet') as event_type_id,
        tweets.tweeted_at as timestamp,
        concat('new tweet by ', customers.name) as description
    from 
        {{ ref('stg_tweets') }} as tweets
        left join {{ ref('stg_customers') }} as customers
            on tweets.user_id = customers.id
),
events as (
    select * from place_order_events
    UNION ALL select * from open_store_events
    UNION ALL select * from send_tweet_events
)

select * from events
