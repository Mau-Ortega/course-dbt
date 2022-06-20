{{
    config(materialized = 'table' )
}}

select

    date(order_creation_date),
    country,
    state,
    promo,
    coalesce(discount,0) as discount,
    sum(order_total) as total_amount,
    sum(order_total)/count(distinct user_id) as avg_ticket

from {{ ref('int_users_orders')}}

group by 1,2,3,4,5
order by 1,2,3,4,5