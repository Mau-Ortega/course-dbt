{{
    config(
        materialized = 'table'
    )
}}

select

    ord.order_id,
    user_id,
    created_at as order_creation_date,
    order_cost,
    shipping_cost,
    order_total,
    shipping_service,
    estimated_delivery_at,
    delivered_at,
    delivered_at - created_at as days_to_delivery,
    estimated_delivery_at - created_at as estimated_days_to_delivery,
    ord.status,
    coalesce(sgp.promo_id,'No Promo'),
    discount,
    sgp.status as promo_status,
    num_products,
    total_items,
    selected_products
    

    

from {{ ref('stg_greenery__orders')}} ord
left join {{ref('int_orders_count')}} ioc on (ord.order_id = ioc.order_id)
left join {{ref('stg_greenery__promos')}} sgp on (ord.promo_id = sgp.promo_id)