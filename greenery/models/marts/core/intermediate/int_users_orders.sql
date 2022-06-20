{{
    config(
        materialized = 'table'
    )
}}

select

    us.user_id,
    first_name,
    last_name,
    email,
    phone_number,
    address,
    zipcode,
    state,
    country,
    order_id, 
    us.created_at as account_creation_date,
    ord.created_at as order_creation_date,
    order_cost,
    shipping_cost,
    order_total,
    shipping_service,
    estimated_delivery_at,
    delivered_at,
    ord.status,
    coalesce(prom.promo_id, 'No Promo') as promo,
    discount


from {{ ref('stg_greenery__users')}} us
left join {{ref('stg_greenery__addresses')}} ad on (us.address_id = ad.address_id)
left join {{ref('stg_greenery__orders')}} ord on (us.user_id = ord.user_id)
left join {{ref('stg_greenery__promos')}} prom on (ord.promo_id = prom.promo_id)
