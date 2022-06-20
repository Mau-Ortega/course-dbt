{{
    config(
        materialized = 'table'
    )
}}

with first_order as
(
    select 
        user_id,
        min(order_creation_date) as first_order_date,
        max(order_creation_date) as last_order_date

    from {{ ref('int_users_orders')}}
    group by 1
)
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
    account_creation_date,
    first_order_date,
    last_order_date

from {{ ref('int_users_orders')}} us
left join first_order fo on us.user_id = fo.user_id

   