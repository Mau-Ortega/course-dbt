{{
    config(
        materialized = 'table'
    )
}}

with first_order as
(
    select 
        user_id as user_id_,
        country,
        state,
        zipcode,
        address,
        min(order_creation_date) as first_order_date,
        max(order_creation_date) as last_order_date

    from {{ ref('int_users_orders')}}
    group by 1,2,3,4,5
)
select

    {{ dbt_utils.star(from=ref('stg_greenery__users'), except=["address_id", "updated_at","created_at"]) }},
    country,
    state,
    zipcode,
    address,
    created_at as account_creation_date,
    first_order_date,
    last_order_date

from {{ ref('stg_greenery__users')}} us
left join first_order fo on us.user_id = fo.user_id_

   