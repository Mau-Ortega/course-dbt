{{
    config(
        materialized = 'table'
    )
}}

select
    session_id,
    created_at,
    user_id,
    sum(case when event_type = 'page_view' then 1 else 0 end) as n_page_view,
    sum(case when event_type = 'add_to_cart' then 1 else 0 end) n_add_to_cart,
    sum(case when event_type = 'checkout' then 1 else 0 end) n_checkout,
    sum(case when event_type = 'package_shipped' then 1 else 0 end) as n_package_shipped

from {{ ref('stg_greenery__events')}}
group by 1,2,3