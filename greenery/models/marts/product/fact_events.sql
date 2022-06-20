{{
    config(
        materialized = 'table'
    )
}}

select
    date(created_at),
    country,
    state,
    count(distinct iseba.user_id) as users,
    count(distinct session_id) as n_sessions,
    count(distinct session_id)/count(distinct iseba.user_id) as avg_sessions_per_user,
    sum(n_page_view) as n_page_view,
    sum(n_page_view)/count(distinct iseba.user_id) as avg_page_view,
    sum(n_add_to_cart) as n_add_to_cart,
    sum(n_add_to_cart)/count(distinct iseba.user_id) as avg_add_to_cart,
    sum(n_checkout) as n_checkout,
    sum(n_checkout)/count(distinct iseba.user_id) as avg_checkout,
    sum(n_package_shipped) as n_package_shipped,
    sum(n_package_shipped)/count(distinct iseba.user_id) as avg_package_shipped

from {{ ref('int_session_events_basic_agg')}} iseba
left join {{ ref('dim_users')}} du on (iseba.user_id = du.user_id)
group by 1,2,3