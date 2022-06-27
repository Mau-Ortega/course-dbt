{{
    config(
        materialized = 'table'
    )
}}


with pivot_and_aggregate_as_product_grain as (
    select
        p.product_id
        ,p.name
        ,count(p.product_id) as nb_times_viewed
        ,sum(case when e.event_type='add_to_cart' then 1
        else 0
        end) as nb_added_to_cart
    from {{ ref('stg_greenery__products')}} p
    left join {{ref('stg_greenery__events')}} e on e.product_id=p.product_id
    group by 1,2
)

select * from pivot_and_aggregate_as_product_grain