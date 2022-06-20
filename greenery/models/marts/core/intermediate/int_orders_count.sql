{{
    config(
        materialized = 'table'
    )
}}

select

    order_id,
    count(distinct product_id) as num_products,
    sum(quantity) as total_items,
    array_agg(name) as selected_products

from  {{ref('int_orders_products')}} 

group by 1

   