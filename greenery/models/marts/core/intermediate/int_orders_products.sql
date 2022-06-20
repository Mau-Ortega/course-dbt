{{
    config(
        materialized = 'table'
    )
}}


select

    order_id,
    ord.product_id,
    quantity,
    name,
    price,
    inventory


from {{ ref('stg_greenery__order_items')}} ord
left join {{ref('stg_greenery__products')}} prod on (ord.product_id = prod.product_id)

   