{{
   config(
       materialized = 'view'
   )

}}

with src_greenery_order_items as (
    select * from {{ source('src_greenery','order_items') }}
)

,renamed_recast as (
    select
    order_id,
    product_id,
    quantity    

    from src_greenery_order_items
)

select * from renamed_recast