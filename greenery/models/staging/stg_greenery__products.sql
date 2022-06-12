{{
   config(
       materialized = 'view'
   )

}}

with src_greenery_products as (
    select * from {{ source('src_greenery','products') }}
)

,renamed_recast as (
    select
    product_id,
    name,
    price,
    inventory      

    from src_greenery_products
)

select * from renamed_recast