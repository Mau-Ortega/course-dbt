{{
   config(
       materialized = 'view'
   )

}}

with src_greenery_promos as (
    select * from {{ source('src_greenery','promos') }}
)

,renamed_recast as (
    select
    promo_id,
    discount,
    status
          
    from src_greenery_promos
)

select * from renamed_recast