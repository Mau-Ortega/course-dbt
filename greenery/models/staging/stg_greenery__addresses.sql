{{
   config(
       materialized = 'view'
   )

}}

with src_greenery_addresses as (
    select * from {{ source('src_greenery','addresses') }}
)

,renamed_recast as (
    select
    address_id,
    address,
    zipcode,
    state,
    country      

    from src_greenery_addresses
)

select * from renamed_recast