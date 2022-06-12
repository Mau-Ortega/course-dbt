{{
   config(
       materialized = 'view'
   )

}}

with src_greenery_events as (
    select * from {{ source('src_greenery','events') }}
)

,renamed_recast as (
    select
    event_id,
    session_id,
    user_id,
    page_url,
    created_at,
    event_type,
    order_id,
    product_id

    from src_greenery_events
)

select * from renamed_recast