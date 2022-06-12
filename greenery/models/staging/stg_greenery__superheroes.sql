{{
   config(
       materialized = 'view'
   )

}}

with src_greenery_superheroes as (
    select * from {{ source('src_greenery','superheroes') }}
)

,renamed_recast as (
    select
    id,
    name,
    gender,
    eye_color,
    race,
    hair_color,
    height,
    publisher,
    skin_color,
    alignment,
    weight,
    created_at,
    updated_at
          
    from src_greenery_superheroes
)

select * from renamed_recast