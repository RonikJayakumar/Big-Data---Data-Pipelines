{{
    config(
        unique_key='SUBURB_NAME',
        alias='s_lga_name'
    )
}}

with source as (
    select * from {{ ref('b_lga_suburb') }}
),

renamed as (
    SELECT
        LGA_NAME,
        SUBURB_NAME
    from source
)

select * from renamed