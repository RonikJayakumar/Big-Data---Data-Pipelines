{{
    config(
        unique_key='LGA_CODE',
        alias='s_lga_code'
    )
}}

with source as (
    select * from {{ ref('b_lga_code') }}
),

renamed as (
    SELECT
        LGA_CODE,
        LGA_NAME
    from source
)

select * from renamed