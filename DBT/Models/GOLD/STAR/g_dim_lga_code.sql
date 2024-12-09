{{
    config(
        unique_key='LGA_CODE',
        alias='dim_lga_code'
    )
}}

with

source  as (

    select * from {{ ref('s_lga_code') }}

),

cleaned as (
    select
        LGA_CODE,
        LGA_NAME
    from source
),

unknown as (
    select
        0 as LGA_CODE,
        'unknown' as LGA_NAME
)
select * from unknown
union all
select * from cleaned