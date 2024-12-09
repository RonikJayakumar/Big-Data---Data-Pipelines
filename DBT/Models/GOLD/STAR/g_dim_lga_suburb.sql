{{
    config(
        unique_key='SUBURB_NAME',
        alias='dim_lga_suburb'
    )
}}

with

source  as (

    select * from {{ ref('s_lga_suburb') }}

),

cleaned as (
    select
        LGA_NAME,
        SUBURB_NAME
    from source
),

unknown as (
    select
        'unknown' as LGA_NAME,
        'unknown' as SUBURB_NAME
)
select * from unknown
union all
select * from cleaned