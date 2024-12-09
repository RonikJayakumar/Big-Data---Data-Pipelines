{{
    config(
        unique_key='HOST_ID',
        alias='host_dim'
    )
}}

with source as (
    select * from {{ ref('host_dimension_snapshot') }}
),

cleaned as (
    select
        HOST_ID,
        HOST_NAME,
        HOST_SINCE,
        HOST_IS_SUPERHOST,
        HOST_NEIGHBOURHOOD,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from end as valid_from,
        dbt_valid_to as valid_to
    from source
),

unknown as (
    select
        0 as HOST_ID,
        'unknown' as HOST_NAME,
        null::date as HOST_SINCE,
        'unknown' as HOST_IS_SUPERHOST,
        'unknown' as HOST_NEIGHBOURHOOD,
        '1900-01-01'::timestamp as valid_from,
        null::timestamp as valid_to
)

select * from unknown
union all
select * from cleaned
