{{
    config(
        unique_key='LISTING_ID',
        alias='listing_dim'
    )
}}

with source as (
    select 
        f.LISTING_ID,
        f.SCRAPE_ID,
        f.SCRAPED_DATE,
        f.LISTING_NEIGHBOURHOOD,
        p.PROPERTY_TYPE,
        p.ROOM_TYPE,
        p.HAS_AVAILABILITY
    from {{ ref('s_facts') }} f
    LEFT JOIN {{ref('property_dimension_snapshot')}} p ON f.LISTING_ID = p.LISTING_ID
),

cleaned as (
    select
        LISTING_ID,
        SCRAPE_ID,
        SCRAPED_DATE,
        LISTING_NEIGHBOURHOOD,
        PROPERTY_TYPE,
        ROOM_TYPE,
        HAS_AVAILABILITY
    from source
),

unknown as (
    select
        'unknown' as LISTING_ID,
        'unknown' as SCRAPE_ID,
        null::date as SCRAPED_DATE,
        'unknown' as LISTING_NEIGHBOURHOOD,
        'unknown' as PROPERTY_TYPE,
        'unknown' as ROOM_TYPE,
        'unknown' as HAS_AVAILABILITY
)

select * from unknown
union all
select * from cleaned
