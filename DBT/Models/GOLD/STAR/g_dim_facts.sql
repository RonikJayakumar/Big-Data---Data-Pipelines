{{
    config(
        unique_key='LISTING_ID',
        alias='fact_listing_data'
    )
}}

with cleaned as (
    select
        LISTING_ID,
        SCRAPE_ID,
        SCRAPED_DATE,
        HOST_ID,
        PRICE,
        ACCOMMODATES,
        AVAILABILITY_30,
        NUMBER_OF_REVIEWS,
        REVIEW_SCORES_RATING,
        REVIEW_SCORES_ACCURACY,
        REVIEW_SCORES_CLEANLINESS,
        REVIEW_SCORES_CHECKIN,
        REVIEW_SCORES_COMMUNICATION,
        REVIEW_SCORES_VALUE
    from {{ ref('s_facts') }}
),

unknown as (
    select
        'unknown' as LISTING_ID,
        'unknown' as SCRAPE_ID,
        null::date as SCRAPED_DATE,
        0 as HOST_ID,
        0 as PRICE,
        0 as ACCOMMODATES,
        0 as AVAILABILITY_30,
        0 as NUMBER_OF_REVIEWS,
        0 as REVIEW_SCORES_RATING,
        0 as REVIEW_SCORES_ACCURACY,
        0 as REVIEW_SCORES_CLEANLINESS,
        0 as REVIEW_SCORES_CHECKIN,
        0 as REVIEW_SCORES_COMMUNICATION,
        0 as REVIEW_SCORES_VALUE
)

select * from unknown
union all
select * from cleaned
