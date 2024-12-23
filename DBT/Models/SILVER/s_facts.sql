{{
    config(
        unique_key='LISTING_ID',
        alias='s_listings'
    )
}}

with source as (
    select 
        f.*,
        c.LGA_NAME,
        s.SUBURB_NAME
    from {{ ref('b_facts') }} f
    LEFT JOIN {{ref('b_lga_code')}} c ON f.LISTING_NEIGHBOURHOOD = c.LGA_NAME
    LEFT JOIN {{ref('b_lga_suburb')}} s ON f.HOST_NEIGHBOURHOOD = s.SUBURB_NAME
),

renamed as (
    select
        LISTING_ID,
        SCRAPE_ID,
        SCRAPED_DATE,
        HOST_ID,
        HOST_NAME,
        HOST_SINCE,
        CASE WHEN HOST_IS_SUPERHOST IN ('t', 'f') THEN HOST_IS_SUPERHOST ELSE NULL END AS HOST_IS_SUPERHOST,
        COALESCE(SUBURB_NAME, HOST_NEIGHBOURHOOD) AS HOST_NEIGHBOURHOOD, 
        COALESCE(LGA_NAME, LISTING_NEIGHBOURHOOD) AS LISTING_NEIGHBOURHOOD,
        PROPERTY_TYPE,
        ROOM_TYPE,
        ACCOMMODATES,
        PRICE,
        CASE WHEN HAS_AVAILABILITY IN ('t', 'f') THEN HAS_AVAILABILITY ELSE NULL END AS HAS_AVAILABILITY,
        CASE WHEN AVAILABILITY_30 > 30 THEN 30 ELSE AVAILABILITY_30 END AS AVAILABILITY_30,
        COALESCE(NUMBER_OF_REVIEWS, 0) AS NUMBER_OF_REVIEWS,
        COALESCE(REVIEW_SCORES_RATING, 0) AS REVIEW_SCORES_RATING,
        COALESCE(REVIEW_SCORES_ACCURACY, 0) AS REVIEW_SCORES_ACCURACY,
        COALESCE(REVIEW_SCORES_CLEANLINESS, 0) AS REVIEW_SCORES_CLEANLINESS,
        COALESCE(REVIEW_SCORES_CHECKIN, 0) AS REVIEW_SCORES_CHECKIN,
        COALESCE(REVIEW_SCORES_COMMUNICATION, 0) AS REVIEW_SCORES_COMMUNICATION,
        COALESCE(REVIEW_SCORES_VALUE, 0) AS REVIEW_SCORES_VALUE
    from source
)

select * from renamed