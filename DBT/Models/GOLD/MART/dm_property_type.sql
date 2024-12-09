{{
    config(
        alias='dm_property_type'
    )
}}

WITH base_data AS (
    SELECT  
        EXTRACT(MONTH FROM l.SCRAPED_DATE) AS month,
        EXTRACT(YEAR FROM l.SCRAPED_DATE) AS year,
        l.PROPERTY_TYPE,
        l.ROOM_TYPE,
        f.ACCOMMODATES,
        CASE WHEN l.HAS_AVAILABILITY = 't' THEN 1 ELSE 0 END AS is_active,
        h.HOST_ID,
        CASE WHEN h.HOST_IS_SUPERHOST = 't' THEN 1 ELSE 0 END AS is_superhost,
        f.PRICE,
        f.REVIEW_SCORES_RATING,
        f.NUMBER_OF_REVIEWS,
        (30 - f.availability_30) AS stays_count,
        (30 - f.availability_30) * f.PRICE AS revenue_estimate
    FROM {{ ref('g_dim_listing') }} l
    JOIN {{ ref('g_dim_facts') }} f ON l.LISTING_ID = f.LISTING_ID
    JOIN {{ ref('g_dim_host') }} h ON f.HOST_ID = h.HOST_ID
),

calculated_metrics AS (
    SELECT
        year,
        month,
        PROPERTY_TYPE,
        ROOM_TYPE,
        ACCOMMODATES,
        COUNT(*) AS listing_count,
        SUM(is_active) AS active_count,
        COUNT(DISTINCT HOST_ID) AS unique_hosts,
        COUNT(DISTINCT CASE WHEN is_superhost = 1 THEN HOST_ID END) AS superhost_count,
        MIN(CASE WHEN is_active = 1 THEN PRICE END) AS min_price,
        MAX(CASE WHEN is_active = 1 THEN PRICE END) AS max_price,
        AVG(CASE WHEN is_active = 1 THEN PRICE END) AS average_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN is_active = 1 THEN PRICE END) AS median_price,
        AVG(CASE WHEN is_active = 1 THEN REVIEW_SCORES_RATING END) AS average_review,
        SUM(stays_count) FILTER (WHERE is_active = 1) AS total_stays,
        SUM(revenue_estimate) FILTER (WHERE is_active = 1) AS total_revenue
    FROM base_data
    GROUP BY 
        year, month, PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES
)

SELECT 
    PROPERTY_TYPE,
    ROOM_TYPE,
    ACCOMMODATES,
    CONCAT(year, '-', LPAD(month::TEXT, 2, '0')) AS month_year,
    CASE 
        WHEN listing_count = 0 THEN NULL
        ELSE ROUND(active_count::numeric / listing_count * 100, 2)
    END AS active_rate,
    CASE 
        WHEN unique_hosts = 0 THEN NULL
        ELSE ROUND(superhost_count::numeric / unique_hosts * 100, 2)
    END AS superhost_rate,
    min_price,
    max_price,
    ROUND(average_price::numeric, 2) AS avg_price,
    median_price, 
    ROUND(average_review::numeric, 2) AS avg_review_score,
    total_stays,
    total_revenue,
    CASE 
        WHEN unique_hosts = 0 THEN NULL
        ELSE ROUND(total_revenue::numeric / unique_hosts, 2)
    END AS revenue_per_host,
    COALESCE(
        ROUND(100.0 * (active_count - LAG(active_count) OVER(PARTITION BY PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES ORDER BY year, month)) 
        / NULLIF(LAG(active_count) OVER(PARTITION BY PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES ORDER BY year, month), 0), 2), 0
    ) AS pct_change_active_listings,
    COALESCE(
        ROUND(100.0 * ((listing_count - active_count) - LAG(listing_count - active_count) OVER(PARTITION BY PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES ORDER BY year, month)) 
        / NULLIF(LAG(listing_count - active_count) OVER(PARTITION BY PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES ORDER BY year, month), 0), 2), 0
    ) AS pct_change_inactive_listings
FROM calculated_metrics
ORDER BY PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES, month_year
