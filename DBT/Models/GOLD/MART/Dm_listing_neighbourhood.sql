{{
    config(
        alias='dm_listing_neighbourhood'
    )
}}

WITH listing_data AS (
    SELECT 
        l.LISTING_ID,
        l.LISTING_NEIGHBOURHOOD,
        l.SCRAPED_DATE,
        EXTRACT(MONTH FROM l.SCRAPED_DATE) as month,
        EXTRACT(YEAR FROM l.SCRAPED_DATE) as year,
        CONCAT(EXTRACT(YEAR FROM l.SCRAPED_DATE),'-', LPAD(EXTRACT(MONTH FROM l.SCRAPED_DATE)::TEXT, 2, '0')) AS month_year,
        (CASE WHEN l.HAS_AVAILABILITY = 't' THEN 1 ELSE 0 END) AS active_listing,
        (CASE WHEN h.HOST_IS_SUPERHOST = 't' THEN 1 ELSE 0 END) AS is_superhost,
        h.HOST_ID,
        f.REVIEW_SCORES_RATING,
        f.PRICE,
        f.AVAILABILITY_30 
    FROM {{ ref('g_dim_listing') }} l
    LEFT JOIN {{ ref('g_dim_facts') }} f ON l.LISTING_ID = f.LISTING_ID
    LEFT JOIN{{ ref('g_dim_host') }} h ON f.HOST_ID = h.HOST_ID
),
    
active_listing_counts AS (
    SELECT
        month,
        year,
        month_year,
        COUNT(*) FILTER (WHERE active_listing = 0) AS inactive_listing_count,
        COUNT(*) FILTER (WHERE active_listing = 1) AS active_listing_count
    FROM
        listing_data
    GROUP BY
        month, year, month_year
),

metrics AS (
    SELECT
        ld.LISTING_NEIGHBOURHOOD,
        ld.month_year,
        (ac.active_listing_count::FLOAT / COUNT(ld.*)) AS active_listing_rate,
        MIN(CASE WHEN ld.active_listing = 1 THEN ld.PRICE END) AS min_price_active,
        MAX(CASE WHEN ld.active_listing = 1 THEN ld.PRICE END) AS max_price_active,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ld.PRICE) FILTER (WHERE ld.active_listing = 1) AS median_price_active,
        AVG(CASE WHEN ld.active_listing = 1 THEN ld.PRICE END) AS avg_price_active,
        COUNT(DISTINCT ld.HOST_ID) AS total_hosts,
        (COUNT(DISTINCT CASE WHEN ld.is_superhost = 1 THEN ld.HOST_ID END) * 100.0 / NULLIF(COUNT(DISTINCT ld.HOST_ID), 0)) AS superhost_rate,
        AVG(CASE WHEN ld.active_listing = 1 THEN ld.REVIEW_SCORES_RATING END) AS avg_review_score_rating,
        (ac.active_listing_count - LAG(ac.active_listing_count) OVER (PARTITION BY ld.LISTING_NEIGHBOURHOOD ORDER BY ld.year, ld.month)) * 100.0 / NULLIF(LAG(ac.active_listing_count) OVER (PARTITION BY ld.LISTING_NEIGHBOURHOOD ORDER BY ld.year, ld.month), 0) AS percentage_change_active,
        (ac.inactive_listing_count - LAG(ac.inactive_listing_count) OVER (PARTITION BY ld.LISTING_NEIGHBOURHOOD ORDER BY ld.year, ld.month)) * 100.0 / NULLIF(LAG(ac.inactive_listing_count) OVER (PARTITION BY ld.LISTING_NEIGHBOURHOOD ORDER BY ld.year, ld.month), 0) AS percentage_change_inactive,
        (30 - AVG(CASE WHEN ld.active_listing = 1 THEN ld.AVAILABILITY_30 END)) AS total_stays,
        (AVG(CASE WHEN ld.active_listing = 1 THEN (30 - ld.AVAILABILITY_30) END) * AVG(ld.PRICE)) AS avg_revenue_per_active_listing
    FROM
        active_listing_counts ac
    JOIN listing_data ld ON ac.month_year = ld.month_year
    GROUP BY
        ld.LISTING_NEIGHBOURHOOD, ld.month_year, ac.active_listing_count, ac.inactive_listing_count, ld.year, ld.month
)

SELECT
    m.LISTING_NEIGHBOURHOOD,
    m.month_year,
    ROUND(m.active_listing_rate::numeric, 2) AS active_listing_rate,
    m.min_price_active,
    m.max_price_active,
    m.median_price_active,
    ROUND(m.avg_price_active::numeric, 2) AS avg_price_active,
    m.total_hosts,
    ROUND(m.superhost_rate::numeric, 2) AS superhost_rate,
    ROUND(m.avg_review_score_rating::numeric, 2) AS avg_review_score_rating,
    ROUND(m.percentage_change_active::numeric, 2) AS percentage_change_active,
    ROUND(m.percentage_change_inactive::numeric, 2) AS percentage_change_inactive,
    ROUND(m.total_stays::numeric, 2) as total_stays,
    ROUND(m.avg_revenue_per_active_listing::numeric, 2) AS avg_revenue_per_active_listing
FROM metrics m
ORDER BY m.LISTING_NEIGHBOURHOOD, m.month_year

