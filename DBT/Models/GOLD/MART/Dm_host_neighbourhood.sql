{{
    config(
        alias='dm_host_neighbourhood'
    )
}}

-- Using a CTE to filter on HAS_AVAILABILITY first
WITH filtered_listings AS (
    SELECT
        LISTING_ID,
        LISTING_NEIGHBOURHOOD,
        EXTRACT(MONTH FROM SCRAPED_DATE) AS month,
        EXTRACT(YEAR FROM SCRAPED_DATE) AS year
    FROM {{ ref('g_dim_listing') }}
    WHERE HAS_AVAILABILITY = 't'
),

neighbourhood AS (
    SELECT
        fl.LISTING_NEIGHBOURHOOD AS HOST_NEIGHBOURHOOD_LGA,
        h.HOST_ID,
        CONCAT(fl.year, '-', LPAD(fl.month::TEXT, 2, '0')) AS month_year,
        SUM((30 - f.availability_30) * f.price) AS estimated_revenue
    FROM {{ ref('g_dim_host') }} h
    LEFT JOIN {{ ref('g_dim_facts') }} f ON f.HOST_ID = h.HOST_ID
    LEFT JOIN filtered_listings fl ON f.LISTING_ID = fl.LISTING_ID
    GROUP BY fl.LISTING_NEIGHBOURHOOD, h.HOST_ID, month_year
),

monthly_data AS (
    SELECT
        HOST_NEIGHBOURHOOD_LGA,
        month_year,
        COUNT(DISTINCT HOST_ID) AS new_hosts_current_month,
        SUM(estimated_revenue) AS estimated_revenue
    FROM neighbourhood
    GROUP BY HOST_NEIGHBOURHOOD_LGA, month_year
),

cumulative_hosts AS (
    SELECT
        HOST_NEIGHBOURHOOD_LGA,
        month_year,
        new_hosts_current_month,
        estimated_revenue,
        SUM(new_hosts_current_month) OVER (PARTITION BY HOST_NEIGHBOURHOOD_LGA 
                                           ORDER BY month_year
                                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_distinct_hosts
    FROM monthly_data
)

SELECT
    HOST_NEIGHBOURHOOD_LGA,
    month_year,
    cumulative_distinct_hosts AS distinct_hosts,
    estimated_revenue,
    ROUND(estimated_revenue / NULLIF(cumulative_distinct_hosts, 0), 2) AS estimated_revenue_per_host
FROM cumulative_hosts
ORDER BY HOST_NEIGHBOURHOOD_LGA, month_year
