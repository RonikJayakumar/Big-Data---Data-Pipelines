-- Question 1:

WITH revenue AS (
    SELECT 
        AVG(l.avg_revenue_per_active_listing) AS avg_revenue_per_active_listing, -- Average revenue across months
        ROW_NUMBER() OVER (ORDER BY AVG(l.avg_revenue_per_active_listing) DESC) AS highest_rank,
        ROW_NUMBER() OVER (ORDER BY AVG(l.avg_revenue_per_active_listing) ASC) AS lowest_rank,
        l.listing_neighbourhood,
        g.lga_code
    FROM gold.dm_listing_neighbourhood l 
    JOIN gold.dim_lga_code g ON l.listing_neighbourhood = g.LGA_NAME 
    GROUP BY l.listing_neighbourhood, g.lga_code
),

demographics AS (
    SELECT 
        c.LGA_CODE,
        c.tot_p_m,
        c.tot_p_f,
        c.age_0_4_yr_p,
        c.age_5_14_yr_p,
        c.Age_15_19_yr_P,
        c.Age_20_24_yr_P,
        c.Age_25_34_yr_P,
        c.Age_35_44_yr_P,
        c.Age_45_54_yr_P,
        c.Age_55_64_yr_P,
        c.Age_65_74_yr_P,
        c.Age_75_84_yr_P,
        c.Age_85ov_P,
        g.Median_tot_hhd_inc_weekly,
        g.Average_household_size
    FROM gold.dim_census_g01 c
    JOIN gold.dim_census_g02 g ON c.LGA_CODE = g.LGA_CODE
)

SELECT 
    r.listing_neighbourhood,
    ROUND(r.avg_revenue_per_active_listing, 2) as avg_revenue_per_active_listing,
    d.tot_p_m,
    d.tot_p_f,
    d.age_0_4_yr_p,
    d.age_5_14_yr_p,
    d.Age_15_19_yr_P,
    d.Age_20_24_yr_P,
    d.Age_25_34_yr_P,
    d.Age_35_44_yr_P,
    d.Age_45_54_yr_P,
    d.Age_55_64_yr_P,
    d.Age_65_74_yr_P,
    d.Age_75_84_yr_P,
    d.Age_85ov_P,
    d.Median_tot_hhd_inc_weekly,
    d.Average_household_size
FROM revenue r 
JOIN demographics d ON r.lga_code = d.LGA_CODE
WHERE (r.highest_rank <= 4 OR r.lowest_rank <= 3)
AND r.listing_neighbourhood != 'unknown'
ORDER BY r.highest_rank, r.lowest_rank;




-- Question 2:
WITH revenue AS (
    SELECT 
        l.listing_neighbourhood,
        g.lga_code,
        AVG(l.avg_revenue_per_active_listing) AS avg_revenue_per_active_listing
    FROM gold.dm_listing_neighbourhood l 
    JOIN gold.dim_lga_code g ON l.listing_neighbourhood = g.LGA_NAME 
    GROUP BY l.listing_neighbourhood, g.lga_code
),

demographics AS (
    SELECT 
        c.LGA_CODE,
        c.Median_age_persons
    FROM gold.dim_census_g02 c
)

SELECT 
    r.listing_neighbourhood,
    ROUND(r.avg_revenue_per_active_listing, 2) AS avg_revenue_per_active_listing,
    d.Median_age_persons
FROM revenue r
JOIN demographics d ON r.lga_code = d.LGA_CODE
WHERE r.listing_neighbourhood IS NOT null
order by r.avg_revenue_per_active_listing ASC;




-- Question 3:
WITH neighbourhood_revenue AS (
    SELECT 
        listing_neighbourhood,
        AVG(avg_revenue_per_active_listing) AS avg_revenue,
        SUM(total_stays) AS total_stays
    FROM gold.dm_listing_neighbourhood
    WHERE listing_neighbourhood IS NOT NULL AND listing_neighbourhood != 'unknown'
    GROUP BY listing_neighbourhood
),

top_5_neighbourhoods AS (
    SELECT 
        listing_neighbourhood,
        avg_revenue,
        total_stays,
        ROW_NUMBER() OVER (ORDER BY avg_revenue DESC) AS revenue_rank
    FROM neighbourhood_revenue
    WHERE total_stays > 0  -- Ensure we're only considering neighborhoods with stays
),

best_listings AS (
    SELECT 
        p.property_type,
        p.room_type,
        f.accommodates,
        n.listing_neighbourhood,
        SUM(d.total_stays) AS total_stays,  -- Aggregate total stays from neighbourhood data
        ROW_NUMBER() OVER (PARTITION BY n.listing_neighbourhood ORDER BY SUM(d.total_stays) DESC) AS stay_rank
    FROM gold.listing_dim p 
    JOIN gold.fact_listing_data f ON p.listing_id = f.listing_id
    JOIN top_5_neighbourhoods n ON p.listing_neighbourhood = n.listing_neighbourhood
    JOIN gold.dm_listing_neighbourhood d ON n.listing_neighbourhood = d.listing_neighbourhood
    GROUP BY 
        p.property_type,
        p.room_type,
        f.accommodates,
        n.listing_neighbourhood
)

-- Select the best type of listing for the top 5 neighbourhoods based on total stays
SELECT 
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    total_stays
FROM best_listings
WHERE stay_rank = 1  -- Ensure we only take the best listing per neighbourhood
ORDER BY total_stays desc 
limit 5;  -- Order by neighbourhood for clarity




-- Question 4:
WITH host_listings AS (
    SELECT 
        f.HOST_ID,
        l.LISTING_NEIGHBOURHOOD AS LGA
    FROM gold.fact_listing_data f
    JOIN gold.listing_dim l ON f.LISTING_ID = l.LISTING_ID
),

hosts_with_multiple_listings AS (
    SELECT 
        HOST_ID,
        COUNT(DISTINCT LISTING_ID) AS listing_count
    FROM gold.fact_listing_data
    GROUP BY HOST_ID
    HAVING COUNT(DISTINCT LISTING_ID) > 1
),

host_lga_counts AS (
    SELECT 
        hl.HOST_ID,
        COUNT(DISTINCT hl.LGA) AS unique_lga_count
    FROM host_listings hl
    JOIN hosts_with_multiple_listings hml ON hl.HOST_ID = hml.HOST_ID
    GROUP BY hl.HOST_ID
),

host_concentration AS (
    SELECT 
        HOST_ID,
        unique_lga_count,
        CASE 
            WHEN unique_lga_count = 1 THEN 'Concentrated'
            ELSE 'Distributed'
        END AS lga_distribution
    FROM host_lga_counts
)

SELECT 
    lga_distribution,
    COUNT(HOST_ID) AS host_count
FROM host_concentration
GROUP BY lga_distribution;





-- Question 5:
WITH unique_listings AS (
    SELECT 
        l.listing_neighbourhood AS LGA,
        f.host_id,
        SUM(d.avg_revenue_per_active_listing) AS total_annual_revenue, -- Total revenue for all listings of a host
        COUNT(*) OVER (PARTITION BY f.host_id, l.listing_neighbourhood) AS listing_count -- Count listings per host in each LGA
    FROM gold.fact_listing_data f 
    JOIN gold.listing_dim l ON f.LISTING_ID = l.LISTING_ID
    JOIN gold.dm_listing_neighbourhood d ON l.listing_neighbourhood = d.listing_neighbourhood
    WHERE l.listing_neighbourhood != 'unknown'
    GROUP BY l.listing_neighbourhood, f.host_id
),

-- Filter to retain only hosts with a single listing per LGA
single_listing_hosts AS (
    SELECT * 
    FROM unique_listings
    WHERE listing_count = 1
),

mortgage AS (
    SELECT 
        c.LGA_NAME AS LGA,
        r.median_mortgage_repay_monthly * 12 AS median_mortgage_annual
    FROM gold.dim_census_g02 r 
    JOIN gold.dim_lga_code c ON r.LGA_CODE = c.LGA_CODE
),

-- Determine repayment eligibility for hosts with a single listing
repayment_eligibility AS (
    SELECT 
        u.LGA,
        u.host_id,
        u.total_annual_revenue,  -- Use total annual revenue for hosts with a single listing
        m.median_mortgage_annual,
        CASE 
            WHEN u.total_annual_revenue > m.median_mortgage_annual THEN 1
            ELSE 0
        END AS can_repay
    FROM single_listing_hosts u 
    JOIN mortgage m ON u.LGA = m.LGA
)

-- Aggregate to the LGA level to find the percentage of hosts who can cover their mortgage
SELECT 
    LGA,
    COUNT(DISTINCT host_id) AS total_hosts,
    SUM(can_repay) AS hosts_can_repay,
    ROUND((SUM(can_repay) * 100.0 / NULLIF(COUNT(DISTINCT host_id), 0)), 2) AS repayment_possibility_percentage,
    MAX(median_mortgage_annual) AS median_mortgage_annual
FROM repayment_eligibility
GROUP BY LGA
ORDER BY repayment_possibility_percentage DESC;

