{{
    config(
        unique_key='LGA_CODE',
        alias='dim_census_g02'
    )
}}

with

source  as (

    select * from {{ ref('s_census_g02') }}

),

cleaned as (
    select
        LGA_CODE,
        Median_age_persons,
        Median_mortgage_repay_monthly,
        Median_tot_prsnl_inc_weekly,
        Median_rent_weekly,
        Median_tot_fam_inc_weekly,
        Average_num_psns_per_bedroom,
        Median_tot_hhd_inc_weekly,
        Average_household_size
    from source
),

unknown as (
    select
        0 as LGA_CODE,
        0 as Median_age_persons,
        0 as Median_mortgage_repay_monthly,
        0 as Median_tot_prsnl_inc_weekly,
        0 as Median_rent_weekly,
        0 as Median_tot_fam_inc_weekly,
        0 as Average_num_psns_per_bedroom,
        0 as Median_tot_hhd_inc_weekly,
        0 as Average_household_size
)
select * from unknown
union all
select * from cleaned