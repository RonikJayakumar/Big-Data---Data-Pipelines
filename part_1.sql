-- Drop and Create Schema
DROP SCHEMA IF EXISTS BRONZE CASCADE;
CREATE SCHEMA BRONZE;
---------------------------
-- Bronze Tables
---------------------------

-- CENSUS_G01 Table
CREATE TABLE BRONZE.CENSUS_G01 (
    LGA_CODE_2016 VARCHAR(50) PRIMARY KEY,
    Tot_P_M INT,
    Tot_P_F INT,
    Tot_P_P INT,
    Age_0_4_yr_M INT NULL,
    Age_0_4_yr_F INT NULL,
    Age_0_4_yr_P INT NULL,
    Age_5_14_yr_M INT NULL,
    Age_5_14_yr_F INT NULL,
    Age_5_14_yr_P INT NULL,
    Age_15_19_yr_M INT NULL,
    Age_15_19_yr_F INT NULL,
    Age_15_19_yr_P INT NULL,
    Age_20_24_yr_M INT NULL,
    Age_20_24_yr_F INT NULL,
    Age_20_24_yr_P INT NULL,
    Age_25_34_yr_M INT NULL,
    Age_25_34_yr_F INT NULL,
    Age_25_34_yr_P INT NULL,
    Age_35_44_yr_M INT NULL,
    Age_35_44_yr_F INT NULL,
    Age_35_44_yr_P INT NULL,
    Age_45_54_yr_M INT NULL,
    Age_45_54_yr_F INT NULL,
    Age_45_54_yr_P INT NULL,
    Age_55_64_yr_M INT NULL,
    Age_55_64_yr_F INT NULL,
    Age_55_64_yr_P INT NULL,
    Age_65_74_yr_M INT NULL,
    Age_65_74_yr_F INT NULL,
    Age_65_74_yr_P INT NULL,
    Age_75_84_yr_M INT NULL,
    Age_75_84_yr_F INT NULL,
    Age_75_84_yr_P INT NULL,
    Age_85ov_M INT NULL,
    Age_85ov_F INT NULL,
    Age_85ov_P INT NULL,
    Counted_Census_Night_home_M INT NULL,
    Counted_Census_Night_home_F INT NULL,
    Counted_Census_Night_home_P INT NULL,
    Count_Census_Nt_Ewhere_Aust_M INT NULL,
    Count_Census_Nt_Ewhere_Aust_F INT NULL,
    Count_Census_Nt_Ewhere_Aust_P INT NULL,
    Indigenous_psns_Aboriginal_M INT NULL,
    Indigenous_psns_Aboriginal_F INT NULL,
    Indigenous_psns_Aboriginal_P INT NULL,
    Indig_psns_Torres_Strait_Is_M INT NULL,
    Indig_psns_Torres_Strait_Is_F INT NULL,
    Indig_psns_Torres_Strait_Is_P INT NULL,
    Indig_Bth_Abor_Torres_St_Is_M INT NULL,
    Indig_Bth_Abor_Torres_St_Is_F INT NULL,
    Indig_Bth_Abor_Torres_St_Is_P INT NULL,
    Indigenous_P_Tot_M INT NULL,
    Indigenous_P_Tot_F INT NULL,
    Indigenous_P_Tot_P INT NULL,
    Birthplace_Australia_M INT NULL,
    Birthplace_Australia_F INT NULL,
    Birthplace_Australia_P INT NULL,
    Birthplace_Elsewhere_M INT NULL,
    Birthplace_Elsewhere_F INT NULL,
    Birthplace_Elsewhere_P INT NULL,
    Lang_spoken_home_Eng_only_M INT NULL,
    Lang_spoken_home_Eng_only_F INT NULL,
    Lang_spoken_home_Eng_only_P INT NULL,
    Lang_spoken_home_Oth_Lang_M INT NULL,
    Lang_spoken_home_Oth_Lang_F INT NULL,
    Lang_spoken_home_Oth_Lang_P INT NULL,
    Australian_citizen_M INT NULL,
    Australian_citizen_F INT NULL,
    Australian_citizen_P INT NULL,
    Age_psns_att_educ_inst_0_4_M INT NULL,
    Age_psns_att_educ_inst_0_4_F INT NULL,
    Age_psns_att_educ_inst_0_4_P INT NULL,
    Age_psns_att_educ_inst_5_14_M INT NULL,
    Age_psns_att_educ_inst_5_14_F INT NULL,
    Age_psns_att_educ_inst_5_14_P INT NULL,
    Age_psns_att_edu_inst_15_19_M INT NULL,
    Age_psns_att_edu_inst_15_19_F INT NULL,
    Age_psns_att_edu_inst_15_19_P INT NULL,
    Age_psns_att_edu_inst_20_24_M INT NULL,
    Age_psns_att_edu_inst_20_24_F INT NULL,
    Age_psns_att_edu_inst_20_24_P INT NULL,
    Age_psns_att_edu_inst_25_ov_M INT NULL,
    Age_psns_att_edu_inst_25_ov_F INT NULL,
    Age_psns_att_edu_inst_25_ov_P INT NULL,
    High_yr_schl_comp_Yr_12_eq_M INT NULL,
    High_yr_schl_comp_Yr_12_eq_F INT NULL,
    High_yr_schl_comp_Yr_12_eq_P INT NULL,
    High_yr_schl_comp_Yr_11_eq_M INT NULL,
    High_yr_schl_comp_Yr_11_eq_F INT NULL,
    High_yr_schl_comp_Yr_11_eq_P INT NULL,
    High_yr_schl_comp_Yr_10_eq_M INT NULL,
    High_yr_schl_comp_Yr_10_eq_F INT NULL,
    High_yr_schl_comp_Yr_10_eq_P INT NULL,
    High_yr_schl_comp_Yr_9_eq_M INT NULL,
    High_yr_schl_comp_Yr_9_eq_F INT NULL,
    High_yr_schl_comp_Yr_9_eq_P INT NULL,
    High_yr_schl_comp_Yr_8_belw_M INT NULL,
    High_yr_schl_comp_Yr_8_belw_F INT NULL,
    High_yr_schl_comp_Yr_8_belw_P INT NULL,
    High_yr_schl_comp_D_n_g_sch_M INT NULL,
    High_yr_schl_comp_D_n_g_sch_F INT NULL,
    High_yr_schl_comp_D_n_g_sch_P INT NULL,
    Count_psns_occ_priv_dwgs_M INT NULL,
    Count_psns_occ_priv_dwgs_F INT NULL,
    Count_psns_occ_priv_dwgs_P INT NULL,
    Count_Persons_other_dwgs_M INT NULL,
    Count_Persons_other_dwgs_F INT NULL,
    Count_Persons_other_dwgs_P INT NULL
);

-- CENSUS_G02 Table
CREATE TABLE BRONZE.CENSUS_G02 (
    LGA_CODE_2016 VARCHAR PRIMARY KEY,
    Median_age_persons INT,
    Median_mortgage_repay_monthly INT,
    Median_tot_prsnl_inc_weekly INT,
    Median_rent_weekly INT,
    Median_tot_fam_inc_weekly INT,
    Average_num_psns_per_bedroom INT,
    Median_tot_hhd_inc_weekly INT,
    Average_household_size INT
);

-- LGA_CODE Table
CREATE TABLE BRONZE.LGA_CODE (
    LGA_CODE INT PRIMARY KEY,
    LGA_NAME VARCHAR
);

-- LGA_SUBURB Table
CREATE TABLE BRONZE.LGA_SUBURB (
    LGA_NAME VARCHAR,
    SUBURB_NAME VARCHAR
);

-- LISTING_DATA Table
CREATE TABLE BRONZE.LISTING_DATA (
    LISTING_ID VARCHAR PRIMARY KEY,
    SCRAPE_ID VARCHAR,
    SCRAPED_DATE DATE,
    HOST_ID INT,
    HOST_NAME VARCHAR(255),
    HOST_SINCE DATE,
    HOST_IS_SUPERHOST CHAR(3),
    HOST_NEIGHBOURHOOD VARCHAR(100) NULL,
    LISTING_NEIGHBOURHOOD VARCHAR(100),
    PROPERTY_TYPE VARCHAR(100),
    ROOM_TYPE VARCHAR(100),
    ACCOMMODATES INT,
    PRICE DECIMAL(10, 2),
    HAS_AVAILABILITY CHAR(3),
    AVAILABILITY_30 INT NULL,
    NUMBER_OF_REVIEWS INT NULL,
    REVIEW_SCORES_RATING INT NULL,
    REVIEW_SCORES_ACCURACY INT NULL,
    REVIEW_SCORES_CLEANLINESS INT NULL,
    REVIEW_SCORES_CHECKIN INT NULL,
    REVIEW_SCORES_COMMUNICATION INT NULL,
    REVIEW_SCORES_VALUE INT NULL
);