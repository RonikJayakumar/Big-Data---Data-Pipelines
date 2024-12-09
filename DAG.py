import os
import logging
import requests
import pandas as pd
import shutil
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

#########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'bde-assignment-3',
    'start_date': datetime.now() - timedelta(days=2),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='assignment3.2',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Load Environment Variables
#
#########################################################

AIRFLOW_DATA = "/home/airflow/gcs/data"
DIMENSIONS = AIRFLOW_DATA+"/dimensions/"
FACTS = AIRFLOW_DATA+"/facts/"

#########################################################
#
#   Custom Logics for Operator
#
#########################################################

def load_census_g01_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    census_file_path = DIMENSIONS + '2016Census_G01_NSW_LGA.csv'
    if not os.path.exists(census_file_path):
        logging.info("No census_g01 file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(census_file_path)

    if len(df) > 0:
        col_names = [
            'LGA_CODE_2016','Tot_P_M','Tot_P_F','Tot_P_P','Age_0_4_yr_M','Age_0_4_yr_F','Age_0_4_yr_P','Age_5_14_yr_M','Age_5_14_yr_F','Age_5_14_yr_P','Age_15_19_yr_M',
                     'Age_15_19_yr_F','Age_15_19_yr_P','Age_20_24_yr_M','Age_20_24_yr_F','Age_20_24_yr_P','Age_25_34_yr_M','Age_25_34_yr_F','Age_25_34_yr_P',
                     'Age_35_44_yr_M','Age_35_44_yr_F','Age_35_44_yr_P','Age_45_54_yr_M','Age_45_54_yr_F','Age_45_54_yr_P','Age_55_64_yr_M','Age_55_64_yr_F',
                     'Age_55_64_yr_P','Age_65_74_yr_M','Age_65_74_yr_F','Age_65_74_yr_P','Age_75_84_yr_M','Age_75_84_yr_F','Age_75_84_yr_P','Age_85ov_M',
                     'Age_85ov_F','Age_85ov_P','Counted_Census_Night_home_M','Counted_Census_Night_home_F','Counted_Census_Night_home_P',
                     'Count_Census_Nt_Ewhere_Aust_M','Count_Census_Nt_Ewhere_Aust_F','Count_Census_Nt_Ewhere_Aust_P','Indigenous_psns_Aboriginal_M',
                     'Indigenous_psns_Aboriginal_F','Indigenous_psns_Aboriginal_P','Indig_psns_Torres_Strait_Is_M','Indig_psns_Torres_Strait_Is_F',
                     'Indig_psns_Torres_Strait_Is_P','Indig_Bth_Abor_Torres_St_Is_M','Indig_Bth_Abor_Torres_St_Is_F','Indig_Bth_Abor_Torres_St_Is_P',
                     'Indigenous_P_Tot_M','Indigenous_P_Tot_F','Indigenous_P_Tot_P','Birthplace_Australia_M','Birthplace_Australia_F','Birthplace_Australia_P',
                     'Birthplace_Elsewhere_M','Birthplace_Elsewhere_F','Birthplace_Elsewhere_P','Lang_spoken_home_Eng_only_M','Lang_spoken_home_Eng_only_F',
                     'Lang_spoken_home_Eng_only_P','Lang_spoken_home_Oth_Lang_M','Lang_spoken_home_Oth_Lang_F','Lang_spoken_home_Oth_Lang_P',
                     'Australian_citizen_M','Australian_citizen_F','Australian_citizen_P','Age_psns_att_educ_inst_0_4_M','Age_psns_att_educ_inst_0_4_F',
                     'Age_psns_att_educ_inst_0_4_P','Age_psns_att_educ_inst_5_14_M','Age_psns_att_educ_inst_5_14_F','Age_psns_att_educ_inst_5_14_P',
                     'Age_psns_att_edu_inst_15_19_M','Age_psns_att_edu_inst_15_19_F','Age_psns_att_edu_inst_15_19_P','Age_psns_att_edu_inst_20_24_M',
                     'Age_psns_att_edu_inst_20_24_F','Age_psns_att_edu_inst_20_24_P','Age_psns_att_edu_inst_25_ov_M','Age_psns_att_edu_inst_25_ov_F',
                     'Age_psns_att_edu_inst_25_ov_P','High_yr_schl_comp_Yr_12_eq_M','High_yr_schl_comp_Yr_12_eq_F','High_yr_schl_comp_Yr_12_eq_P',
                     'High_yr_schl_comp_Yr_11_eq_M','High_yr_schl_comp_Yr_11_eq_F','High_yr_schl_comp_Yr_11_eq_P','High_yr_schl_comp_Yr_10_eq_M',
                     'High_yr_schl_comp_Yr_10_eq_F','High_yr_schl_comp_Yr_10_eq_P','High_yr_schl_comp_Yr_9_eq_M','High_yr_schl_comp_Yr_9_eq_F',
                     'High_yr_schl_comp_Yr_9_eq_P','High_yr_schl_comp_Yr_8_belw_M','High_yr_schl_comp_Yr_8_belw_F','High_yr_schl_comp_Yr_8_belw_P',
                     'High_yr_schl_comp_D_n_g_sch_M','High_yr_schl_comp_D_n_g_sch_F','High_yr_schl_comp_D_n_g_sch_P','Count_psns_occ_priv_dwgs_M',
                     'Count_psns_occ_priv_dwgs_F','Count_psns_occ_priv_dwgs_P','Count_Persons_other_dwgs_M','Count_Persons_other_dwgs_F',
                     'Count_Persons_other_dwgs_P'
        ]
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bronze.census_g01 (LGA_CODE_2016,Tot_P_M,Tot_P_F,Tot_P_P,Age_0_4_yr_M,Age_0_4_yr_F,Age_0_4_yr_P,Age_5_14_yr_M,Age_5_14_yr_F,
                    Age_5_14_yr_P,Age_15_19_yr_M,Age_15_19_yr_F,Age_15_19_yr_P,Age_20_24_yr_M,Age_20_24_yr_F,Age_20_24_yr_P,Age_25_34_yr_M,Age_25_34_yr_F,
                    Age_25_34_yr_P,Age_35_44_yr_M,Age_35_44_yr_F,Age_35_44_yr_P,Age_45_54_yr_M,Age_45_54_yr_F,Age_45_54_yr_P,Age_55_64_yr_M,Age_55_64_yr_F,
                    Age_55_64_yr_P,Age_65_74_yr_M,Age_65_74_yr_F,Age_65_74_yr_P,Age_75_84_yr_M,Age_75_84_yr_F,Age_75_84_yr_P,Age_85ov_M,Age_85ov_F,Age_85ov_P,
                    Counted_Census_Night_home_M,Counted_Census_Night_home_F,Counted_Census_Night_home_P,Count_Census_Nt_Ewhere_Aust_M,Count_Census_Nt_Ewhere_Aust_F,
                    Count_Census_Nt_Ewhere_Aust_P,Indigenous_psns_Aboriginal_M,Indigenous_psns_Aboriginal_F,Indigenous_psns_Aboriginal_P,Indig_psns_Torres_Strait_Is_M,
                    Indig_psns_Torres_Strait_Is_F,Indig_psns_Torres_Strait_Is_P,Indig_Bth_Abor_Torres_St_Is_M,Indig_Bth_Abor_Torres_St_Is_F,Indig_Bth_Abor_Torres_St_Is_P,
                    Indigenous_P_Tot_M,Indigenous_P_Tot_F,Indigenous_P_Tot_P,Birthplace_Australia_M,Birthplace_Australia_F,Birthplace_Australia_P,Birthplace_Elsewhere_M,
                    Birthplace_Elsewhere_F,Birthplace_Elsewhere_P,Lang_spoken_home_Eng_only_M,Lang_spoken_home_Eng_only_F,Lang_spoken_home_Eng_only_P,Lang_spoken_home_Oth_Lang_M,
                    Lang_spoken_home_Oth_Lang_F,Lang_spoken_home_Oth_Lang_P,Australian_citizen_M,Australian_citizen_F,Australian_citizen_P,Age_psns_att_educ_inst_0_4_M,
                    Age_psns_att_educ_inst_0_4_F,Age_psns_att_educ_inst_0_4_P,Age_psns_att_educ_inst_5_14_M,Age_psns_att_educ_inst_5_14_F,Age_psns_att_educ_inst_5_14_P,
                    Age_psns_att_edu_inst_15_19_M,Age_psns_att_edu_inst_15_19_F,Age_psns_att_edu_inst_15_19_P,Age_psns_att_edu_inst_20_24_M,Age_psns_att_edu_inst_20_24_F,
                    Age_psns_att_edu_inst_20_24_P,Age_psns_att_edu_inst_25_ov_M,Age_psns_att_edu_inst_25_ov_F,Age_psns_att_edu_inst_25_ov_P,High_yr_schl_comp_Yr_12_eq_M,
                    High_yr_schl_comp_Yr_12_eq_F,High_yr_schl_comp_Yr_12_eq_P,High_yr_schl_comp_Yr_11_eq_M,High_yr_schl_comp_Yr_11_eq_F,High_yr_schl_comp_Yr_11_eq_P,
                    High_yr_schl_comp_Yr_10_eq_M,High_yr_schl_comp_Yr_10_eq_F,High_yr_schl_comp_Yr_10_eq_P,High_yr_schl_comp_Yr_9_eq_M,High_yr_schl_comp_Yr_9_eq_F,
                    High_yr_schl_comp_Yr_9_eq_P,High_yr_schl_comp_Yr_8_belw_M,High_yr_schl_comp_Yr_8_belw_F,High_yr_schl_comp_Yr_8_belw_P,High_yr_schl_comp_D_n_g_sch_M,
                    High_yr_schl_comp_D_n_g_sch_F,High_yr_schl_comp_D_n_g_sch_P,Count_psns_occ_priv_dwgs_M,Count_psns_occ_priv_dwgs_F,Count_psns_occ_priv_dwgs_P,
                    Count_Persons_other_dwgs_M,Count_Persons_other_dwgs_F,Count_Persons_other_dwgs_P)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(DIMENSIONS, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(census_file_path, os.path.join(archive_folder, '2016Census_G01_NSW_LGA.csv'))
    return None
    
    
def load_census_g02_func(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()

    # Path to the census G02 file
    census_file_path2 = DIMENSIONS + '2016Census_G02_NSW_LGA.csv'
    
    # Check if the file exists
    if not os.path.exists(census_file_path2):
        logging.info("No census_g02 file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(census_file_path2)
    
    # Check if the dataframe is empty
    if df.empty:
        logging.error("DataFrame is empty, exiting.")
        return None

    # If the dataframe is not empty, proceed
    if len(df) > 0:
        col_names = ['LGA_CODE_2016', 'Median_age_persons', 'Median_mortgage_repay_monthly', 'Median_tot_prsnl_inc_weekly', 
                     'Median_rent_weekly', 'Median_tot_fam_inc_weekly', 'Average_num_psns_per_bedroom', 
                     'Median_tot_hhd_inc_weekly', 'Average_household_size']
        
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bronze.census_g02 (LGA_CODE_2016, Median_age_persons, Median_mortgage_repay_monthly, 
                    Median_tot_prsnl_inc_weekly, Median_rent_weekly, Median_tot_fam_inc_weekly, 
                    Average_num_psns_per_bedroom, Median_tot_hhd_inc_weekly, Average_household_size) 
                    VALUES %s
                    """
        
        # Use execute_values to batch insert the data
        result = execute_values(cursor, insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(DIMENSIONS, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(census_file_path2, os.path.join(archive_folder, '2016Census_G02_NSW_LGA.csv'))

    return None


def load_lga_code(**kwargs):

    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    lga_file_path = DIMENSIONS + 'NSW_LGA_CODE.csv'
    if not os.path.exists(lga_file_path):
        logging.info("No NSW LGA CODE file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(lga_file_path)

    if len(df) > 0:
        col_names = ['LGA_CODE', 'LGA_NAME']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bronze.lga_code (LGA_CODE, LGA_NAME)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(DIMENSIONS, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(lga_file_path, os.path.join(archive_folder, 'NSW_LGA_CODE.csv'))
    return None
    

def load_lga_suburb(**kwargs):

    # Set up Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    suburb_file_path = DIMENSIONS+'NSW_LGA_SUBURB.csv'
    if not os.path.exists(suburb_file_path):
        logging.info("No NSW LGA SUBURB file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(suburb_file_path)

    if len(df) > 0:
        col_names = ['LGA_NAME','SUBURB_NAME']

        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO BRONZE.LGA_SUBURB (LGA_NAME,SUBURB_NAME)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(DIMENSIONS, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(suburb_file_path, os.path.join(archive_folder, 'NSW_LGA_SUBURB.csv'))
    
    return None


def load_facts(**kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cursor_ps = conn_ps.cursor()

    # Get list of CSV files and sort them by date
    filelist = sorted(
    [k for k in os.listdir(FACTS) if k.endswith('.csv')],
    key=lambda x: (int(x.split('_')[1].split('.')[0]), int(x.split('_')[0]))
    )
    
    if len(filelist) == 0:
        logging.info("No CSV files found in the FACTS directory.")
        return None  # Exit gracefully if no files are found

    for fname in filelist:
        logging.info(f"Processing file: {fname}")
        df = pd.read_csv(os.path.join(FACTS, fname))
        
        # Verify and process DataFrame
        if len(df) > 0:
            # List of column names
            col_names = [
                'LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME', 'HOST_SINCE',
                'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD', 'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE',
                'ROOM_TYPE', 'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
                'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING', 'REVIEW_SCORES_ACCURACY', 'REVIEW_SCORES_CLEANLINESS',
                'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION', 'REVIEW_SCORES_VALUE'
            ]

            # Data type conversions
        # Convert columns to appropriate data types
        df['LISTING_ID'] = df['LISTING_ID'].astype(str)
        df['SCRAPE_ID'] = df['SCRAPE_ID'].astype(str)

        # Convert 'SCRAPED_DATE' to ISO format (YYYY-MM-DD) from DMY format
        if 'SCRAPED_DATE' in df.columns:
            df['SCRAPED_DATE'] = pd.to_datetime(df['SCRAPED_DATE'], format='%Y-%m-%d', errors='coerce')
            df['SCRAPED_DATE'] = df['SCRAPED_DATE'].fillna(pd.to_datetime('01/01/1990'))
            df['SCRAPED_DATE'] = df['SCRAPED_DATE'].dt.strftime('%Y-%m-%d')  # Change to YYYY-MM-DD

        df['HOST_ID'] = df['HOST_ID'].astype(int)
        df['HOST_NAME'] = df['HOST_NAME'].astype(str)

        # Convert 'HOST_SINCE' to ISO format (YYYY-MM-DD) from DMY format
        if 'HOST_SINCE' in df.columns:
            df['HOST_SINCE'] = pd.to_datetime(df['HOST_SINCE'], format='%d/%m/%Y', errors='coerce')
            df['HOST_SINCE'] = df['HOST_SINCE'].fillna(pd.to_datetime('01/01/1990'))
            df['HOST_SINCE'] = df['HOST_SINCE'].dt.strftime('%Y-%m-%d')  # Change to YYYY-MM-DD

        # Continue converting other columns
        df['HOST_IS_SUPERHOST'] = df['HOST_IS_SUPERHOST'].astype(str)
        df['HOST_NEIGHBOURHOOD'] = df['HOST_NEIGHBOURHOOD'].astype(str)
        df['LISTING_NEIGHBOURHOOD'] = df['LISTING_NEIGHBOURHOOD'].astype(str)
        df['PROPERTY_TYPE'] = df['PROPERTY_TYPE'].astype(str)
        df['ROOM_TYPE'] = df['ROOM_TYPE'].astype(str)
        df['ACCOMMODATES'] = df['ACCOMMODATES'].astype(int)
        df['PRICE'] = df['PRICE'].astype(int)
        df['HAS_AVAILABILITY'] = df['HAS_AVAILABILITY'].astype(str)
        df['AVAILABILITY_30'] = df['AVAILABILITY_30'].fillna(0).astype(int)
        df['NUMBER_OF_REVIEWS'] = df['NUMBER_OF_REVIEWS'].fillna(0).astype(int)
        df['REVIEW_SCORES_RATING'] = df['REVIEW_SCORES_RATING'].fillna(0).astype(int)
        df['REVIEW_SCORES_ACCURACY'] = df['REVIEW_SCORES_ACCURACY'].fillna(0).astype(int)
        df['REVIEW_SCORES_CLEANLINESS'] = df['REVIEW_SCORES_CLEANLINESS'].fillna(0).astype(int)
        df['REVIEW_SCORES_CHECKIN'] = df['REVIEW_SCORES_CHECKIN'].fillna(0).astype(int)
        df['REVIEW_SCORES_COMMUNICATION'] = df['REVIEW_SCORES_COMMUNICATION'].fillna(0).astype(int)
        df['REVIEW_SCORES_VALUE'] = df['REVIEW_SCORES_VALUE'].fillna(0).astype(int)

            # Insert data
        values = df[col_names].values.tolist()
        insert_sql = """
                INSERT INTO bronze.LISTING_DATA (
                    LISTING_ID, SCRAPE_ID, SCRAPED_DATE, HOST_ID, HOST_NAME, HOST_SINCE,
                    HOST_IS_SUPERHOST, HOST_NEIGHBOURHOOD, LISTING_NEIGHBOURHOOD, PROPERTY_TYPE, ROOM_TYPE,
                    ACCOMMODATES, PRICE, HAS_AVAILABILITY, AVAILABILITY_30, NUMBER_OF_REVIEWS,
                    REVIEW_SCORES_RATING, REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS, REVIEW_SCORES_CHECKIN,
                    REVIEW_SCORES_COMMUNICATION, REVIEW_SCORES_VALUE) 
                VALUES %s
               	ON CONFLICT (listing_id) DO NOTHING;

            """
        execute_values(cursor_ps, insert_sql, values)
        conn_ps.commit()
        logging.info(f"Data for {fname} inserted successfully.")

        # Archive processed file
        archive_folder = os.path.join(FACTS, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(os.path.join(FACTS, fname), os.path.join(archive_folder, fname))
        logging.info(f"{fname} moved to archive.")
    
    return None



#########################################################
#
#   Function to trigger dbt Cloud Job
#
#########################################################

def trigger_dbt_cloud_job(**kwargs):
    dbt_cloud_url = Variable.get("DBT_CLOUD_URL")
    dbt_cloud_account_id = Variable.get("DBT_CLOUD_ACCOUNT_ID")
    dbt_cloud_job_id = Variable.get("DBT_CLOUD_JOB_ID")
    
    # Define the URL for the dbt Cloud job API dynamically using URL, account ID, and job ID
    url = f"https://{dbt_cloud_url}/api/v2/accounts/{dbt_cloud_account_id}/jobs/{dbt_cloud_job_id}/run/"
    
    # Get the dbt Cloud API token from Airflow Variables
    dbt_cloud_token = Variable.get("DBT_CLOUD_API_TOKEN")
    
    # Define the headers and body for the request
    headers = {
        'Authorization': f'Token {dbt_cloud_token}',
        'Content-Type': 'application/json'
    }
    data = {
        "cause": "Triggered via API"
    }
    
    # Make the POST request to trigger the dbt Cloud job
    response = requests.post(url, headers=headers, json=data)
    
    # Check if the response is successful
    if response.status_code == 200:
        logging.info("Successfully triggered dbt Cloud job.")
        return response.json()
    else:
        logging.error(f"Failed to trigger dbt Cloud job: {response.status_code}, {response.text}")
        raise AirflowException("Failed to trigger dbt Cloud job.")

#########################################################
#
#   DAG Operator Setup
#
#########################################################

load_census_g01_task = PythonOperator(
    task_id="load_census_g01_id",
    python_callable=load_census_g01_func,
    provide_context=True,
    dag=dag
)

load_census_g02_task = PythonOperator(
    task_id="load_census_g02_id",
    python_callable=load_census_g02_func,
    provide_context=True,
    dag=dag
)

load_lga_code_task = PythonOperator(
    task_id="load_lga_code_id",
    python_callable=load_lga_code,
    provide_context=True,
    dag=dag
)

load_lga_suburb_task = PythonOperator(
    task_id="load_lga_suburb_id",
    python_callable=load_lga_suburb,
    provide_context=True,
    dag=dag
)

load_facts_task = PythonOperator(
    task_id="load_facts_id",
    python_callable=load_facts,
    provide_context=True,
    dag=dag
)

trigger_dbt_job_task = PythonOperator(
    task_id='trigger_dbt_job',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)

[load_census_g01_task, load_census_g02_task, load_facts_task, load_lga_suburb_task, load_lga_code_task] >> trigger_dbt_job_task
