{{
    config(
        unique_key='LISTING_ID',
        alias='facts'
    )
}}

select * from BRONZE.LISTING_DATA