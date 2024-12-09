{% snapshot property_dimension_snapshot %}

{{
    config(
      strategy='check',
      unique_key='LISTING_ID',
      check_cols=['LISTING_ID', 'PROPERTY_TYPE', 'ROOM_TYPE', 'HAS_AVAILABILITY'],
      alias='property_dimension_snapshot'
    )
}}


SELECT
    LISTING_ID,
    PROPERTY_TYPE,
    ROOM_TYPE,
    HAS_AVAILABILITY
FROM {{ ref('s_facts') }}

{% endsnapshot %}