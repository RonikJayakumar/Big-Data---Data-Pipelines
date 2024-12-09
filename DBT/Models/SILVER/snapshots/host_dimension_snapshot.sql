{% snapshot host_dimension_snapshot %}

{{
    config(
      strategy='check',
      unique_key='HOST_ID',
      check_cols=['HOST_ID', 'HOST_NAME', 'HOST_SINCE', 'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD'],
      alias='host_dimension_snapshot'
    )
}}


SELECT
    HOST_ID,
    HOST_NAME,
    HOST_SINCE,
    HOST_IS_SUPERHOST,
    HOST_NEIGHBOURHOOD
FROM {{ ref('s_facts') }}

{% endsnapshot %}