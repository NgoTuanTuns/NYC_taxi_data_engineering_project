{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'fhvhv_trip_id'
  )
}}

SELECT * 
FROM {{ ref('fhvhv_transforming') }}
{% if is_incremental() %}
WHERE ingestion_at > (SELECT MAX(ingestion_at) FROM {{ this }})
{% endif %}

