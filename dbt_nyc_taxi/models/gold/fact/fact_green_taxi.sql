{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='green_taxi_trip_id'
  )
}}

SELECT * 
FROM {{ ref('green_taxi_transforming') }}
{% if is_incremental() %}
WHERE ingestion_at > (SELECT MAX(ingestion_at) FROM {{ this }})
{% endif %}
