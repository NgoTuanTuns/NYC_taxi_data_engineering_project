{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='taxi_trip_id'
  )
}}

SELECT * FROM {{ ref('Green_taxi_transforming') }}