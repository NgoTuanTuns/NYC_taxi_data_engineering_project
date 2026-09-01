{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='yellow_taxi_trip_id'
  )
}}

SELECT * FROM {{ ref('yellow_taxi_transforming') }}