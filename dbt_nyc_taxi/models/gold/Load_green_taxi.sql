{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='taxi_trip_id'
  )
}}

select * from {{ ref('Green_taxi_transforming') }}