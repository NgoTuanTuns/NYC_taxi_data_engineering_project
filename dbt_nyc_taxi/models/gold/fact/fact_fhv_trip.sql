{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'fhv_trip_id'
    )
}}

SELECT * FROM {{ ref('fhv_transforming') }}