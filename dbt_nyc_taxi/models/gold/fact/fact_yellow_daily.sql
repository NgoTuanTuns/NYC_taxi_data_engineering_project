{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'daily_zone_vendor_key'
  )
}}

WITH silver_data AS (
    SELECT * 
    FROM {{ ref('yellow_taxi_transforming') }}
    {% if is_incremental() %}
    WHERE ingestion_at > (SELECT MAX(max_ingestion_at) FROM {{ this }})
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        "CAST(tpep_pickup_datetime AS DATE)",
        "PULocationID",
        "VendorID"
    ]) }} AS daily_zone_vendor_key,
    CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
    PULocationID AS pickup_location_id,
    VendorID AS vendor_id,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    ROUND(SUM(trip_distance), 2) AS total_trip_distance,
    ROUND(SUM(fare_amount), 2) AS total_fare_amount,
    ROUND(SUM(tip_amount), 2) AS total_tip_amount,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare_per_trip,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    MAX(ingestion_at) AS max_ingestion_at
FROM silver_data
GROUP BY 1, 2, 3, 4
