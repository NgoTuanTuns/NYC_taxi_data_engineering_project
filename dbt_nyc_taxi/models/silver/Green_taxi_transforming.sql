{{
  config(
    materialized = 'incremental',
    incremental_strategy='merge'
    )
}}
WITH source as(
    SELECT *
    FROM {{ ref('green_taxi') }}
    {% if is_incremental() %}
    WHERE ingestion_at > (SELECT MAX(ingestion_at) FROM {{ this }})
    {% endif %}
),

deduplication as(
SELECT *
FROM(
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY 
            vendorId,
            lpep_pickup_datetime,
            lpep_dropoff_datetime,
            PULocationID,
            DOLocationID,
            passenger_count,
            trip_distance,
            fare_amount,
            total_amount 
            ORDER BY lpep_pickup_datetime
        ) AS row_number
    FROM source
)t 
WHERE row_number = 1
),

complete_null_handling AS (
    SELECT *,
        CASE
        WHEN passenger_count IS NULL 
            AND RatecodeID IS NULL 
            AND store_AND_fwd_flag IS NULL 
            THEN 'Vendor_feed_incomplete'
        WHEN passenger_count = 0 THEN 'Not_recorded'
        WHEN passenger_count IS NULL THEN 'Partial_null'
    ELSE 'Complete' END 
    AS data_completeness_flag
    FROM deduplication
),
handling_invalid_value as(
    SELECT *,
        CASE 
            WHEN TIMEDIFF(SECOND, lpep_pickup_datetime, lpep_dropoff_datetime) <= 0 THEN 'invalid_pickup_and_drop_time'
            WHEN trip_distance < 0 THEN 'invalid_distance'
            WHEN passenger_count < 0 THEN 'invalid_passenger_count'
            WHEN mta_tax < 0 THEN 'invalid_tax'
            WHEN improvement_surcharge < 0 THEN 'invalid_surcharge'
            WHEN fare_amount < 0 AND payment_type not IN (4,6) THEN 'suspicious_negative_fare'
            WHEN total_amount < 0 AND payment_type not IN (4,6) THEN 'suspicious_negative_total'
            WHEN fare_amount < 0 or total_amount < 0 AND payment_type IN (4,6) THEN 'valid_refund'
        ELSE 'valid'
    END AS row_quality_flag
    FROM complete_null_handling
),

valid_data as (
    SELECT *
    FROM handling_invalid_value
    WHERE row_quality_flag NOT IN ('invalid_distance', 'invalid_passenger', 'invalid_tax', 'invalid_surcharge', 'invalid_pickup_and_drop_time')
),


cast_data_types_and_enrich AS(
SELECT
    VendorID,
    cast(lpep_pickup_datetime AS TIMESTAMP_NTZ),
    cast(lpep_dropoff_datetime AS TIMESTAMP_NTZ),
    cast(cast(timestampdiff(HOUR, lpep_pickup_datetime, lpep_dropoff_datetime) AS STRING) || ":" ||
    cast(timestampdiff(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) - timestampdiff(HOUR, lpep_pickup_datetime, lpep_dropoff_datetime)*60 AS STRING) || ":" ||
    cast(timestampdiff(SECOND, lpep_pickup_datetime, lpep_dropoff_datetime) - timestampdiff(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime)*60 AS STRING) AS TIME) AS trip_duration,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    trip_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    cbd_congestion_fee,
    ingestion_at,
    data_completeness_flag,
    row_quality_flag
FROM valid_data
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'VendorID', 
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'total_amount'
    ]) }} AS taxi_trip_id,
    *
FROM cast_data_types_and_enrich