WITH source AS (
    SELECT * FROM {{ ref('yellow_taxi') }}
    WHERE ingestion_at = (SELECT max(ingestion_at) FROM {{ ref('yellow_taxi') }})
),

deduplicate AS(
    SELECT *
    FROM(
        SELECT*,
        ROW_NUMBER() OVER(PARTITION BY Vendorid, 
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        PULocationID,
        DOLocationID,
        fare_amount,
        passenger_count,
        total_amount ORDER BY tpep_pickup_datetime) AS row_num
        FROM source
    )t
    WHERE row_num = 1
),

complete_null_handling AS(
SELECT
*,
    CASE 
    WHEN passenger_count = 0 THEN 'Not_recorded'
    WHEN passenger_count IS NULL AND RatecodeID IS NULL AND store_and_fwd_flag IS NULL THEN 'Incomplete_vendor_feed'
    WHEN passenger_count IS NULL THEN 'Partial_null'
    ELSE 'Complete'
    END as data_completeness_flag
FROM deduplicate
{# MNAR #}
)



, handling_invalid_value as(
    SELECT
        *,
        CASE
            WHEN timestampdiff(second, tpep_pickup_datetime, tpep_dropoff_datetime) <= 0 THEN 'invalid_pickup_and_drop_time'
            WHEN trip_distance < 0 THEN 'invalid_distance'
            WHEN passenger_count < 0 THEN 'invalid_passenger'
            WHEN mta_tax < 0 THEN 'invalid_tax'
            WHEN improvement_surcharge < 0 THEN 'invalid_surcharge'
            WHEN fare_amount < 0 AND payment_type NOT IN (4, 6) THEN 'suspicious_negative_fare'
            WHEN total_amount < 0 AND payment_type NOT IN (4, 6) THEN 'suspicious_negative_total'
            WHEN fare_amount < 0 AND payment_type IN (4, 6) THEN 'valid_refund'
            ELSE 'valid'
        END AS row_quality_flag
    FROM complete_null_handling
),
valid_data as (
    SELECT *
    FROM handling_invalid_value
    WHERE row_quality_flag NOT IN ('invalid_distance', 'invalid_passenger', 'invalid_tax', 'invalid_surcharge', 'invalid_pickup_and_drop_time')
),


cast_data_types_and_enrich as(
select
    VendorID,
    cast(tpep_pickup_datetime as TIMESTAMP_NTZ),
    cast(tpep_dropoff_datetime as TIMESTAMP_NTZ),
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
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee,
    cbd_congestion_fee,
    data_completeness_flag,
    row_quality_flag
from valid_data
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'VendorID', 
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'total_amount'
    ]) }} AS taxi_trip_id,
    *
FROM cast_data_types_and_enrich



