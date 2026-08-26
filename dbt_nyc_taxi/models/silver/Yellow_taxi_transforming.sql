{{config(
    materialized='incremental',
    incremental_strategy='merge'
)}}

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
            WHEN trip_distance < 0        THEN 'invalid_distance'
            WHEN passenger_count < 0      THEN 'invalid_passenger'
            WHEN mta_tax < 0             THEN 'invalid_tax'
            WHEN improvement_surcharge < 0 THEN 'invalid_surcharge'
            WHEN fare_amount < 0 AND payment_type NOT IN (4, 6)
                THEN 'suspicious_negative_fare'
            WHEN total_amount < 0 AND payment_type NOT IN (4, 6)
                THEN 'suspicious_negative_total'
            WHEN fare_amount < 0 AND payment_type IN (4, 6)
                THEN 'valid_refund'
            ELSE 'valid'
        END AS row_quality_flag
    FROM complete_null_handling
)   

, cast_data_types_and_enrich as(
select
    VendorID,
    cast(tpep_pickup_datetime as TIMESTAMP_NTZ),
    cast(tpep_dropoff_datetime as TIMESTAMP_NTZ),
    timestampdiff(hour, tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_hour,
    timestampdiff(minute, tpep_pickup_datetime, tpep_dropoff_datetime) - timestampdiff(hour, tpep_pickup_datetime, tpep_dropoff_datetime)*60 as trip_duration_minute,
    timestampdiff(second, tpep_pickup_datetime, tpep_dropoff_datetime) - timestampdiff(minute, tpep_pickup_datetime, tpep_dropoff_datetime)*60 as trip_duration_second,
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
from handling_invalid_value
where row_quality_flag not in ('invalid_distance', 'invalid_passenger', 'invalid_tax', 'invalid_surcharge', 'invalid_pickup_and_drop_time')
)

SELECT
    *
FROM cast_data_types_and_enrich



