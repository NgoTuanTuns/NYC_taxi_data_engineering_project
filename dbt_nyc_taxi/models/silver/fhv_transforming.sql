{{
  config(
    materialized = 'incremental',
    incremental_strategy='merge'
    )
}}
WITH source AS(
SELECT * FROM {{ ref('fhv_trip') }}
{% if is_incremental() %}
  WHERE ingestion_at > (SELECT max(ingestion_at) FROM {{this}})
{% endif %}
),

null_pu_do_handling AS(
    SELECT *,
    CASE
      WHEN PULocationID IS NULL AND DOLocationID IS NULL THEN 'unknown_pickup_dropoff_zone'
      WHEN PULocationID IS NULL THEN 'unknown_pickup_zone'
      WHEN DOLocationID IS NULL THEN 'unknown_dropoff_zone'
    ELSE 'Complete_pu_do_zone'
    END AS location_flag
   FROM source
),

affiliated_and_dispatch_handling AS (
  SELECT
    *,
    CASE
        WHEN dispatching_base_num IS NULL 
            THEN 'invalid_no_dispatcher'       
        WHEN Affiliated_base_number IS NULL 
            THEN 'non_affiliated_or_independent'
        WHEN Affiliated_base_number = dispatching_base_num 
            THEN 'self_dispatched'  
        ELSE 'cross_base_dispatch'
    END AS dispatch_type_flag
 FROM null_pu_do_handling
),

handling_invalid_value AS (
  SELECT *,
  CASE
    WHEN timestampdiff(second, pickup_datetime, dropOff_datetime) <= 0 THEN 'invalid_pu_do_time'
    WHEN PUlocationID < 0 THEN 'invalid_pu_location'
    WHEN DOlocationID < 0 THEN 'invalid_do_location'
  ELSE 'valid_row'
  END AS row_quality
 FROM affiliated_and_dispatch_handling
),


deduplication AS(
  SELECT * FROM(
    SELECT *,
    row_number() over(partition by
      dispatching_base_num,
      pickup_datetime,
      dropOff_datetime,
      PUlocationID,
      DOlocationID,
      SR_Flag, 
      Affiliated_base_number
    ORDER BY pickup_datetime) AS row_num
   FROM handling_invalid_value
  )t
  WHERE row_num = 1
),


cast_and_enrich_data AS (
  SELECT
      dispatching_base_num,
      cast(pickup_datetime AS TIMESTAMP_NTZ),
      cast(dropoff_datetime AS TIMESTAMP_NTZ),
      LPAD(cast(timestampdiff(HOUR, pickup_datetime, dropOff_datetime) AS BIGINT), 2, '0') || ":" ||
      LPAD(cast(timestampdiff(MINUTE, pickup_datetime, dropOff_datetime) - timestampdiff(HOUR, pickup_datetime, dropOff_datetime)*60 AS BIGINT), 2, '0')   || ":" ||
      LPAD(cast(timestampdiff(SECOND, pickup_datetime, dropOff_datetime) - timestampdiff(MINUTE, pickup_datetime, dropOff_datetime)*60 AS BIGINT), 2, '0') AS trip_duration,
      PUlocationID,
      DOlocationID,
      SR_Flag, 
      Affiliated_base_number,
      ingestion_at,
      location_flag,
      dispatch_type_flag
  FROM deduplication
  WHERE row_quality LIKE 'valid_row'
  AND dispatch_type_flag NOT LIKE 'invalid_no_dispatcher'
)

SELECT
*
FROM cast_and_enrich_data



