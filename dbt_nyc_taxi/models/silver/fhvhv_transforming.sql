{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'megre'
    )
}}
with source as(
    SELECT *
    FROM {{ ref('fhvhv_trip') }}
    {% if is_incremental() %}
    WHERE ingestion_at > coalesce((select max(ingestion_at) from {{ this }}), '1900-01-01')
    {% endif %}
),

null_handling as (
    SELECT 
    hvfhs_license_num,
    dispatching_base_num,
    originating_base_num,
    request_datetime,
    on_scene_datetime,
    pickup_datetime,
    dropoff_datetime,
    PULocationID,
    DOLocationID,
    trip_miles,
    trip_time,
    base_passenger_fare,
    bcf,
    sales_tax,
    driver_pay,
    ingestion_at,
    COALESCE(airport_fee, 0) AS airport_fee,
    COALESCE(congestion_surcharge, 0) AS congestion_surcharge,
    COALESCE(cbd_congestion_fee, 0) AS cbd_congestion_fee,
    COALESCE(tips, 0) AS tips,
    COALESCE(tolls, 0) AS tolls,
    COALESCE(shared_request_flag, 'N') AS shared_request_flag,
    COALESCE(shared_match_flag, 'N') AS shared_match_flag,
    COALESCE(access_a_ride_flag, 'N') AS access_a_ride_flag,
    COALESCE(wav_request_flag, 'N') AS wav_request_flag,
    COALESCE(wav_match_flag, 'N') AS wav_match_flag,
    CASE
        WHEN originating_base_num IS NULL AND dispatching_base_num IS NOT NULL
            THEN 'direct_dispatch'
        WHEN originating_base_num = dispatching_base_num
            THEN 'same_base_dispatch'
        WHEN originating_base_num != dispatching_base_num
            THEN 'cross_base_dispatch'
        ELSE 'unknown'
    END AS dispatch_routing_flag
    FROM source
),

invalid_handling as (
    SELECT
        *,
        CASE
            WHEN hvfhs_license_num IS NULL THEN 'invalid_no_platform'
            WHEN dispatching_base_num IS NULL THEN 'invalid_no_dispatcher'
            WHEN pickup_datetime IS NULL THEN 'invalid_no_pickup_time'
            WHEN dropoff_datetime IS NULL THEN 'invalid_no_dropoff_time'
            WHEN PULocationID IS NULL THEN 'invalid_no_pickup_location'
            WHEN DOLocationID IS NULL THEN 'invalid_no_dropoff_location'
            WHEN trip_miles IS NULL
              OR trip_miles <= 0 THEN 'invalid_trip_miles'
            WHEN trip_time IS NULL
              OR trip_time <= 0 THEN 'invalid_trip_time'
            WHEN base_passenger_fare IS NULL
              OR base_passenger_fare < 0 THEN 'invalid_fare'
            WHEN driver_pay IS NULL
              OR driver_pay < 0 THEN 'invalid_driver_pay'
            WHEN timestampdiff(second, pickup_datetime, dropoff_datetime) <= 0
                THEN 'invalid_duration'
            ELSE 'valid'
        END AS row_quality_flag
    FROM null_handling
),

deduplication as (
    SELECT *
    From (
        select *, 
        row_number() 
            over(partition by 
                    hvfhs_license_num,
                    dispatching_base_num,
                    originating_base_num,
                    request_datetime,
                    on_scene_datetime,
                    pickup_datetime,
                    dropoff_datetime,
                    PULocationID,
                    DOLocationID,
                    trip_miles,
                    trip_time
                order by pickup_datetime
            ) as row_number
        from invalid_handling
        where row_quality_flag like 'valid'
    ) t
    where row_number == 1
),

cast_and_enrich_data as (
    select 
        hvfhs_license_num,
        dispatching_base_num,
        originating_base_num,
        CAST(request_datetime  AS TIMESTAMP_NTZ) AS request_datetime,
        CAST(on_scene_datetime AS TIMESTAMP_NTZ) AS on_scene_datetime,
        CAST(pickup_datetime   AS TIMESTAMP_NTZ) AS pickup_datetime,
        CAST(dropoff_datetime  AS TIMESTAMP_NTZ) AS dropoff_datetime,
        LPAD(CAST(trip_time / 3600 AS BIGINT), 2, '0') || ':' ||
        LPAD(CAST((trip_time % 3600) / 60 AS BIGINT), 2, '0') || ':' ||
        LPAD(CAST(trip_time % 60 AS BIGINT), 2, '0') trip_duration,
        PULocationID,
        DOLocationID,
        trip_miles,
        base_passenger_fare,
        tolls,
        bcf,
        sales_tax,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee,
        tips,
        driver_pay,
        ROUND(base_passenger_fare - driver_pay - tips, 2) AS platform_fee,
        shared_request_flag,
        shared_match_flag,
        access_a_ride_flag,
        wav_request_flag,
        wav_match_flag,
        dispatch_routing_flag,
        row_quality_flag,
        ingestion_at
    FROM deduplication
)

SELECT {{dbt_utils.generate_surrogate_key([
    "hvfhs_license_num",
    "dispatching_base_num",
    "originating_base_num",
    "request_datetime",
    "on_scene_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "trip_duration"
])}} as fhvhv_trip_id,
*
FROM cast_and_enrich_data



