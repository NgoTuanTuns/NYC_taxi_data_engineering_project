SELECT
*
FROM {{ source('source', 'fhv_trip') }}