{{ config(materialized='table') }}

with fhv_tripdata as (
    SELECT *,
        'Fhv' as service_type
    FROM {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    SELECT *
    FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT 
    fhv_tripdata.dispatching_base_num, 
    fhv_tripdata.affiliated_base_num, 
    fhv_tripdata.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,
    fhv_tripdata.pickup_datetime, 
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.service_type,
    fhv_tripdata.sr_flag 
FROM fhv_tripdata
INNER JOIN dim_zones as pickup_zone
    ON fhv_tripdata.pickup_locationid = pickup_zone.locationid
INNER JOIN dim_zones as dropoff_zone
    ON fhv_tripdata.dropoff_locationid = dropoff_zone.locationid