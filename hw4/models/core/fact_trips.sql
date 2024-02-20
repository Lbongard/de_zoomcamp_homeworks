{{config(materialized='table')}}

with green_tripdata as(
    select *,
            'Green' as service_type
    from {{ref('stg_green_cab_data')}}
),

yellow_tripdata as(
    select *,
            'Yellow' as service_type
    from {{ref('stg_yellow_cab_data')}}
),

trips_unioned as(

select *
from green_tripdata 

UNION ALL

SELECT *
from yellow_tripdata
),

dim_zones as(
    select *
    from {{ref('dim_zones')}}
    where borough != 'Unknown'
)

select t.*, 
        pickup_loc.borough pickup_borough,
        pickup_loc.zone pickup_zone,
        pickup_loc.service_zone pickup_service_zone , 
        dropoff_loc.borough dropoff_borough,
        dropoff_loc.zone dropoff_zone,
        dropoff_loc.service_zone dropoff_service_zone 
from trips_unioned t 
     inner join dim_zones pickup_loc
     on t.pickup_locationid = pickup_loc.locationid
     inner join dim_zones dropoff_loc
     on t.dropoff_locationid = dropoff_loc.locationid