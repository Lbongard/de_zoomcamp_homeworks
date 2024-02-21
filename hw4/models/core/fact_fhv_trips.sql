{{config(materialized='table')}}

with fhv_tripdata as(
    select *,
            'Green' as service_type
    from {{ref('stg_fhv_trip_data')}}
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
from fhv_tripdata t 
     inner join dim_zones pickup_loc
     on t.pickup_locationid = pickup_loc.locationid
     inner join dim_zones dropoff_loc
     on t.dropoff_locationid = dropoff_loc.locationid