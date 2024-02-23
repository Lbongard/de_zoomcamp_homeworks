{{ config(materialized='view') }}

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num']) }} as tripid,

    dispatching_base_num, 
    cast(pickup_datetime as timestamp) as pickup_datetime, 
    cast(dropOff_datetime as timestamp) as dropoff_datetime, 
    cast(PUlocationID as integer) as  pickup_locationid, 
    cast(DOlocationID as integer) as dropoff_locationid, 
    cast(SR_Flag as integer) SR_Flag, 
    Affiliated_base_number, 
from {{ source('staging','fhv_cab_data') }}
--where rn = 1


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
