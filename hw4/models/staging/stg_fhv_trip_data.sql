{{
    config(
        materialized='view'
    )
}}

-- with tripdata as 
-- (
--   select *,
--     row_number() over(partition by vendorid, lpep_pickup_datetime) as rn
--   from {{ source('staging','green_cab_data') }}
--   where vendorid is not null 
-- )
select
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- identifiers
    {{ dbt.safe_cast("PUlocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    SR_Flag,
    Affiliated_base_number

from {{ source('staging','fhv_cab_data') }}
where extract(year from cast(pickup_datetime as timestamp)) = 2019


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}