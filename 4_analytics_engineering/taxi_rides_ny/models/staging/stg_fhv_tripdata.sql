{{ config( materialized='view') }}

select
    --identifiers
    {{ dbt.safe_cast('dispatching_base_num', api.Column.translate_type("string")) }} as dispatching_base_num,
    {{ dbt.safe_cast('pulocation_id', api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast('dolocation_id', api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast('affiliated_base_number', api.Column.translate_type("string")) }} as affiliated_base_num,

    --timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(drop_off_datetime as timestamp) as dropoff_datetime,

    --trip info
    {{ dbt.safe_cast('sr__flag', api.Column.translate_type("string")) }} as sr_flag

from {{ source("staging","fhv_tripdata") }}
where EXTRACT(YEAR FROM pickup_datetime) = 2019

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}



