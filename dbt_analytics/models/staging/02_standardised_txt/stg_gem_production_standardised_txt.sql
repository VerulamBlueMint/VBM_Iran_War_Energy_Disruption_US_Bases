with source as (

    select * from {{ ref('stg_gem_production') }}

)

select

    -- text columns: lower + trim + nullif empty string
    nullif(lower(ltrim(rtrim(cast(unit_id          as varchar)))), '') as unit_id,
    nullif(lower(ltrim(rtrim(cast(unit_name        as varchar)))), '') as unit_name,
    nullif(lower(ltrim(rtrim(cast(country_area     as varchar)))), '') as country_area,
    nullif(lower(ltrim(rtrim(cast(fuel_description as varchar)))), '') as fuel_description,
    nullif(lower(ltrim(rtrim(cast(units_original   as varchar)))), '') as units_original,
    nullif(lower(ltrim(rtrim(cast(units_converted  as varchar)))), '') as units_converted,

    -- numeric columns: pass through unchanged
    quantity_original,
    quantity_converted,
    data_year

from source
