with source as (

    select * from {{ ref('stg_us_bases') }}

)

select

    -- text columns: lower + trim + nullif empty string
    nullif(lower(ltrim(rtrim(cast(country_state      as varchar)))), '') as country_state,
    nullif(lower(ltrim(rtrim(cast(site               as varchar)))), '') as site,
    nullif(lower(ltrim(rtrim(cast(component          as varchar)))), '') as component,
    nullif(lower(ltrim(rtrim(cast(name_nearest_city  as varchar)))), '') as name_nearest_city,
    nullif(lower(ltrim(rtrim(cast(lat_long_sources   as varchar)))), '') as lat_long_sources,

    -- numeric columns: pass through unchanged
    count_building_owned,
    building_owned_sqft,
    count_bldgs_leased,
    bldgs_leased_sqft,
    count_bldgs_other,
    bldgs_other_sqft,
    acres_owned,
    total_acres,
    plant_replacement_value_m,
    latitude,
    longitude

from source
