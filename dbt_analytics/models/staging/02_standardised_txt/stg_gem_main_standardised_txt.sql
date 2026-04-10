with source as (

    select * from {{ ref('stg_gem_main') }}

)

select

    -- text columns: lower + trim + nullif empty string
    nullif(lower(ltrim(rtrim(cast(unit_id                as varchar)))), '') as unit_id,
    nullif(lower(ltrim(rtrim(cast(unit_name              as varchar)))), '') as unit_name,
    nullif(lower(ltrim(rtrim(cast(unit_name_local_script as varchar)))), '') as unit_name_local_script,
    nullif(lower(ltrim(rtrim(cast(fuel_type              as varchar)))), '') as fuel_type,
    nullif(lower(ltrim(rtrim(cast(country_area           as varchar)))), '') as country_area,
    nullif(lower(ltrim(rtrim(cast(subnational_unit       as varchar)))), '') as subnational_unit,
    nullif(lower(ltrim(rtrim(cast(production_type        as varchar)))), '') as production_type,
    nullif(lower(ltrim(rtrim(cast(status                 as varchar)))), '') as status,
    nullif(lower(ltrim(rtrim(cast(status_detail          as varchar)))), '') as status_detail,
    nullif(lower(ltrim(rtrim(cast(operator               as varchar)))), '') as operator,
    nullif(lower(ltrim(rtrim(cast(owners                 as varchar)))), '') as owners,
    nullif(lower(ltrim(rtrim(cast(parents                as varchar)))), '') as parents,
    nullif(lower(ltrim(rtrim(cast(government_unit_id     as varchar)))), '') as government_unit_id,
    nullif(lower(ltrim(rtrim(cast(wiki_url_project       as varchar)))), '') as wiki_url_project,
    nullif(lower(ltrim(rtrim(cast(wiki_url_field         as varchar)))), '') as wiki_url_field,
    nullif(lower(ltrim(rtrim(cast(name_other             as varchar)))), '') as name_other,
    nullif(lower(ltrim(rtrim(cast(location_accuracy      as varchar)))), '') as location_accuracy,
    nullif(lower(ltrim(rtrim(cast(onshore_offshore       as varchar)))), '') as onshore_offshore,
    nullif(lower(ltrim(rtrim(cast(field_outline_wkt      as varchar)))), '') as field_outline_wkt,
    nullif(lower(ltrim(rtrim(cast(basin                  as varchar)))), '') as basin,
    nullif(lower(ltrim(rtrim(cast(blocks                 as varchar)))), '') as blocks,

    -- numeric columns: pass through unchanged
    status_year,
    discovery_year,
    fid_year,
    production_start_year,
    latitude,
    longitude

from source
