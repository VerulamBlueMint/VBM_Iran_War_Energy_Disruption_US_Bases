with source as (

    select * from {{ ref('stg_gem_production_standardised_txt') }}

),

mapped as (

    select
        s.*,
        coalesce(geo.canonical_value,   s.country_area)    as country_area_canonical_mapped,
        coalesce(fuel.canonical_value,  s.fuel_description) as fuel_description_canonical_mapped,
        coalesce(units.canonical_value, s.units_converted)  as units_converted_canonical_mapped

    from source s
    left join {{ ref('map_geography') }} geo
        on geo.raw_value = s.country_area
    left join {{ ref('map_fuel_description') }} fuel
        on fuel.raw_value = s.fuel_description
    left join {{ ref('map_fuel_description') }} units
        on units.raw_value = s.units_converted

)

select * from mapped
