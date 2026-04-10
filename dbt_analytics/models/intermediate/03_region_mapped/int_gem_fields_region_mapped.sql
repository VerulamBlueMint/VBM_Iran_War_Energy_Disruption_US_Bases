with source as (

    select * from {{ ref('stg_gem_main_standardised_txt') }}

),

mapped as (

    select
        s.*,
        coalesce(geo.canonical_value, s.country_area) as country_area_canonical_mapped,
        reg.region

    from source s
    left join {{ ref('map_geography') }} geo
        on geo.raw_value = s.country_area
    left join {{ ref('map_region') }} reg
        on reg.canonical_value = coalesce(geo.canonical_value, s.country_area)

)

select * from mapped
