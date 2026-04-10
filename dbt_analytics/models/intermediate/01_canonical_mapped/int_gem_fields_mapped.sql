with source as (

    select * from {{ ref('stg_gem_main_standardised_txt') }}

),

mapped as (

    select
        s.*,
        coalesce(geo.canonical_value, s.country_area) as country_area_canonical_mapped

    from source s
    left join {{ ref('map_geography') }} geo
        on geo.raw_value = s.country_area

)

select * from mapped
