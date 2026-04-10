with source as (

    select * from {{ ref('int_gem_production_enriched') }}

),

mapped as (

    select
        s.*,
        reg.region

    from source s
    left join {{ ref('map_region') }} reg
        on reg.canonical_value = s.country_area_canonical_mapped

)

select * from mapped
