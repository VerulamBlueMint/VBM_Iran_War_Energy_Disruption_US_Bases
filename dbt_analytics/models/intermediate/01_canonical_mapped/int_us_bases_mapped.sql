with source as (

    select * from {{ ref('stg_us_bases_standardised_txt') }}

),

mapped as (

    select
        s.*,
        coalesce(geo.canonical_value, s.country_state) as country_state_canonical_mapped

    from source s
    left join {{ ref('map_geography') }} geo
        on geo.raw_value = s.country_state

)

select * from mapped
