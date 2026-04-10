with source as (

    select * from {{ ref('stg_us_troops_standardised_txt') }}

),

mapped as (

    select
        s.*,
        coalesce(geo.canonical_value, s.duty_state_country) as duty_state_country_canonical_mapped,
        case
            when s.location = 'overseas'
                and coalesce(geo.canonical_value, s.duty_state_country) = 'georgia'
                then 'europe'
            else reg.region
        end as region

    from source s
    left join {{ ref('map_geography') }} geo
        on geo.raw_value = s.duty_state_country
    left join {{ ref('map_region') }} reg
        on reg.canonical_value = coalesce(geo.canonical_value, s.duty_state_country)

)

select * from mapped
