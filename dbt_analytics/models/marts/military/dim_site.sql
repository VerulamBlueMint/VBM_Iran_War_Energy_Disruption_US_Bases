-- models/mart/military/dim_site.sql

{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

with source as (

    select * from {{ ref('int_us_bases_region_mapped') }}

),

deduplicated as (

    select
        site,
        component,
        country_state_canonical_mapped,
        region,
        min(name_nearest_city) as name_nearest_city,
        min(latitude)          as latitude,
        min(longitude)         as longitude,
        min(lat_long_sources)  as lat_long_sources
    from source
    group by
        site,
        component,
        country_state_canonical_mapped,
        region

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['site', 'component', 'country_state_canonical_mapped']) }} as site_key,
        {{ dbt_utils.generate_surrogate_key(['country_state_canonical_mapped', 'region']) }}            as geo_key,
        site,
        component,
        name_nearest_city,
        latitude,
        case
            when site in (
                'batt west property site 2',
                'riverine range',
                'naval hospital',
                'isabela transmitter site',
                'navsuppfac beaufort sc'
            ) and longitude > 0 then longitude * -1
            else longitude
        end as longitude,
        lat_long_sources
    from deduplicated

)

select * from final