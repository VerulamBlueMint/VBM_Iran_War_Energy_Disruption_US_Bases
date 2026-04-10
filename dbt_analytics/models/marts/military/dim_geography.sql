-- models/mart/military/dim_geography.sql

{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

with bases as (

    select
        country_state_canonical_mapped as country_or_state,
        region
    from {{ ref('int_us_bases_region_mapped') }}

),

troops as (

    select
        duty_state_country_canonical_mapped as country_or_state,
        region
    from {{ ref('int_us_troops_region_mapped') }}

),

unioned as (

    select country_or_state, region from bases
    union
    select country_or_state, region from troops

),

centroids as (

    select
        canonical_value,
        latitude,
        longitude
    from {{ ref('map_country_centroids') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['country_or_state', 'region']) }} as geo_key,
        country_or_state,
        region,
        cast(case when region = 'us domestic' then 1 else 0 end as bit) as is_domestic,
        c.latitude,
        c.longitude
    from unioned u
    left join centroids c on c.canonical_value = u.country_or_state

)

select * from final