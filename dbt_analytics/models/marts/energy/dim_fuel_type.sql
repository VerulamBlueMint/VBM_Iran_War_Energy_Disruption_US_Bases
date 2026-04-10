-- models/mart/energy/dim_fuel_type.sql

{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

with production as (

    select distinct fuel_description_canonical_mapped as fuel_description
    from {{ ref('int_gem_production_region_mapped') }}

),

reserves as (

    select distinct fuel_description_canonical_mapped as fuel_description
    from {{ ref('int_gem_reserves_region_mapped') }}

),

unioned as (

    select fuel_description from production
    union
    select fuel_description from reserves

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['fuel_description']) }} as fuel_key,
        fuel_description
    from unioned

)

select * from final
