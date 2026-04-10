-- models/mart/energy/fact_energy_production.sql

{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

with source as (

    select * from {{ ref('int_gem_production_region_mapped') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['unit_id']) }}                          as field_key,
        {{ dbt_utils.generate_surrogate_key(['fuel_description_canonical_mapped']) }} as fuel_key,
        unit_id,
        unit_name,
        country_area_canonical_mapped,
        region,
        fuel_description_canonical_mapped,
        units_converted_canonical_mapped,
        quantity_original,
        units_original,
        units_standardised,
        quantity_standardised,
        data_year
    from source

)

select * from final