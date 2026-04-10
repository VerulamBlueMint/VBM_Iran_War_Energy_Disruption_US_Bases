-- models/mart/energy/fact_energy_reserves.sql

{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

with source as (

    select * from {{ ref('int_gem_reserves_region_mapped') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['unit_id']) }}                                      as field_key,
        {{ dbt_utils.generate_surrogate_key(['fuel_description_canonical_mapped']) }}             as fuel_key,
        {{ dbt_utils.generate_surrogate_key(['reserves_classification_canonical_mapped']) }}      as classification_key,
        unit_id,
        unit_name,
        country_area_canonical_mapped,
        region,
        fuel_description_canonical_mapped,
        units_converted_canonical_mapped,
        reserves_classification_canonical_mapped,
        quantity,
        units,
        units_standardised,
        quantity_standardised,
        data_year
    from source

)

select * from final