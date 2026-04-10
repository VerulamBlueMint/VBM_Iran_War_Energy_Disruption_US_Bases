-- models/mart/military/fact_base_inventory.sql

{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

with source as (

    select * from {{ ref('int_us_bases_region_mapped') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['site', 'component', 'country_state_canonical_mapped']) }} as site_key,
        {{ dbt_utils.generate_surrogate_key(['country_state_canonical_mapped', 'region']) }}            as geo_key,
        cast('2024-09-30' as date)                                                                      as snapshot_date,
        count_building_owned,
        building_owned_sqft,
        count_bldgs_leased,
        bldgs_leased_sqft,
        count_bldgs_other,
        bldgs_other_sqft,
        acres_owned,
        total_acres,
        plant_replacement_value_m
    from source

)

select * from final
