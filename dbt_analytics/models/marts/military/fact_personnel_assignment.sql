-- models/mart/military/fact_personnel_assignment.sql

{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

with source as (

    select * from {{ ref('int_us_troops_region_mapped') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['duty_state_country_canonical_mapped', 'region']) }} as geo_key,
        {{ dbt_utils.generate_surrogate_key(['branch', 'component_category']) }}                  as branch_key,
        location,
        cast('2025-12-31' as date)                                                                as snapshot_date,
        personnel_count
    from source

)

select * from final
