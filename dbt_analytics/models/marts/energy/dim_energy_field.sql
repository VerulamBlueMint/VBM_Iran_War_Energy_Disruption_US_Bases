-- models/mart/energy/dim_energy_field.sql

{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

with source as (

    select * from {{ ref('int_gem_fields_region_mapped') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['unit_id']) }} as field_key,
        unit_id,
        unit_name,
        unit_name_local_script,
        country_area_canonical_mapped,
        region,
        subnational_unit,
        production_type,
        status,
        status_detail,
        status_year,
        discovery_year,
        fid_year,
        production_start_year,
        operator,
        onshore_offshore,
        latitude,
        longitude,
        location_accuracy,
        basin
    from source

)

select * from final
