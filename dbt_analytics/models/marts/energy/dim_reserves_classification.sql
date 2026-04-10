-- models/mart/energy/dim_reserves_classification.sql

{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

with source as (

    select distinct reserves_classification_canonical_mapped as reserves_classification
    from {{ ref('int_gem_reserves_region_mapped') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['reserves_classification']) }} as classification_key,
        reserves_classification
    from source

)

select * from final
