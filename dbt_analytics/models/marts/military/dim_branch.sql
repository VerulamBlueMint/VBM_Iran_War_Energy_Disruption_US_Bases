-- models/mart/military/dim_branch.sql

{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

with source as (

    select distinct
        branch,
        component_category
    from {{ ref('int_us_troops_region_mapped') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['branch', 'component_category']) }} as branch_key,
        branch,
        component_category
    from source

)

select * from final
