with source as (

    select * from {{ ref('int_gem_production_mapped') }}

),

enriched as (

    select
        s.*,
        case
            when units_converted_canonical_mapped = 'million_bbl_per_year'
                then 'bbl/day'
            when units_converted_canonical_mapped = 'million_cubic_meters_per_year'
                then 'MWh/day'
            when units_converted_canonical_mapped = 'million_boe_per_year'
                then 'boe/y'
        end as units_standardised,
        case
            when units_converted_canonical_mapped = 'million_bbl_per_year'
                then cast(quantity_converted as decimal(18,6)) * 1000000 / 365
            when units_converted_canonical_mapped = 'million_cubic_meters_per_year'
                then cast(quantity_converted as decimal(18,6)) * 1000000 * 0.01056 / 365
            when units_converted_canonical_mapped = 'million_boe_per_year'
                then cast(quantity_converted as decimal(18,6))
        end as quantity_standardised

    from source s

)

select * from enriched