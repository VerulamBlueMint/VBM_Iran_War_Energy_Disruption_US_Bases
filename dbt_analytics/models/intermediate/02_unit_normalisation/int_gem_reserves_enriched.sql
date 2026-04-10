with source as (

    select * from {{ ref('int_gem_reserves_mapped') }}

),

enriched as (

    select
        s.*,
        case
            when units_converted_canonical_mapped = 'million_bbl'
                then 'bbl'
            when units_converted_canonical_mapped = 'million_cubic_meters'
                then 'MWh'
            when units_converted_canonical_mapped = 'million_boe'
                then 'boe'
        end as units_standardised,
        case
            when units_converted_canonical_mapped = 'million_bbl'
                then cast(quantity_converted as decimal(18,6)) * 1000000
            when units_converted_canonical_mapped = 'million_cubic_meters'
                then cast(quantity_converted as decimal(18,6)) * 1000000 * 0.01056
            when units_converted_canonical_mapped = 'million_boe'
                then cast(quantity_converted as decimal(18,6))
        end as quantity_standardised

    from source s

)

select * from enriched