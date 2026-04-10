{{ config(
    materialized = 'table',
    schema = 'mart'
) }}

with military_base_prv as (

    select
        s.site                                  as name,
        s.latitude,
        s.longitude,
        g.region,
        sum(f.plant_replacement_value_m)        as total_prv
    from {{ ref('dim_site') }} s
    inner join {{ ref('fact_base_inventory') }} f
        on f.site_key = s.site_key
    inner join {{ ref('dim_geography') }} g
        on g.geo_key = s.geo_key
    group by
        s.site,
        s.latitude,
        s.longitude,
        g.region

),

military_bases as (

    select
        'Military Base'                         as category,
        name,
        latitude,
        longitude,
        region,
        case
            when max(total_prv) over () = min(total_prv) over () then 50
            else greatest(
                (total_prv - min(total_prv) over ())
                / nullif(max(total_prv) over () - min(total_prv) over (), 0)
                * 95 + 5,
            5)
        end                                     as size_value
    from military_base_prv

),

energy_field_ranked as (

    select
        field_key,
        quantity_standardised,
        row_number() over (
            partition by field_key
            order by quantity_standardised desc
        )                                       as rn
    from {{ ref('fact_energy_production') }}
    where fuel_description_canonical_mapped in ('oil', 'gas')

),

energy_field_dominant as (

    select
        field_key,
        quantity_standardised                   as dominant_production
    from energy_field_ranked
    where rn = 1

),

energy_field_joined as (

    select
        d.unit_name                             as name,
        d.latitude,
        d.longitude,
        d.region,
        coalesce(p.dominant_production, 0)      as dominant_production
    from {{ ref('dim_energy_field') }} d
    left join energy_field_dominant p
        on p.field_key = d.field_key

),

energy_fields as (

    select
        'Energy Field'                          as category,
        name,
        latitude,
        longitude,
        region,
        case
            when max(dominant_production) over () = min(dominant_production) over () then 50
            else greatest(
                (dominant_production - min(dominant_production) over ())
                / nullif(max(dominant_production) over () - min(dominant_production) over (), 0)
                * 95 + 5,
            5)
        end                                     as size_value
    from energy_field_joined

),

troop_location_counts as (

    select
        g.country_or_state                      as name,
        g.latitude,
        g.longitude,
        g.region,
        sum(f.personnel_count)                  as total_personnel
    from {{ ref('dim_geography') }} g
    inner join {{ ref('fact_personnel_assignment') }} f
        on f.geo_key = g.geo_key
    where f.location = 'overseas'
      and f.snapshot_date = '2025-12-31'
    group by
        g.country_or_state,
        g.latitude,
        g.longitude,
        g.region

),

troop_locations as (

    select
        'Troop Location'                        as category,
        name,
        latitude,
        longitude,
        region,
        case
            when max(total_personnel) over () = min(total_personnel) over () then 50
            else greatest(
                (total_personnel - min(total_personnel) over ())
                / nullif(max(total_personnel) over () - min(total_personnel) over (), 0)
                * 95 + 5,
            5)
        end                                     as size_value
    from troop_location_counts

),

final as (

    select * from military_bases
    union all
    select * from energy_fields
    union all
    select * from troop_locations

)

select * from final