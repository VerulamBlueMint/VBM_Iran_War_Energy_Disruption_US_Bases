with source as (

    select * from {{ ref('stg_us_troops') }}

)

select

    -- text columns: lower + trim + nullif empty string
    nullif(lower(ltrim(rtrim(cast(location           as varchar)))), '') as location,
    nullif(lower(ltrim(rtrim(cast(duty_state_country as varchar)))), '') as duty_state_country,
    nullif(lower(ltrim(rtrim(cast(branch             as varchar)))), '') as branch,
    nullif(lower(ltrim(rtrim(cast(component_category as varchar)))), '') as component_category,

    -- numeric columns: pass through unchanged
    personnel_count

from source
