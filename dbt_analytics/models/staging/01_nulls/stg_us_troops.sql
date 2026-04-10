-- models/staging/stg_us_troops.sql
--
-- Staging model for DMDC Personnel Assignment
-- Source: landing.dmdc_personnel
--
-- Cleaning applied:
--   1. Filter out 3,715 fully null ghost rows (Fabric Get Data loading artefact)
--   2. Strip commas and cast all personnel columns from varchar to int
--      (landing table stores numbers as comma-formatted varchar e.g. '4,584')
--   3. Unpivot from wide format (18 personnel columns) to long format
--      producing one row per location per branch per component_category
--   4. Null personnel counts set to zero
--   5. Standardise all column names to snake_case
--
-- Output grain: duty_state_country + branch + component_category
--
-- Branch / component_category mapping:
--   ARMY_AD                   -> Army            / Active Duty
--   NAVY_AD                   -> Navy            / Active Duty
--   MARINE_CORPS_AD           -> Marine Corps    / Active Duty
--   AIR_FORCE_AD              -> Air Force       / Active Duty
--   SPACE_FORCE_AD            -> Space Force     / Active Duty
--   COAST_GUARD_AD            -> Coast Guard     / Active Duty
--   ARMY_NATIONAL_GUARD       -> Army            / National Guard
--   ARMY_RESERVE              -> Army            / Reserve
--   NAVY_RESERVE              -> Navy            / Reserve
--   MARINE_CORPS_RESERVE      -> Marine Corps    / Reserve
--   AIR_NATIONAL_GUARD        -> Air Force       / National Guard
--   AIR_FORCE_RESERVE         -> Air Force       / Reserve
--   COAST_GUARD_RESERVE       -> Coast Guard     / Reserve
--   ARMY_DOD_CIVILIAN         -> Army            / DoD Civilian
--   NAVY_DOD_CIVILIAN         -> Navy            / DoD Civilian
--   MARINE_CORPS_DOD_CIVILIAN -> Marine Corps    / DoD Civilian
--   AIR_FORCE_DOD_CIVILIAN    -> Air Force       / DoD Civilian
--   FOURTH_ESTATE_DOD_CIVILIAN-> Fourth Estate   / DoD Civilian

WITH source AS (
    SELECT
        LOCATION                                                                    AS location,
        DUTY_STATE_COUNTRY                                                          AS duty_state_country,
        TRY_CAST(REPLACE(ARMY_AD,                   ',', '') AS INT)                AS army_ad,
        TRY_CAST(REPLACE(NAVY_AD,                   ',', '') AS INT)                AS navy_ad,
        TRY_CAST(REPLACE(MARINE_CORPS_AD,           ',', '') AS INT)                AS marine_corps_ad,
        TRY_CAST(REPLACE(AIR_FORCE_AD,              ',', '') AS INT)                AS air_force_ad,
        TRY_CAST(REPLACE(SPACE_FORCE_AD,            ',', '') AS INT)                AS space_force_ad,
        TRY_CAST(REPLACE(COAST_GUARD_AD,            ',', '') AS INT)                AS coast_guard_ad,
        TRY_CAST(REPLACE(ARMY_NATIONAL_GUARD,       ',', '') AS INT)                AS army_national_guard,
        TRY_CAST(REPLACE(ARMY_RESERVE,              ',', '') AS INT)                AS army_reserve,
        TRY_CAST(REPLACE(NAVY_RESERVE,              ',', '') AS INT)                AS navy_reserve,
        TRY_CAST(REPLACE(MARINE_CORPS_RESERVE,      ',', '') AS INT)                AS marine_corps_reserve,
        TRY_CAST(REPLACE(AIR_NATIONAL_GUARD,        ',', '') AS INT)                AS air_national_guard,
        TRY_CAST(REPLACE(AIR_FORCE_RESERVE,         ',', '') AS INT)                AS air_force_reserve,
        TRY_CAST(REPLACE(COAST_GUARD_RESERVE,       ',', '') AS INT)                AS coast_guard_reserve,
        TRY_CAST(REPLACE(ARMY_DOD_CIVILIAN,         ',', '') AS INT)                AS army_dod_civilian,
        TRY_CAST(REPLACE(NAVY_DOD_CIVILIAN,         ',', '') AS INT)                AS navy_dod_civilian,
        TRY_CAST(REPLACE(MARINE_CORPS_DOD_CIVILIAN, ',', '') AS INT)                AS marine_corps_dod_civilian,
        TRY_CAST(REPLACE(AIR_FORCE_DOD_CIVILIAN,    ',', '') AS INT)                AS air_force_dod_civilian,
        TRY_CAST(REPLACE(FOURTH_ESTATE_DOD_CIVILIAN,',', '') AS INT)                AS fourth_estate_dod_civilian
    FROM {{ source('landing', 'dmdc_personnel') }}
    WHERE DUTY_STATE_COUNTRY IS NOT NULL
      AND LOCATION           IS NOT NULL
),

unpivoted AS (
    SELECT location, duty_state_country, 'Army'          AS branch, 'Active Duty'    AS component_category, army_ad                   AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Navy'          AS branch, 'Active Duty'    AS component_category, navy_ad                   AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Marine Corps'  AS branch, 'Active Duty'    AS component_category, marine_corps_ad           AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Air Force'     AS branch, 'Active Duty'    AS component_category, air_force_ad              AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Space Force'   AS branch, 'Active Duty'    AS component_category, space_force_ad            AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Coast Guard'   AS branch, 'Active Duty'    AS component_category, coast_guard_ad            AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Army'          AS branch, 'National Guard' AS component_category, army_national_guard       AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Army'          AS branch, 'Reserve'        AS component_category, army_reserve              AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Navy'          AS branch, 'Reserve'        AS component_category, navy_reserve              AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Marine Corps'  AS branch, 'Reserve'        AS component_category, marine_corps_reserve      AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Air Force'     AS branch, 'National Guard' AS component_category, air_national_guard        AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Air Force'     AS branch, 'Reserve'        AS component_category, air_force_reserve         AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Coast Guard'   AS branch, 'Reserve'        AS component_category, coast_guard_reserve       AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Army'          AS branch, 'DoD Civilian'   AS component_category, army_dod_civilian         AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Navy'          AS branch, 'DoD Civilian'   AS component_category, navy_dod_civilian         AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Marine Corps'  AS branch, 'DoD Civilian'   AS component_category, marine_corps_dod_civilian AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Air Force'     AS branch, 'DoD Civilian'   AS component_category, air_force_dod_civilian    AS personnel_count FROM source
    UNION ALL
    SELECT location, duty_state_country, 'Fourth Estate' AS branch, 'DoD Civilian'   AS component_category, fourth_estate_dod_civilian AS personnel_count FROM source
)

SELECT
    location,
    duty_state_country,
    branch,
    component_category,
    COALESCE(personnel_count, 0) AS personnel_count
FROM unpivoted
