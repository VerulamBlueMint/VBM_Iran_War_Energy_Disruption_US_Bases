-- =========================================
-- DOD STRATEGIC INTELLIGENCE DASHBOARD
-- DATA PROFILING QUERIES
-- analyses/profiling.sql
-- =========================================
--
-- Run method: Copy individual queries and run
-- directly against Fabric Warehouse via the
-- SQL endpoint. Do not materialise as a model.
--
-- IMPORTANT - LEADING SPACES IN BSR COLUMNS:
-- The following BSR columns were loaded via
-- Fabric Get Data with a leading space in the
-- column name. They must be referenced with
-- bracket notation throughout:
--   [ Count_Building_Owned]
--   [ Building_Owned_SqFt]
--   [ Count_Bldgs_Leased]
--   [ Count_Bldgs_Other]
--   [ Bldgs_Other_SqFt]
--   [ Acres_Owned]
--   [ Total_Acres]
-- This will be resolved in the staging model
-- by aliasing to clean column names.
-- Bldgs_Leased_SqFt has no leading space.
--
-- Source references (dbt syntax):
--   {{ source('landing', 'base_structure_report') }}
--     → dbt_dev_landing.Base_Structure_Report_FY25_with_lat_lon_converted
--   {{ source('landing', 'dmdc_personnel') }}
--     → dbt_dev_landing.DMDC_Website_Location_Report_2512_converted
--   {{ source('landing', 'gem_main') }}
--     → dbt_dev_landing.Global_Oil_and_Gas_Extraction_Tracker_March_2026_field_level_main_converted
--   {{ source('landing', 'gem_production') }}
--     → dbt_dev_landing.Global_Oil_and_Gas_Extraction_Tracker_March_2026_field_level_production_converted
--   {{ source('landing', 'gem_reserves') }}
--     → dbt_dev_landing.Global_Oil_and_Gas_Extraction_Tracker_March_2026_field_level_reserves_converted
-- =========================================


-- =========================================
-- A. BASE STRUCTURE REPORT (BSR)
-- Source: landing.base_structure_report
-- Declared grain: one row per site per component
-- =========================================

-- A1. Shape and volume
SELECT
    COUNT(*)                        AS total_rows,
    COUNT(DISTINCT Site)            AS distinct_sites,
    COUNT(DISTINCT Country_State)   AS distinct_country_states,
    COUNT(DISTINCT Component)       AS distinct_components
FROM {{ source('landing', 'base_structure_report') }};


-- A2. Null / blank counts on all columns
SELECT
    SUM(CASE WHEN Country_State              IS NULL OR TRIM(Country_State)     = '' THEN 1 ELSE 0 END) AS null_country_state,
    SUM(CASE WHEN Site                       IS NULL OR TRIM(Site)              = '' THEN 1 ELSE 0 END) AS null_site,
    SUM(CASE WHEN Component                  IS NULL OR TRIM(Component)         = '' THEN 1 ELSE 0 END) AS null_component,
    SUM(CASE WHEN Name_Nearest_City          IS NULL OR TRIM(Name_Nearest_City) = '' THEN 1 ELSE 0 END) AS null_nearest_city,
    SUM(CASE WHEN [ Count_Building_Owned]    IS NULL                                  THEN 1 ELSE 0 END) AS null_count_building_owned,
    SUM(CASE WHEN [ Building_Owned_SqFt]     IS NULL                                  THEN 1 ELSE 0 END) AS null_building_owned_sqft,
    SUM(CASE WHEN [ Count_Bldgs_Leased]      IS NULL                                  THEN 1 ELSE 0 END) AS null_count_bldgs_leased,
    SUM(CASE WHEN Bldgs_Leased_SqFt          IS NULL                                  THEN 1 ELSE 0 END) AS null_bldgs_leased_sqft,
    SUM(CASE WHEN [ Count_Bldgs_Other]       IS NULL                                  THEN 1 ELSE 0 END) AS null_count_bldgs_other,
    SUM(CASE WHEN [ Bldgs_Other_SqFt]        IS NULL                                  THEN 1 ELSE 0 END) AS null_bldgs_other_sqft,
    SUM(CASE WHEN [ Acres_Owned]             IS NULL                                  THEN 1 ELSE 0 END) AS null_acres_owned,
    SUM(CASE WHEN [ Total_Acres]             IS NULL                                  THEN 1 ELSE 0 END) AS null_total_acres,
    SUM(CASE WHEN Plant_Replacement_Value_M  IS NULL                                  THEN 1 ELSE 0 END) AS null_plant_replacement_value_m,
    SUM(CASE WHEN Latitude                   IS NULL                                  THEN 1 ELSE 0 END) AS null_latitude,
    SUM(CASE WHEN Longitude                  IS NULL                                  THEN 1 ELSE 0 END) AS null_longitude,
    SUM(CASE WHEN Lat_Long_Sources           IS NULL OR TRIM(Lat_Long_Sources)  = '' THEN 1 ELSE 0 END) AS null_lat_long_sources
FROM {{ source('landing', 'base_structure_report') }};


-- A3. Grain check - Site + Component + Country_State uniqueness
WITH grain_check AS (
    SELECT
        Site,
        Component,
        Country_State,
        COUNT(*) AS row_count
    FROM {{ source('landing', 'base_structure_report') }}
    GROUP BY Site, Component, Country_State
    HAVING COUNT(*) > 1
)
SELECT *
FROM grain_check
ORDER BY row_count DESC;


-- A4. Exact duplicate rows (full-row match)
WITH dupes AS (
    SELECT *,
           COUNT(*) OVER (
               PARTITION BY
                   Country_State,
                   Site,
                   Component,
                   Name_Nearest_City,
                   [ Count_Building_Owned],
                   [ Building_Owned_SqFt],
                   [ Count_Bldgs_Leased],
                   Bldgs_Leased_SqFt,
                   [ Count_Bldgs_Other],
                   [ Bldgs_Other_SqFt],
                   [ Acres_Owned],
                   [ Total_Acres],
                   Plant_Replacement_Value_M,
                   Latitude,
                   Longitude,
                   Lat_Long_Sources
           ) AS row_count
    FROM {{ source('landing', 'base_structure_report') }}
)
SELECT *
FROM dupes
WHERE row_count > 1
ORDER BY row_count DESC;


-- =========================================
-- B. DMDC PERSONNEL ASSIGNMENT
-- Source: landing.dmdc_personnel
-- Declared grain: one row per country/state
--                 per branch per component category
-- Note: source is wide format - one column per
-- branch/component combination. Unpivoted to
-- declared grain in staging.
-- =========================================

-- B1. Shape and volume
SELECT
    COUNT(*)                            AS total_rows,
    COUNT(DISTINCT DUTY_STATE_COUNTRY)  AS distinct_locations,
    COUNT(DISTINCT LOCATION)            AS distinct_location_groups
FROM {{ source('landing', 'dmdc_personnel') }};


-- B2. Null / blank counts on all columns
SELECT
    SUM(CASE WHEN LOCATION                   IS NULL OR TRIM(LOCATION)           = '' THEN 1 ELSE 0 END) AS null_location,
    SUM(CASE WHEN DUTY_STATE_COUNTRY         IS NULL OR TRIM(DUTY_STATE_COUNTRY) = '' THEN 1 ELSE 0 END) AS null_duty_state_country,
    SUM(CASE WHEN ARMY_AD                    IS NULL THEN 1 ELSE 0 END) AS null_army_ad,
    SUM(CASE WHEN NAVY_AD                    IS NULL THEN 1 ELSE 0 END) AS null_navy_ad,
    SUM(CASE WHEN MARINE_CORPS_AD            IS NULL THEN 1 ELSE 0 END) AS null_marine_corps_ad,
    SUM(CASE WHEN AIR_FORCE_AD               IS NULL THEN 1 ELSE 0 END) AS null_air_force_ad,
    SUM(CASE WHEN SPACE_FORCE_AD             IS NULL THEN 1 ELSE 0 END) AS null_space_force_ad,
    SUM(CASE WHEN COAST_GUARD_AD             IS NULL THEN 1 ELSE 0 END) AS null_coast_guard_ad,
    SUM(CASE WHEN ARMY_NATIONAL_GUARD        IS NULL THEN 1 ELSE 0 END) AS null_army_national_guard,
    SUM(CASE WHEN ARMY_RESERVE               IS NULL THEN 1 ELSE 0 END) AS null_army_reserve,
    SUM(CASE WHEN NAVY_RESERVE               IS NULL THEN 1 ELSE 0 END) AS null_navy_reserve,
    SUM(CASE WHEN MARINE_CORPS_RESERVE       IS NULL THEN 1 ELSE 0 END) AS null_marine_corps_reserve,
    SUM(CASE WHEN AIR_NATIONAL_GUARD         IS NULL THEN 1 ELSE 0 END) AS null_air_national_guard,
    SUM(CASE WHEN AIR_FORCE_RESERVE          IS NULL THEN 1 ELSE 0 END) AS null_air_force_reserve,
    SUM(CASE WHEN COAST_GUARD_RESERVE        IS NULL THEN 1 ELSE 0 END) AS null_coast_guard_reserve,
    SUM(CASE WHEN ARMY_DOD_CIVILIAN          IS NULL THEN 1 ELSE 0 END) AS null_army_dod_civilian,
    SUM(CASE WHEN NAVY_DOD_CIVILIAN          IS NULL THEN 1 ELSE 0 END) AS null_navy_dod_civilian,
    SUM(CASE WHEN MARINE_CORPS_DOD_CIVILIAN  IS NULL THEN 1 ELSE 0 END) AS null_marine_corps_dod_civilian,
    SUM(CASE WHEN AIR_FORCE_DOD_CIVILIAN     IS NULL THEN 1 ELSE 0 END) AS null_air_force_dod_civilian,
    SUM(CASE WHEN FOURTH_ESTATE_DOD_CIVILIAN IS NULL THEN 1 ELSE 0 END) AS null_fourth_estate_dod_civilian
FROM {{ source('landing', 'dmdc_personnel') }};


-- B3. Grain check - DUTY_STATE_COUNTRY uniqueness
WITH grain_check AS (
    SELECT
        DUTY_STATE_COUNTRY,
        COUNT(*) AS row_count
    FROM {{ source('landing', 'dmdc_personnel') }}
    GROUP BY DUTY_STATE_COUNTRY
    HAVING COUNT(*) > 1
)
SELECT *
FROM grain_check
ORDER BY row_count DESC;


-- B4. Exact duplicate rows (full-row match)
WITH dupes AS (
    SELECT *,
           COUNT(*) OVER (
               PARTITION BY
                   LOCATION,
                   DUTY_STATE_COUNTRY,
                   ARMY_AD,
                   NAVY_AD,
                   MARINE_CORPS_AD,
                   AIR_FORCE_AD,
                   SPACE_FORCE_AD,
                   COAST_GUARD_AD,
                   ARMY_NATIONAL_GUARD,
                   ARMY_RESERVE,
                   NAVY_RESERVE,
                   MARINE_CORPS_RESERVE,
                   AIR_NATIONAL_GUARD,
                   AIR_FORCE_RESERVE,
                   COAST_GUARD_RESERVE,
                   ARMY_DOD_CIVILIAN,
                   NAVY_DOD_CIVILIAN,
                   MARINE_CORPS_DOD_CIVILIAN,
                   AIR_FORCE_DOD_CIVILIAN,
                   FOURTH_ESTATE_DOD_CIVILIAN
           ) AS row_count
    FROM {{ source('landing', 'dmdc_personnel') }}
)
SELECT *
FROM dupes
WHERE row_count > 1
ORDER BY row_count DESC;


-- =========================================
-- C. GEM MAIN - FIELD LEVEL
-- Source: landing.gem_main
-- Declared grain: one row per energy field
-- =========================================

-- C1. Shape and volume
SELECT
    COUNT(*)                        AS total_rows,
    COUNT(DISTINCT Unit_ID)         AS distinct_unit_ids,
    COUNT(DISTINCT Country_Area)    AS distinct_countries,
    COUNT(DISTINCT Status)          AS distinct_statuses,
    COUNT(DISTINCT Fuel_Type)       AS distinct_fuel_types
FROM {{ source('landing', 'gem_main') }};


-- C2. Null / blank counts on all columns
SELECT
    SUM(CASE WHEN Unit_ID              IS NULL OR TRIM(Unit_ID)              = '' THEN 1 ELSE 0 END) AS null_unit_id,
    SUM(CASE WHEN Unit_Name            IS NULL OR TRIM(Unit_Name)            = '' THEN 1 ELSE 0 END) AS null_unit_name,
    SUM(CASE WHEN Unit_Name_Local_Script IS NULL OR TRIM(Unit_Name_Local_Script) = '' THEN 1 ELSE 0 END) AS null_unit_name_local_script,
    SUM(CASE WHEN Fuel_Type            IS NULL OR TRIM(Fuel_Type)            = '' THEN 1 ELSE 0 END) AS null_fuel_type,
    SUM(CASE WHEN Country_Area         IS NULL OR TRIM(Country_Area)         = '' THEN 1 ELSE 0 END) AS null_country_area,
    SUM(CASE WHEN Subnational_Unit     IS NULL OR TRIM(Subnational_Unit)     = '' THEN 1 ELSE 0 END) AS null_subnational_unit,
    SUM(CASE WHEN Production_Type      IS NULL OR TRIM(Production_Type)      = '' THEN 1 ELSE 0 END) AS null_production_type,
    SUM(CASE WHEN Status               IS NULL OR TRIM(Status)               = '' THEN 1 ELSE 0 END) AS null_status,
    SUM(CASE WHEN Status_Detail        IS NULL OR TRIM(Status_Detail)        = '' THEN 1 ELSE 0 END) AS null_status_detail,
    SUM(CASE WHEN Status_Year          IS NULL                                    THEN 1 ELSE 0 END) AS null_status_year,
    SUM(CASE WHEN Discovery_Year       IS NULL                                    THEN 1 ELSE 0 END) AS null_discovery_year,
    SUM(CASE WHEN FID_Year             IS NULL                                    THEN 1 ELSE 0 END) AS null_fid_year,
    SUM(CASE WHEN Production_Start_Year IS NULL                                   THEN 1 ELSE 0 END) AS null_production_start_year,
    SUM(CASE WHEN Operator             IS NULL OR TRIM(Operator)             = '' THEN 1 ELSE 0 END) AS null_operator,
    SUM(CASE WHEN Owners               IS NULL OR TRIM(Owners)               = '' THEN 1 ELSE 0 END) AS null_owners,
    SUM(CASE WHEN Parents              IS NULL OR TRIM(Parents)              = '' THEN 1 ELSE 0 END) AS null_parents,
    SUM(CASE WHEN Government_Unit_ID   IS NULL OR TRIM(Government_Unit_ID)   = '' THEN 1 ELSE 0 END) AS null_government_unit_id,
    SUM(CASE WHEN Wiki_URL_Project     IS NULL OR TRIM(Wiki_URL_Project)     = '' THEN 1 ELSE 0 END) AS null_wiki_url_project,
    SUM(CASE WHEN Wiki_URL_Field       IS NULL OR TRIM(Wiki_URL_Field)       = '' THEN 1 ELSE 0 END) AS null_wiki_url_field,
    SUM(CASE WHEN Name_Other           IS NULL OR TRIM(Name_Other)           = '' THEN 1 ELSE 0 END) AS null_name_other,
    SUM(CASE WHEN Latitude             IS NULL                                    THEN 1 ELSE 0 END) AS null_latitude,
    SUM(CASE WHEN Longitude            IS NULL                                    THEN 1 ELSE 0 END) AS null_longitude,
    SUM(CASE WHEN Location_Accuracy    IS NULL OR TRIM(Location_Accuracy)    = '' THEN 1 ELSE 0 END) AS null_location_accuracy,
    SUM(CASE WHEN Onshore_Offshore     IS NULL OR TRIM(Onshore_Offshore)     = '' THEN 1 ELSE 0 END) AS null_onshore_offshore,
    SUM(CASE WHEN Field_Outline_WKT    IS NULL OR TRIM(Field_Outline_WKT)    = '' THEN 1 ELSE 0 END) AS null_field_outline_wkt,
    SUM(CASE WHEN Basin                IS NULL OR TRIM(Basin)                = '' THEN 1 ELSE 0 END) AS null_basin,
    SUM(CASE WHEN Blocks               IS NULL OR TRIM(Blocks)               = '' THEN 1 ELSE 0 END) AS null_blocks
FROM {{ source('landing', 'gem_main') }};


-- C3. Grain check - Unit_ID uniqueness
WITH grain_check AS (
    SELECT
        Unit_ID,
        COUNT(*) AS row_count
    FROM {{ source('landing', 'gem_main') }}
    GROUP BY Unit_ID
    HAVING COUNT(*) > 1
)
SELECT *
FROM grain_check
ORDER BY row_count DESC;


-- C4. Exact duplicate rows (full-row match)
WITH dupes AS (
    SELECT *,
           COUNT(*) OVER (
               PARTITION BY
                   Unit_ID,
                   Unit_Name,
                   Fuel_Type,
                   Country_Area,
                   Subnational_Unit,
                   Production_Type,
                   Status,
                   Status_Year,
                   Operator,
                   Onshore_Offshore,
                   Latitude,
                   Longitude,
                   Location_Accuracy,
                   Basin
           ) AS row_count
    FROM {{ source('landing', 'gem_main') }}
)
SELECT *
FROM dupes
WHERE row_count > 1
ORDER BY row_count DESC;


-- =========================================
-- D. GEM PRODUCTION - FIELD LEVEL
-- Source: landing.gem_production
-- Declared grain: one row per field per
--                 fuel description per data year
-- =========================================

-- D1. Shape and volume
SELECT
    COUNT(*)                            AS total_rows,
    COUNT(DISTINCT Unit_ID)             AS distinct_unit_ids,
    COUNT(DISTINCT Fuel_Description)    AS distinct_fuel_descriptions,
    COUNT(DISTINCT Data_Year)           AS distinct_data_years,
    MIN(Data_Year)                      AS min_data_year,
    MAX(Data_Year)                      AS max_data_year
FROM {{ source('landing', 'gem_production') }};


-- D2. Null / blank counts on all columns
SELECT
    SUM(CASE WHEN Unit_ID            IS NULL OR TRIM(Unit_ID)            = '' THEN 1 ELSE 0 END) AS null_unit_id,
    SUM(CASE WHEN Unit_Name          IS NULL OR TRIM(Unit_Name)          = '' THEN 1 ELSE 0 END) AS null_unit_name,
    SUM(CASE WHEN Country_Area       IS NULL OR TRIM(Country_Area)       = '' THEN 1 ELSE 0 END) AS null_country_area,
    SUM(CASE WHEN Fuel_Description   IS NULL OR TRIM(Fuel_Description)   = '' THEN 1 ELSE 0 END) AS null_fuel_description,
    SUM(CASE WHEN Quantity_Original  IS NULL                                   THEN 1 ELSE 0 END) AS null_quantity_original,
    SUM(CASE WHEN Units_Original     IS NULL OR TRIM(Units_Original)     = '' THEN 1 ELSE 0 END) AS null_units_original,
    SUM(CASE WHEN Quantity_Converted IS NULL                                   THEN 1 ELSE 0 END) AS null_quantity_converted,
    SUM(CASE WHEN Units_Converted    IS NULL OR TRIM(Units_Converted)    = '' THEN 1 ELSE 0 END) AS null_units_converted,
    SUM(CASE WHEN Data_Year          IS NULL                                   THEN 1 ELSE 0 END) AS null_data_year
FROM {{ source('landing', 'gem_production') }};


-- D3. Grain check - Unit_ID + Fuel_Description + Data_Year
WITH grain_check AS (
    SELECT
        Unit_ID,
        Fuel_Description,
        Data_Year,
        COUNT(*) AS row_count
    FROM {{ source('landing', 'gem_production') }}
    GROUP BY Unit_ID, Fuel_Description, Data_Year
    HAVING COUNT(*) > 1
)
SELECT *
FROM grain_check
ORDER BY row_count DESC;


-- D4. Exact duplicate rows (full-row match)
WITH dupes AS (
    SELECT *,
           COUNT(*) OVER (
               PARTITION BY
                   Unit_ID,
                   Unit_Name,
                   Country_Area,
                   Fuel_Description,
                   Quantity_Original,
                   Units_Original,
                   Quantity_Converted,
                   Units_Converted,
                   Data_Year
           ) AS row_count
    FROM {{ source('landing', 'gem_production') }}
)
SELECT *
FROM dupes
WHERE row_count > 1
ORDER BY row_count DESC;


-- =========================================
-- E. GEM RESERVES - FIELD LEVEL
-- Source: landing.gem_reserves
-- Declared grain: one row per field per
--                 fuel description per
--                 reserves classification
--                 per data year
-- =========================================

-- E1. Shape and volume
SELECT
    COUNT(*)                                AS total_rows,
    COUNT(DISTINCT Unit_ID)                 AS distinct_unit_ids,
    COUNT(DISTINCT Fuel_Description)        AS distinct_fuel_descriptions,
    COUNT(DISTINCT Reserves_Classification) AS distinct_classifications,
    COUNT(DISTINCT Data_Year)               AS distinct_data_years,
    MIN(Data_Year)                          AS min_data_year,
    MAX(Data_Year)                          AS max_data_year
FROM {{ source('landing', 'gem_reserves') }};


-- E2. Null / blank counts on all columns
SELECT
    SUM(CASE WHEN Unit_ID                 IS NULL OR TRIM(Unit_ID)                 = '' THEN 1 ELSE 0 END) AS null_unit_id,
    SUM(CASE WHEN Unit_Name               IS NULL OR TRIM(Unit_Name)               = '' THEN 1 ELSE 0 END) AS null_unit_name,
    SUM(CASE WHEN Country_Area            IS NULL OR TRIM(Country_Area)            = '' THEN 1 ELSE 0 END) AS null_country_area,
    SUM(CASE WHEN Fuel_Description        IS NULL OR TRIM(Fuel_Description)        = '' THEN 1 ELSE 0 END) AS null_fuel_description,
    SUM(CASE WHEN Reserves_Classification IS NULL OR TRIM(Reserves_Classification) = '' THEN 1 ELSE 0 END) AS null_reserves_classification,
    SUM(CASE WHEN Quantity                IS NULL                                       THEN 1 ELSE 0 END) AS null_quantity,
    SUM(CASE WHEN Units                   IS NULL OR TRIM(Units)                   = '' THEN 1 ELSE 0 END) AS null_units,
    SUM(CASE WHEN Quantity_Converted      IS NULL                                       THEN 1 ELSE 0 END) AS null_quantity_converted,
    SUM(CASE WHEN Units_Converted         IS NULL OR TRIM(Units_Converted)         = '' THEN 1 ELSE 0 END) AS null_units_converted,
    SUM(CASE WHEN Data_Year               IS NULL                                       THEN 1 ELSE 0 END) AS null_data_year
FROM {{ source('landing', 'gem_reserves') }};


-- E3. Grain check - Unit_ID + Fuel_Description +
--                   Reserves_Classification + Data_Year
WITH grain_check AS (
    SELECT
        Unit_ID,
        Fuel_Description,
        Reserves_Classification,
        Data_Year,
        COUNT(*) AS row_count
    FROM {{ source('landing', 'gem_reserves') }}
    GROUP BY Unit_ID, Fuel_Description, Reserves_Classification, Data_Year
    HAVING COUNT(*) > 1
)
SELECT *
FROM grain_check
ORDER BY row_count DESC;


-- E4. Exact duplicate rows (full-row match)
WITH dupes AS (
    SELECT *,
           COUNT(*) OVER (
               PARTITION BY
                   Unit_ID,
                   Unit_Name,
                   Country_Area,
                   Fuel_Description,
                   Reserves_Classification,
                   Quantity,
                   Units,
                   Quantity_Converted,
                   Units_Converted,
                   Data_Year
           ) AS row_count
    FROM {{ source('landing', 'gem_reserves') }}
)
SELECT *
FROM dupes
WHERE row_count > 1
ORDER BY row_count DESC;


-- =========================================
-- F. CROSS-DATASET PROFILING
-- These queries to be run last - they depend on
-- understanding each source in isolation first
-- =========================================

-- F1. Geography join readiness - BSR vs DMDC
-- Confirms viability of conformed dim_geography
SELECT
    COUNT(DISTINCT d.DUTY_STATE_COUNTRY)                        AS total_dmdc_locations,
    COUNT(DISTINCT CASE WHEN b.Country_State IS NOT NULL
        THEN d.DUTY_STATE_COUNTRY END)                          AS matched_to_bsr,
    COUNT(DISTINCT CASE WHEN b.Country_State IS NULL
        THEN d.DUTY_STATE_COUNTRY END)                          AS unmatched_to_bsr
FROM {{ source('landing', 'dmdc_personnel') }} d
LEFT JOIN (
    SELECT DISTINCT Country_State
    FROM {{ source('landing', 'base_structure_report') }}
) b ON TRIM(LOWER(d.DUTY_STATE_COUNTRY)) = TRIM(LOWER(b.Country_State));


-- F2. GEM production vs reserves Unit_ID overlap
SELECT
    COUNT(DISTINCT CASE WHEN r.Unit_ID IS NULL
        THEN p.Unit_ID END)                                     AS in_production_only,
    COUNT(DISTINCT CASE WHEN p.Unit_ID IS NULL
        THEN r.Unit_ID END)                                     AS in_reserves_only,
    COUNT(DISTINCT CASE WHEN p.Unit_ID IS NOT NULL
        AND r.Unit_ID IS NOT NULL
        THEN p.Unit_ID END)                                     AS in_both
FROM {{ source('landing', 'gem_production') }} p
FULL OUTER JOIN {{ source('landing', 'gem_reserves') }} r
    ON p.Unit_ID = r.Unit_ID;


-- F3. Country coverage across all sources
SELECT
    all_countries.country,
    MAX(CASE WHEN src = 'gem'  THEN 1 ELSE 0 END)              AS in_gem,
    MAX(CASE WHEN src = 'dmdc' THEN 1 ELSE 0 END)              AS in_dmdc,
    MAX(CASE WHEN src = 'bsr'  THEN 1 ELSE 0 END)              AS in_bsr
FROM (
    SELECT DISTINCT Country_Area        AS country, 'gem'  AS src
    FROM {{ source('landing', 'gem_main') }}
    UNION ALL
    SELECT DISTINCT DUTY_STATE_COUNTRY  AS country, 'dmdc' AS src
    FROM {{ source('landing', 'dmdc_personnel') }}
    UNION ALL
    SELECT DISTINCT Country_State       AS country, 'bsr'  AS src
    FROM {{ source('landing', 'base_structure_report') }}
) all_countries
GROUP BY all_countries.country
ORDER BY in_gem DESC, in_dmdc DESC, in_bsr DESC;
