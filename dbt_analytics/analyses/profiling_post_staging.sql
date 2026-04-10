-- =========================================
-- DOD STRATEGIC INTELLIGENCE DASHBOARD
-- POST-STAGING PROFILING QUERIES
-- analyses/profiling_post_staging.sql
-- =========================================
--
-- Run method: Copy individual queries and run
-- directly against Fabric Warehouse via the
-- SQL endpoint. Do not materialise as a model.
--
-- Purpose: Inspect distinct categorical values
-- in the standardised_txt staging views before
-- building the intermediate layer and seed
-- mapping tables. Results feed directly into
-- the geography mapping seed and energy
-- category seeds.
--
-- Sources: dbt_dev_staging standardised_txt views
-- (02_standardised_txt layer - text is lowercased,
-- trimmed, and blanks converted to null)


-- =========================================
-- A. STG_US_BASES_STANDARDISED_TXT
-- =========================================

-- A1. Distinct country_state values
SELECT
    country_state,
    COUNT(*)                                                            AS site_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_us_bases_standardised_txt') }}
GROUP BY country_state
ORDER BY country_state;


-- A2. Distinct component values
SELECT
    component,
    COUNT(*)                                                            AS site_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_us_bases_standardised_txt') }}
GROUP BY component
ORDER BY component;


-- =========================================
-- B. STG_US_TROOPS_STANDARDISED_TXT
-- =========================================

-- B1. Distinct duty_state_country values
SELECT
    duty_state_country,
    COUNT(*)                                                            AS row_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_us_troops_standardised_txt') }}
GROUP BY duty_state_country
ORDER BY duty_state_country;


-- B2. Distinct branch values
SELECT
    branch,
    COUNT(*)                                                            AS row_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_us_troops_standardised_txt') }}
GROUP BY branch
ORDER BY branch;


-- B3. Distinct component_category values
SELECT
    component_category,
    COUNT(*)                                                            AS row_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_us_troops_standardised_txt') }}
GROUP BY component_category
ORDER BY component_category;


-- =========================================
-- C. STG_GEM_MAIN_STANDARDISED_TXT
-- =========================================

-- C1. Distinct country_area values
SELECT
    country_area,
    COUNT(*)                                                            AS field_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_main_standardised_txt') }}
GROUP BY country_area
ORDER BY country_area;


-- C2. Distinct fuel_type values
SELECT
    fuel_type,
    COUNT(*)                                                            AS field_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_main_standardised_txt') }}
GROUP BY fuel_type
ORDER BY fuel_type;


-- C3. Distinct subnational_unit values
SELECT
    subnational_unit,
    COUNT(*)                                                            AS field_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_main_standardised_txt') }}
GROUP BY subnational_unit
ORDER BY subnational_unit;


-- C4. Distinct production_type values
SELECT
    production_type,
    COUNT(*)                                                            AS field_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_main_standardised_txt') }}
GROUP BY production_type
ORDER BY production_type;


-- C5. Distinct status values
SELECT
    status,
    COUNT(*)                                                            AS field_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_main_standardised_txt') }}
GROUP BY status
ORDER BY status;


-- C6. Distinct status_detail values
SELECT
    status_detail,
    COUNT(*)                                                            AS field_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_main_standardised_txt') }}
GROUP BY status_detail
ORDER BY status_detail;


-- C7. Distinct onshore_offshore values
SELECT
    onshore_offshore,
    COUNT(*)                                                            AS field_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_main_standardised_txt') }}
GROUP BY onshore_offshore
ORDER BY onshore_offshore;


-- =========================================
-- D. STG_GEM_PRODUCTION_STANDARDISED_TXT
-- =========================================

-- D1. Distinct country_area values
SELECT
    country_area,
    COUNT(*)                                                            AS row_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_production_standardised_txt') }}
GROUP BY country_area
ORDER BY country_area;


-- D2. Distinct fuel_description values
SELECT
    fuel_description,
    COUNT(*)                                                            AS row_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_production_standardised_txt') }}
GROUP BY fuel_description
ORDER BY fuel_description;


-- D3. Distinct units_original values
SELECT
    units_original,
    COUNT(*)                                                            AS row_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_production_standardised_txt') }}
GROUP BY units_original
ORDER BY units_original;


-- D4. Distinct units_converted values
SELECT
    units_converted,
    COUNT(*)                                                            AS row_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_production_standardised_txt') }}
GROUP BY units_converted
ORDER BY units_converted;


-- =========================================
-- E. STG_GEM_RESERVES_STANDARDISED_TXT
-- =========================================

-- E1. Distinct country_area values
SELECT
    country_area,
    COUNT(*)                                                            AS row_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_reserves_standardised_txt') }}
GROUP BY country_area
ORDER BY country_area;


-- E2. Distinct fuel_description values
SELECT
    fuel_description,
    COUNT(*)                                                            AS row_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_reserves_standardised_txt') }}
GROUP BY fuel_description
ORDER BY fuel_description;


-- E3. Distinct reserves_classification values
SELECT
    reserves_classification,
    COUNT(*)                                                            AS row_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_reserves_standardised_txt') }}
GROUP BY reserves_classification
ORDER BY reserves_classification;


-- E4. Distinct units values (original)
SELECT
    units,
    COUNT(*)                                                            AS row_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_reserves_standardised_txt') }}
GROUP BY units
ORDER BY units;


-- E5. Distinct units_converted values
SELECT
    units_converted,
    COUNT(*)                                                            AS row_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2))     AS pct
FROM {{ ref('stg_gem_reserves_standardised_txt') }}
GROUP BY units_converted
ORDER BY units_converted;