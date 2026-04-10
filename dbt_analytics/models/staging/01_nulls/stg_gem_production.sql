-- models/staging/stg_gem_production.sql
--
-- Staging model for GEM field-level production data
-- Source: landing.gem_production
--
-- Cleaning applied:
--   1. Set null Data_Year to 2026 - per GEM field definition,
--      a null data year means year of the source document (March 2026)
--   2. Correct Data_Year 2029 to 2019 - identified as a typo
--      during profiling (D1)
--   3. Standardise all column names to snake_case

WITH source AS (
    SELECT
        Unit_ID             AS unit_id,
        Unit_Name           AS unit_name,
        Country_Area        AS country_area,
        Fuel_Description    AS fuel_description,
        Quantity_Original   AS quantity_original,
        Units_Original      AS units_original,
        Quantity_Converted  AS quantity_converted,
        Units_Converted     AS units_converted,
        Data_Year           AS data_year
    FROM {{ source('landing', 'gem_production') }}
),

cleaned AS (
    SELECT
        unit_id,
        unit_name,
        country_area,
        fuel_description,
        quantity_original,
        units_original,
        quantity_converted,
        units_converted,
        CASE
            WHEN data_year IS NULL THEN 2026
            WHEN data_year = 2029  THEN 2019
            ELSE data_year
        END AS data_year
    FROM source
)

SELECT * FROM cleaned
