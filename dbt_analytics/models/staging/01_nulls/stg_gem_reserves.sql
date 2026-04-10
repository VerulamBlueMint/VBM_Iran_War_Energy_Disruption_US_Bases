-- models/staging/stg_gem_reserves.sql
--
-- Staging model for GEM field-level reserves data
-- Source: landing.gem_reserves
--
-- Cleaning applied:
--   1. Set null Data_Year to 2026 - per GEM field definition,
--      a null data year means year of the source document (March 2026)
--      This also resolves the 2 grain violations identified in E3
--      (L100000312399 and L100000312412) which were caused by
--      null Data_Year values
--   2. Standardise all column names to snake_case

WITH source AS (
    SELECT
        Unit_ID                 AS unit_id,
        Unit_Name               AS unit_name,
        Country_Area            AS country_area,
        Fuel_Description        AS fuel_description,
        Reserves_Classification AS reserves_classification,
        Quantity                AS quantity,
        Units                   AS units,
        Quantity_Converted      AS quantity_converted,
        Units_Converted         AS units_converted,
        Data_Year               AS data_year
    FROM {{ source('landing', 'gem_reserves') }}
),

cleaned AS (
    SELECT
        unit_id,
        unit_name,
        country_area,
        fuel_description,
        reserves_classification,
        quantity,
        units,
        quantity_converted,
        units_converted,
        CASE
            WHEN data_year IS NULL THEN 2026
            ELSE data_year
        END AS data_year
    FROM source
)

SELECT * FROM cleaned
