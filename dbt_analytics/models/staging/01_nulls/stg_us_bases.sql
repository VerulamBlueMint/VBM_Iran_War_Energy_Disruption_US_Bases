-- models/staging/stg_us_bases.sql
--
-- Staging model for the Base Structure Report (BSR) FY25
-- Source: landing.base_structure_report
--
-- Cleaning applied:
--   1. Filter out 4 fully null ghost rows
--   2. Alias 7 columns with leading spaces to clean snake_case names
--   3. Cast all numeric columns from varchar to correct types
--      (landing table stores numbers as varchar including comma-formatted values)
--   4. Aggregate BARRIGADA (Navy Active, Guam) two-row grain violation
--      by summing all measures into a single row
--   5. Standardise all column names to snake_case

WITH source AS (
    SELECT
        Country_State                                                       AS country_state,
        Site                                                                AS site,
        Component                                                           AS component,
        Name_Nearest_City                                                   AS name_nearest_city,
        TRY_CAST(REPLACE([ Count_Building_Owned],  ',', '') AS INT)         AS count_building_owned,
        TRY_CAST(REPLACE([ Building_Owned_SqFt],   ',', '') AS BIGINT)      AS building_owned_sqft,
        TRY_CAST(REPLACE([ Count_Bldgs_Leased],    ',', '') AS INT)         AS count_bldgs_leased,
        TRY_CAST(REPLACE(Bldgs_Leased_SqFt,        ',', '') AS BIGINT)      AS bldgs_leased_sqft,
        TRY_CAST(REPLACE([ Count_Bldgs_Other],     ',', '') AS INT)         AS count_bldgs_other,
        TRY_CAST(REPLACE([ Bldgs_Other_SqFt],      ',', '') AS BIGINT)      AS bldgs_other_sqft,
        TRY_CAST(REPLACE([ Acres_Owned],           ',', '') AS DECIMAL(12,2)) AS acres_owned,
        TRY_CAST(REPLACE([ Total_Acres],           ',', '') AS DECIMAL(12,2)) AS total_acres,
        TRY_CAST(REPLACE(REPLACE(Plant_Replacement_Value_M, '$', ''), ',', '') AS DECIMAL(18,2)) AS plant_replacement_value_m,
        TRY_CAST(Latitude                                  AS DECIMAL(9,6)) AS latitude,
        TRY_CAST(Longitude                                 AS DECIMAL(9,6)) AS longitude,
        Lat_Long_Sources                                                    AS lat_long_sources
    FROM {{ source('landing', 'base_structure_report') }}
    WHERE Country_State IS NOT NULL
      AND Site          IS NOT NULL
      AND Component     IS NOT NULL
),

aggregated AS (
    SELECT
        country_state,
        site,
        component,
        MAX(name_nearest_city)          AS name_nearest_city,
        SUM(count_building_owned)       AS count_building_owned,
        SUM(building_owned_sqft)        AS building_owned_sqft,
        SUM(count_bldgs_leased)         AS count_bldgs_leased,
        SUM(bldgs_leased_sqft)          AS bldgs_leased_sqft,
        SUM(count_bldgs_other)          AS count_bldgs_other,
        SUM(bldgs_other_sqft)           AS bldgs_other_sqft,
        SUM(acres_owned)                AS acres_owned,
        SUM(total_acres)                AS total_acres,
        SUM(plant_replacement_value_m)  AS plant_replacement_value_m,
        MAX(latitude)                   AS latitude,
        MAX(longitude)                  AS longitude,
        MAX(lat_long_sources)           AS lat_long_sources
    FROM source
    GROUP BY
        country_state,
        site,
        component
)

SELECT * FROM aggregated
