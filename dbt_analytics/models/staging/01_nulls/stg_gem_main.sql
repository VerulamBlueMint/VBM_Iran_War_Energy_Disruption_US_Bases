-- models/staging/stg_gem_main.sql
--
-- Staging model for GEM field-level main data
-- Source: landing.gem_main
--
-- Cleaning applied:
--   1. Standardise all column names to snake_case
--
-- No null issues found in analytically critical columns.
-- 618 null lat/lon rows retained - these fields will not
-- render on the map layer in the dashboard.
-- High null rates on optional columns (owners, basin, blocks
-- etc.) are expected and retained as-is.

WITH source AS (
    SELECT
        Unit_ID                 AS unit_id,
        Unit_Name               AS unit_name,
        Unit_Name_Local_Script  AS unit_name_local_script,
        Fuel_Type               AS fuel_type,
        Country_Area            AS country_area,
        Subnational_Unit        AS subnational_unit,
        Production_Type         AS production_type,
        Status                  AS status,
        Status_Detail           AS status_detail,
        Status_Year             AS status_year,
        Discovery_Year          AS discovery_year,
        FID_Year                AS fid_year,
        Production_Start_Year   AS production_start_year,
        Operator                AS operator,
        Owners                  AS owners,
        Parents                 AS parents,
        Government_Unit_ID      AS government_unit_id,
        Wiki_URL_Project        AS wiki_url_project,
        Wiki_URL_Field          AS wiki_url_field,
        Name_Other              AS name_other,
        Latitude                AS latitude,
        Longitude               AS longitude,
        Location_Accuracy       AS location_accuracy,
        Onshore_Offshore        AS onshore_offshore,
        Field_Outline_WKT       AS field_outline_wkt,
        Basin                   AS basin,
        Blocks                  AS blocks
    FROM {{ source('landing', 'gem_main') }}
)

SELECT * FROM source
