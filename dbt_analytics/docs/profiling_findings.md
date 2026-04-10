{% docs profiling_findings %}

# Data Profiling Findings - DoD Strategic Intelligence Dashboard

**Sources**:
- `USA_Global_Military_Presence_WH.dbt_dev_landing.Base_Structure_Report_FY25_with_lat_lon_converted`
- `USA_Global_Military_Presence_WH.dbt_dev_landing.DMDC_Website_Location_Report_2512_converted`
- `USA_Global_Military_Presence_WH.dbt_dev_landing.Global_Oil_and_Gas_Extraction_Tracker_March_2026_field_level_main_converted`
- `USA_Global_Military_Presence_WH.dbt_dev_landing.Global_Oil_and_Gas_Extraction_Tracker_March_2026_field_level_production_converted`
- `USA_Global_Military_Presence_WH.dbt_dev_landing.Global_Oil_and_Gas_Extraction_Tracker_March_2026_field_level_reserves_converted`

**Run method**: Copy individual queries from `analyses/profiling.sql` and run directly against the Fabric Warehouse via the SQL endpoint.

**Known loading issue**: Seven BSR columns landed with a leading space in the column name. Use bracket notation until fixed in staging: `[ Count_Building_Owned]`, `[ Building_Owned_SqFt]`, `[ Count_Bldgs_Leased]`, `[ Count_Bldgs_Other]`, `[ Bldgs_Other_SqFt]`, `[ Acres_Owned]`, `[ Total_Acres]`. `Bldgs_Leased_SqFt` is fine.

---

## A. Base Structure Report - base_structure_report
**Declared grain**: Site + Component + Country_State

### A1. Shape and volume
| Metric | Result |
|---|---|
| Total rows | 1,661 |
| Distinct sites | 1,654 |
| Distinct country/states | 100 |
| Distinct components | 13 |

---

### A2. Null / blank baseline
| Field | Null / blank count |
|---|---|
| Country_State | 4 |
| Site | 4 |
| Component | 4 |
| Name_Nearest_City | 4 |
| Count_Building_Owned | 4 |
| Building_Owned_SqFt | 4 |
| Count_Bldgs_Leased | 4 |
| Bldgs_Leased_SqFt | 4 |
| Count_Bldgs_Other | 4 |
| Bldgs_Other_SqFt | 4 |
| Acres_Owned | 4 |
| Total_Acres | 4 |
| Plant_Replacement_Value_M | 4 |
| Latitude | 4 |
| Longitude | 4 |
| Lat_Long_Sources | 4 |

Same 4 nulls across every column - these are completely empty rows with nothing in them at all. Strip them out in staging.

---

### A3. Grain check - Site + Component + Country_State
| Metric | Result |
|---|---|
| Combinations with more than one row | 2 |

- [ ] Grain is clean - no violations
- [x] Grain violations found - resolution required

| Site | Component | Country_State | Row count |
|---|---|---|---|
| NULL | NULL | NULL | 4 |
| BARRIGADA | Navy Active | Guam | 2 |

The 4 null rows are the same empties from A2. BARRIGADA is more interesting - two rows for the same site and component, but with different numbers. Not a simple duplicate. See A4.

---

### A4. Exact duplicate rows (full-row match)
| Duplicate row count | Notes |
|---|---|
| 4 | The 4 empty rows again - nothing new here |

- [ ] No exact duplicate rows found
- [x] Exact duplicate rows found - deduplication required

The 4 nulls show up again because they are identical to each other. BARRIGADA doesn't appear here because the two rows have different values - they're not exact copies.

**BARRIGADA breakdown**:

| Row | Buildings | Sq Ft | Acres Owned | Total Acres | PRV ($M) |
|---|---|---|---|---|---|
| Row 1 | 32 | 253,178 | 1,365.96 | 1,570.29 | $838.06 |
| Row 2 | 3 | 664 | 431.89 | 432.97 | $5.18 |

Two separate parcels, no sub-site ID to tell them apart. Best approach is to sum the measures and treat them as one site. Combined: 35 buildings, 253,842 sq ft, 1,797.85 acres owned, 2,003.26 total acres, $843.24M PRV.

---

## B. DMDC Personnel Assignment - dmdc_personnel
**Declared grain**: DUTY_STATE_COUNTRY (wide format - unpivoted in staging)

### B1. Shape and volume
| Metric | Result |
|---|---|
| Total rows | 3,947 |
| Distinct locations (DUTY_STATE_COUNTRY) | 229 |
| Distinct location groups (LOCATION) | 2 |

---

### B2. Null / blank baseline
| Field | Null / blank count |
|---|---|
| LOCATION | 3,715 |
| DUTY_STATE_COUNTRY | 3,715 |
| ARMY_AD | 3,715 |
| NAVY_AD | 3,715 |
| MARINE_CORPS_AD | 3,715 |
| AIR_FORCE_AD | 3,714 |
| SPACE_FORCE_AD | 3,715 |
| COAST_GUARD_AD | 3,715 |
| ARMY_NATIONAL_GUARD | 3,715 |
| ARMY_RESERVE | 3,715 |
| NAVY_RESERVE | 3,715 |
| MARINE_CORPS_RESERVE | 3,715 |
| AIR_NATIONAL_GUARD | 3,715 |
| AIR_FORCE_RESERVE | 3,715 |
| COAST_GUARD_RESERVE | 3,715 |
| ARMY_DOD_CIVILIAN | 3,715 |
| NAVY_DOD_CIVILIAN | 3,715 |
| MARINE_CORPS_DOD_CIVILIAN | 3,715 |
| AIR_FORCE_DOD_CIVILIAN | 3,715 |
| FOURTH_ESTATE_DOD_CIVILIAN | 3,715 |

3,715 out of 3,947 rows are completely empty - Fabric Get Data picked up thousands of trailing blank rows from the source file. I will filter out 3,715 null rows in staging.

---

### B3. Grain check - DUTY_STATE_COUNTRY uniqueness
| Metric | Result |
|---|---|
| Locations with more than one row | 3 |

- [ ] Grain is clean - one row per location
- [x] Grain violations found - resolution required

| DUTY_STATE_COUNTRY | Row count |
|---|---|
| NULL | 3,715 |
| GEORGIA | 2 |
| PUERTO RICO | 2 |
| GUAM | 2 |

The 3,715 nulls are the ghost rows again. GEORGIA, PUERTO RICO and GUAM each appear twice - these need checking. 

---

### B4. Exact duplicate rows (full-row match)
| Duplicate row count | Notes |
|---|---|
| 3,715 | Ghost rows again |

- [ ] No exact duplicate rows found
- [x] Exact duplicate rows found - deduplication required

Same story as B2 and B3.

---

## C. GEM Main - gem_main
**Declared grain**: Unit_ID

### C1. Shape and volume
| Metric | Result |
|---|---|
| Total rows | 7,673 |
| Distinct Unit_IDs | 7,673 |
| Distinct countries | 95 |
| Distinct statuses | 9 |
| Distinct fuel types | 4 |

Unit_ID count matches total rows exactly - grain is already confirmed clean before we even run C3.

---

### C2. Null / blank baseline
| Field | Null / blank count |
|---|---|
| Unit_ID | 0 |
| Unit_Name | 0 |
| Unit_Name_Local_Script | 5,579 |
| Fuel_Type | 0 |
| Country_Area | 0 |
| Subnational_Unit | 2,885 |
| Production_Type | 2,530 |
| Status | 213 |
| Status_Detail | 7,549 |
| Status_Year | 222 |
| Discovery_Year | 2,459 |
| FID_Year | 7,017 |
| Production_Start_Year | 5,301 |
| Operator | 1,273 |
| Owners | 3,752 |
| Parents | 3,752 |
| Government_Unit_ID | 4,053 |
| Wiki_URL_Project | 5,150 |
| Wiki_URL_Field | 0 |
| Name_Other | 7,155 |
| Latitude | 618 |
| Longitude | 618 |
| Location_Accuracy | 617 |
| Onshore_Offshore | 0 |
| Field_Outline_WKT | 6,563 |
| Basin | 5,332 |
| Blocks | 5,347 |

High null counts on many columns but that's just the nature of this dataset - GEM doesn't have complete information for every field in the world. The columns that matter most for the model are all clean: Unit_ID, Fuel_Type, Country_Area, Wiki_URL_Field, Onshore_Offshore all zero nulls. The 618 null lat/lons mean those fields won't show on the map - noted but not a blocker. Local script names are only populated for non-Latin countries so 5,579 nulls there is expected.

---

### C3. Grain check - Unit_ID uniqueness
| Metric | Result |
|---|---|
| Unit_IDs with more than one row | 0 |

- [x] Grain is clean - Unit_ID is unique
- [ ] Grain violations found - resolution required

Already obvious from C1 but confirmed. Unit_ID is a solid primary key.

---

### C4. Exact duplicate rows (full-row match)
| Duplicate row count | Notes |
|---|---|
| 0 | Clean |

- [x] No exact duplicate rows found
- [ ] Exact duplicate rows found - deduplication required

---

## D. GEM Production - gem_production
**Declared grain**: Unit_ID + Fuel_Description + Data_Year

### D1. Shape and volume
| Metric | Result |
|---|---|
| Total rows | 11,834 |
| Distinct Unit_IDs | 6,045 |
| Distinct fuel descriptions | 25 |
| Distinct data years | 45 |
| Min data year | 1975 |
| Max data year | 2029 |

Data years go up to 2029 - I will assume a typo and will treat it as 2019 in staging.

---

### D2. Null / blank baseline
| Field | Null / blank count |
|---|---|
| Unit_ID | 0 |
| Unit_Name | 0 |
| Country_Area | 0 |
| Fuel_Description | 0 |
| Quantity_Original | 0 |
| Units_Original | 0 |
| Quantity_Converted | 0 |
| Units_Converted | 0 |
| Data_Year | 20 |

Very clean. The 20 null Data_Year rows are not a problem - a production record with no year can be used because GEM production notes tell us to treat a null as meaning year of source document. Set to 2026 in staging.
---

### D3. Grain check - Unit_ID + Fuel_Description + Data_Year
| Metric | Result |
|---|---|
| Combinations with more than one row | 0 |

- [x] Grain is clean - no violations
- [ ] Grain violations found - resolution required

---

### D4. Exact duplicate rows (full-row match)
| Duplicate row count | Notes |
|---|---|
| 0 | Clean |

- [x] No exact duplicate rows found
- [ ] Exact duplicate rows found - deduplication required

---

## E. GEM Reserves - gem_reserves
**Declared grain**: Unit_ID + Fuel_Description + Reserves_Classification + Data_Year

### E1. Shape and volume
| Metric | Result |
|---|---|
| Total rows | 7,060 |
| Distinct Unit_IDs | 4,251 |
| Distinct fuel descriptions | 21 |
| Distinct reserves classifications | 233 |
| Distinct data years | 47 |
| Min data year | 1938 |
| Max data year | 2025 |

233 different classification codes is a lot. This is the double-counting risk we flagged in the physical data model - you can't just sum reserves across all classifications for a field, you'll massively overstate the total. The analytics tab needs a classification filter.

---

### E2. Null / blank baseline
| Field | Null / blank count |
|---|---|
| Unit_ID | 0 |
| Unit_Name | 0 |
| Country_Area | 0 |
| Fuel_Description | 0 |
| Reserves_Classification | 0 |
| Quantity | 0 |
| Units | 0 |
| Quantity_Converted | 0 |
| Units_Converted | 0 |
| Data_Year | 89 |

Very clean. The 89 null Data_Year rows are not a problem - a production record with no year can be used because 
GEM production notes tell us to treat a null as meaning year of source document. I will set to 2026 in staging

---

### E3. Grain check - Unit_ID + Fuel_Description + Reserves_Classification + Data_Year
| Metric | Result |
|---|---|
| Combinations with more than one row | 2 |

- [ ] Grain is clean - no violations
- [x] Grain violations found - resolution required

| Unit_ID | Fuel_Description | Reserves_Classification | Data_Year | Row count |
|---|---|---|---|---|
| L100000312399 | oil | reserves | NULL | 2 |
| L100000312412 | oil | reserves | NULL | 2 |

Both violations are caused by null Data_Years - same two records, nothing to tell them apart. Dropping null Data_Year rows in staging fixes this automatically.

---

### E4. Exact duplicate rows (full-row match)
| Duplicate row count | Notes |
|---|---|
| 0 | Clean |

- [x] No exact duplicate rows found
- [ ] Exact duplicate rows found - deduplication required

---

## F. Cross-Dataset Profiling

### F1. Geography join readiness - BSR vs DMDC
| Metric | Result |
|---|---|
| Total DMDC locations | 229 |
| Matched to BSR Country_State | 94 |
| Unmatched to BSR Country_State | 135 |

- [ ] Match rate acceptable - conformed dim_geography viable
- [x] Match rate low - name standardisation required

41% match rate. The main culprit is case - DMDC is all caps, BSR is mixed case. Fix the case in staging and the match rate will improve significantly. Some names genuinely differ between sources (TURKEY vs Türkiye, CONGO (BRAZZAVILLE) vs Republic of the Congo) and will need a mapping seed table to resolve. To be addressed in the data cleaning phase.

---

### F2. GEM production vs reserves Unit_ID overlap
| Metric | Result |
|---|---|
| In production only | 3,001 |
| In reserves only | 1,207 |
| In both | 3,044 |

More fields have production data than reserves, and a decent chunk have only one or the other. That's normal for this kind of dataset. The analytics tab just needs to handle missing values gracefully when a field doesn't have both.

---

### F3. Country coverage across all sources

Three issues stop a clean join between sources:

- **Case**: GEM is mixed case, DMDC is all caps - same countries look like different entities
- **Name differences**: TURKEY vs Türkiye, CONGO (BRAZZAVILLE) vs Republic of the Congo etc.
- **Geography type mismatch**: DMDC has US states (ALABAMA, TEXAS), GEM only has countries, BSR has a mix

All three to be resolved with a geography mapping seed table in the data cleaning phase.

---

## Open Decisions

| # | Decision | Informed by | Status |
|---|---|---|---|
| 1 | BSR - drop 4 fully null rows in staging | A2, A3 | Closed |
| 2 | BSR - aggregate BARRIGADA two rows into one by summing measures | A3, A4 | Closed |
| 3 | BSR - alias leading-space column names to clean names in staging | Known issue | Closed |
| 4 | DMDC - reload table to remove 3,715 ghost rows before staging | B2, B3 | Open |
| 5 | DMDC - check GEORGIA, PUERTO RICO, GUAM duplicates after reload | B3 | Open |
| 6 | GEM production - drop 20 null Data_Year rows in staging | D2 | Closed |
| 7 | GEM reserves - drop 89 null Data_Year rows in staging, fixes E3 grain violations | E2, E3 | Closed |
| 8 | GEM reserves - classification filter mandatory in analytics tab | E1 | Closed |
| 9 | Cross-dataset - geography mapping seed table needed before dim_geography can be built | F1, F3 | Open |
| 10 | GEM production vs reserves - handle nulls gracefully in analytics tab for fields missing one or the other | F2 | Closed |

{% enddocs %}
