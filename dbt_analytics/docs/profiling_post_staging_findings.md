{% docs profiling_post_staging_findings %}

# Data Profiling Findings - Post-Standardisation

**Sources**: dbt_dev_staging 02_standardised_txt views
**Purpose**: Inspect distinct categorical values before building the intermediate layer and seed mapping tables. All values are lowercased, trimmed, and blanks converted to null at the standardised_txt layer. The values recorded here are exactly what the intermediate layer will join against.

---

## A. stg_us_bases_standardised_txt

### A1. Distinct country_state values
100 distinct values, 1,657 rows. Ghost rows confirmed gone - no nulls in country_state. Text is lowercase throughout confirming standardisation is working correctly.

Covers all 50 US states plus DC, territories, and overseas base locations. Overseas locations present: australia, bahrain, belgium, bulgaria, canada, curacao, cyprus, diego garcia, djibouti, el salvador, estonia, germany, greece, greenland, guantanamo bay naval base, honduras, iceland, israel, italy, japan, johnston atoll, jordan, kenya, kuwait, lithuania, marshall islands, netherlands, niger, norway, oman, peru, poland, portugal, qatar, romania, singapore, south korea, spain, turkey, united arab emirates, united kingdom, wake island.

One truncation noted: `saint helena, ascension, and t` is truncated in the query output - full value is `saint helena, ascension, and tristan da cunha` confirmed from source.

**Decision A1**: All country_state values retained. No naming anomalies at this layer - standardisation has resolved the capitalisation issues present in the raw source. No values excluded.

---

### A2. Distinct component values
13 distinct values. Clean and consistent. Two truncations in the query output: `washington headquarters servic` is `washington headquarters services`.

| component | site_count | pct |
|---|---|---|
| air force active | 334 | 20.17 |
| air force reserve | 12 | 0.72 |
| air national guard | 117 | 7.07 |
| army active | 330 | 19.93 |
| army national guard | 99 | 5.98 |
| army reserve | 200 | 12.08 |
| marine corps active | 66 | 3.99 |
| marine corps reserve | 16 | 0.97 |
| navy active | 431 | 26.03 |
| navy reserve | 22 | 1.33 |
| us army corps of engineers | 1 | 0.06 |
| us space force active | 26 | 1.57 |
| washington headquarters services | 2 | 0.12 |

**Decision A2**: These 13 values are the canonical component values for dim_site. Valid values for accepted_values test confirmed from this list.

---

## B. stg_us_troops_standardised_txt

### B1. Distinct duty_state_country values
232 distinct values. Every location shows exactly 18 rows confirming the unpivot is working correctly - one row per branch/component category combination per location. Georgia, Guam, and Puerto Rico show 36 rows each confirming dual classification under both UNITED STATES and OVERSEAS location groups. This is correct behaviour and not a duplicate.

Two truncations in the query output: `british atlantic ocean territo` is `british atlantic ocean territory` and `micronesia, federated states o` is `micronesia, federated states of`.

**Duplicate value pair requiring resolution in geography mapping seed:**

| Values | Issue | Canonical |
|---|---|---|
| northern mariana / northern mariana islands | Same location, two spellings | northern mariana islands |

**Special codes present:**

| Value | Issue |
|---|---|
| us_zz-unknown | Unknown US location - no geographic equivalent |
| overseas_zz-unknown | Unknown overseas location - no geographic equivalent |
| armed forces europe | Not a country - covers Europe-based personnel |
| armed forces pacific | Not a country - covers Pacific-based personnel |

**Decision B1**: Special codes retained with null geo_key in dim_geography and flagged as unresolvable. Personnel counts are preserved so numbers are not lost. Will appear in dashboard as an unresolved category.

**Naming differences vs BSR to resolve in geography mapping seed:**

| DMDC standardised value | BSR standardised value | Canonical |
|---|---|---|
| korea, south | south korea | south korea |
| virgin islands | virgin islands, u.s. | virgin islands |
| northern mariana | northern mariana islands | northern mariana islands |
| congo (brazzaville) | (not in BSR) | republic of the congo |
| congo (kinshasa) | (not in BSR) | democratic republic of the congo |
| cote divoire | (not in BSR) | cote divoire |
| burma | (not in BSR) | myanmar |

**Decision B2 - case normalisation**: All source values are now lowercase at the standardised_txt layer. Geography mapping seed maps raw_value to canonical_value. All canonical values will be lowercase.

---

### B2. Distinct branch values
7 distinct values. Clean and consistent. Row counts reflect the unpivot structure correctly.

| branch | row_count | pct |
|---|---|---|
| air force | 928 | 22.22 |
| army | 928 | 22.22 |
| coast guard | 464 | 11.11 |
| fourth estate | 232 | 5.56 |
| marine corps | 696 | 16.67 |
| navy | 696 | 16.67 |
| space force | 232 | 5.56 |

**Decision B3**: These 7 values are the canonical branch values for dim_branch. Valid values for accepted_values test confirmed from this list.

---

### B3. Distinct component_category values
4 distinct values. Clean and consistent.

| component_category | row_count | pct |
|---|---|---|
| active duty | 1,392 | 33.33 |
| dod civilian | 1,160 | 27.78 |
| national guard | 464 | 11.11 |
| reserve | 1,160 | 27.78 |

**Decision B4**: These 4 values are the canonical component_category values for dim_branch. Valid values for accepted_values test confirmed from this list.

---

## C. stg_gem_main_standardised_txt

### C1. Distinct country_area values
95 distinct countries. Naming conventions consistent with stg_gem_production and stg_gem_reserves throughout - same spelling, same use of türkiye, côte d'ivoire, republic of the congo, myanmar. United States is the largest single country at 26.18% (2,009 fields).

**GEM naming vs other sources - to resolve in geography mapping seed:**

| GEM standardised value | DMDC standardised value | Canonical |
|---|---|---|
| türkiye | turkey | turkey |
| côte d'ivoire | cote divoire | cote divoire |
| republic of the congo | congo (brazzaville) | republic of the congo |
| myanmar | burma | myanmar |

**Decision C1**: GEM country names are consistent across all three GEM tables. Naming differences vs BSR and DMDC resolved via geography mapping seed.

---

### C2. Distinct fuel_type values
4 distinct values - clean and consistent.

| fuel_type | field_count | pct |
|---|---|---|
| gas | 1,296 | 16.89 |
| gas and condensate | 56 | 0.73 |
| oil | 1,376 | 17.93 |
| oil and gas | 4,945 | 64.45 |

**Decision C2**: These 4 values are the canonical fuel categories for dim_fuel_type. All fuel_description variants in gem_production and gem_reserves will be mapped to one of these 4 values in the fuel_description mapping seed.

---

### C3. Distinct subnational_unit values
37.6% null as expected from first-round profiling. High variety of values - US states, Canadian provinces, Russian oblasts, Chinese provinces, Colombian departments. Not used analytically at this stage.

Note: `khanty-mansi autonomous okrug,` appears with a trailing comma suggesting truncation in the source.

**Decision C3**: No action at this stage. Retained as-is in dim_energy_field.

---

### C4. Distinct production_type values
3 values including NULL.

| production_type | field_count | pct |
|---|---|---|
| NULL | 2,530 | 32.97 |
| conventional | 3,800 | 49.52 |
| unconventional | 1,343 | 17.50 |

**Decision C4**: Retain as-is. NULL values retained in dim_energy_field. Valid values for accepted_values test: conventional, unconventional.

---

### C5. Distinct status values
9 distinct values plus NULL.

| status | field_count | pct |
|---|---|---|
| NULL | 213 | 2.78 |
| abandoned | 21 | 0.27 |
| cancelled | 10 | 0.13 |
| decommissioning | 17 | 0.22 |
| discovered | 455 | 5.93 |
| exploration | 1 | 0.01 |
| in-development | 229 | 2.98 |
| mothballed | 240 | 3.13 |
| operating | 6,481 | 84.47 |
| underground gas storage | 6 | 0.08 |

**Decision C5**: The analytics tab filter for active fields uses the exact string `operating` - lowercase confirmed. All 9 values retained in dim_energy_field. Valid values for accepted_values test confirmed from this list.

---

### C6. Distinct status_detail values
11 values including NULL. 98.38% null - this is a sparsely populated field by design. Values present are operational phase descriptors that provide more granularity than status alone.

| status_detail | field_count | pct |
|---|---|---|
| NULL | 7,549 | 98.38 |
| actual | 22 | 0.29 |
| advanced | 12 | 0.16 |
| assumed | 1 | 0.01 |
| complete | 1 | 0.01 |
| decline | 15 | 0.20 |
| early | 17 | 0.22 |
| in progress | 2 | 0.03 |
| plateau | 3 | 0.04 |
| ramp up | 49 | 0.64 |
| stated | 2 | 0.03 |

**Decision C6**: Retain as-is in dim_energy_field. 98.38% null rate means this column is not suitable as a dashboard filter. Valid values for accepted_values test confirmed from this list if needed.

---

### C7. Distinct onshore_offshore values
3 distinct values. Clean and immediately usable as a dashboard filter.

| onshore_offshore | field_count | pct |
|---|---|---|
| offshore | 2,049 | 26.70 |
| onshore | 5,181 | 67.52 |
| unknown | 443 | 5.77 |

**Decision C7**: These 3 values are the canonical onshore_offshore values for dim_energy_field. Valid values for accepted_values test confirmed from this list. The dashboard operational picture tab filter uses the exact strings `onshore` and `offshore` - lowercase confirmed.

---

## D. stg_gem_production_standardised_txt

### D1. Distinct country_area values
74 distinct countries. Consistent with C1 naming conventions throughout - côte d'ivoire, türkiye, republic of the congo, myanmar all match C1 exactly. Not all C1 countries appear here as some fields have no production data - expected and consistent with first-round profiling findings.

**Decision D1**: No action needed. Country names consistent across all GEM tables.

---

### D2. Distinct fuel_description values
25 distinct values. All now lowercase from the standardised_txt layer.

| fuel_description | row_count | pct |
|---|---|---|
| [not stated] | 1 | 0.01 |
| associated gas | 486 | 4.11 |
| coal bed methane | 1 | 0.01 |
| coal seam gas | 50 | 0.42 |
| condensate | 790 | 6.68 |
| crude oil | 49 | 0.41 |
| crude oil and condensate | 22 | 0.19 |
| dry gas | 246 | 2.08 |
| gas | 3,869 | 32.69 |
| gas and condensate | 1 | 0.01 |
| gas condensate | 249 | 2.10 |
| hydrocarbons | 46 | 0.39 |
| liquids | 7 | 0.06 |
| lpg | 36 | 0.30 |
| ngl | 735 | 6.21 |
| non-associated gas | 191 | 1.61 |
| nonassociated gas | 1 | 0.01 |
| oil | 4,314 | 36.45 |
| oil and condensate | 545 | 4.61 |
| oil and gas | 3 | 0.03 |
| oil and gas condensate | 5 | 0.04 |
| oil and ngl | 1 | 0.01 |
| oil, ngl, and gas | 4 | 0.03 |
| sales gas | 181 | 1.53 |
| total liquids | 1 | 0.01 |

**Decision D2**: Build a fuel_description mapping seed collapsing all 25 variants to the 4 canonical fuel_type values from C2. Mappings:
- oil, crude oil, crude oil and condensate, oil and condensate, oil and ngl, liquids, total liquids, hydrocarbons -> oil
- gas, dry gas, non-associated gas, nonassociated gas, associated gas, sales gas, coal bed methane, coal seam gas -> gas
- oil and gas, oil and gas condensate, oil, ngl, and gas -> oil and gas
- condensate, gas condensate, gas and condensate, lpg, ngl -> gas and condensate
- [not stated] -> null

---

### D3. Distinct units_original values
62 distinct original unit types. Informational only - not used in calculations. No action needed.

---

### D4. Distinct units_converted values
Critical finding. Three values instead of the expected one.

| units_converted | row_count | pct |
|---|---|---|
| million bbl/y | 6,749 | 57.03 |
| million boe/y | 60 | 0.51 |
| million m³/y | 5,025 | 42.46 |

42.46% of production records are in million m³/y and 0.51% in million boe/y. Using quantity_converted directly without normalisation would produce wrong production loss figures for 43% of records.

**Decision D4 - units normalisation**: Add a conversion step in the intermediate layer to normalise all production records to million bbl/y using the following conversion factors:
- million m³/y x 6.29 = million bbl/y
- million boe/y x 1 = million bbl/y (boe and bbl are equivalent)

A new column `quantity_converted_mbbl_y` will be added in the intermediate model. The original `quantity_converted` and `units_converted` columns are retained for traceability.

---

## E. stg_gem_reserves_standardised_txt

### E1. Distinct country_area values
91 distinct countries. Consistent with C1 and D1 naming conventions throughout - côte d'ivoire, türkiye, republic of the congo, myanmar all match. Not all C1 countries appear here - some fields have production data but no reserves data and vice versa, consistent with first-round profiling findings.

**Decision E1**: No action needed. Country names consistent across all GEM tables.

---

### E2. Distinct fuel_description values
21 distinct values. All lowercase from the standardised_txt layer.

| fuel_description | row_count | pct |
|---|---|---|
| [not stated] | 12 | 0.17 |
| associated gas | 19 | 0.27 |
| coal bed methane | 3 | 0.04 |
| coal seam gas | 60 | 0.85 |
| condensate | 509 | 7.21 |
| crude oil | 9 | 0.13 |
| crude oil and condensate | 101 | 1.43 |
| gas | 2,790 | 39.52 |
| gas condensate | 9 | 0.13 |
| hydrocarbons | 202 | 2.86 |
| liquid hydrocarbons | 1 | 0.01 |
| liquids | 40 | 0.57 |
| lpg | 6 | 0.08 |
| ngl | 149 | 2.11 |
| non-associated gas | 2 | 0.03 |
| nonassociated gas | 12 | 0.17 |
| oil | 3,040 | 43.06 |
| oil and condensate | 65 | 0.92 |
| oil and gas | 4 | 0.06 |
| oil and gas condensate | 8 | 0.11 |
| sales gas | 19 | 0.27 |

**Decision E2**: Same fuel_description mapping seed as D2. Additional mapping: liquid hydrocarbons -> oil. [not stated] -> null. Values present in reserves but not production (liquid hydrocarbons) and values present in production but not reserves (dry gas, gas and condensate, oil and ngl, oil, ngl, and gas, total liquids) are documented here for reference - the mapping seed covers all values from both tables.

---

### E3. Distinct reserves_classification values
233 distinct values. Extremely varied - mixes international standards (1p/2p/3p), Russian ABC system (a+b+c1), Norwegian (eur), Norwegian Petroleum Directorate (7f, 5f, 4f), descriptive labels, in-place volumes, and Cyrillic characters. All values are now lowercase from the standardised_txt layer except Cyrillic characters which are unaffected by lower().

High frequency values:

| reserves_classification | row_count | pct |
|---|---|---|
| remaining reserves | 1,711 | 24.24 |
| reserves | 641 | 9.08 |
| 2p reserves | 1,347 | 19.08 |
| remaining recoverable reserves | 335 | 4.75 |
| volume in place | 293 | 4.15 |
| eur | 238 | 3.37 |
| 1p reserves | 276 | 3.91 |
| oil in place | 140 | 1.98 |
| original oil in place | 160 | 2.27 |
| recoverable reserves | 199 | 2.82 |

**Decision E3a - standard classification for analytics tab**: `remaining reserves` is the primary classification (24.24%) as it represents what is left to extract - the most intuitive measure for a destruction scenario. `reserves` is the secondary fallback for fields without a `remaining reserves` figure.

**Decision E3b - in-place volumes**: Retain in the analytics tab calculation. Physical destruction of a field destroys hydrocarbons in place regardless of whether they were economically recoverable. Values like `oil in place`, `stoiip`, `giip`, `volume in place`, `original oil in place`, `gas in place` are included.

**Decision E3c - case inconsistencies now resolved**: Standardised_txt layer has lowercased all values - `2p reserves`, `2P reserves`, and `2p Reserves` are now all `2p reserves`. `1P reserves` and `1p reserves` are now both `1p reserves`. No further action needed for case inconsistencies.

**Decision E3d - Cyrillic characters**: lower() does not affect Cyrillic. The following Cyrillic-containing values require mapping in the reserves_classification mapping seed to Latin equivalents:
- а + в + с1 -> a + b + c1
- а + в1 + в2 + с1 -> a + b1 + b2 + c1
- авс1 + с2 -> abc1 + c2
- авс1+с2 -> abc1+c2
- в + с1 -> b + c1
- в + с1 balance reserves -> b + c1 balance reserves
- в + с1 recoverable reserves -> b + c1 recoverable reserves
- в1 -> b1
- в2 -> b2
- с1 + c2 -> c1 + c2
- с1 + с2 -> c1 + c2
- с1 recoverable reserves -> c1 recoverable reserves
- с2 balance reserves -> c2 balance reserves
- с2 recoverable reserves -> c2 recoverable reserves
- geological reserves в+с1 -> geological reserves b+c1
- geological reserves с2 -> geological reserves c2
- geological в + с1 ю1-2 -> geological b + c1 yu1-2
- geological в + с1 ю1-3 -> geological b + c1 yu1-3
- geological с2 ю1- 2 -> geological c2 yu1-2
- initial recoverable в+с1 ю1 -> initial recoverable b+c1 yu1
- initial recoverable с2 ю1-2 -> initial recoverable c2 yu1-2
- remaining recoverable в+с1 -> remaining recoverable b+c1
- remaining recoverable с2 ю1- -> remaining recoverable c2 yu1-

---

### E4. Distinct units values (original)
44 distinct original unit types. Informational only - not used in calculations. No action needed.

---

### E5. Distinct units_converted values
Same problem as D4.

| units_converted | row_count | pct |
|---|---|---|
| million bbl | 3,934 | 55.72 |
| million boe | 221 | 3.13 |
| million m³ | 2,905 | 41.15 |

41.15% of reserves records are in million m³ and 3.13% in million boe. Using quantity_converted directly without normalisation would produce wrong reserves destruction figures for 44% of records.

**Decision E5 - units normalisation**: Add a conversion step in the intermediate layer to normalise all reserves records to million bbl using the following conversion factors:
- million m³ x 6.29 = million bbl
- million boe x 1 = million bbl

A new column `quantity_converted_mbbl` will be added in the intermediate model. The original `quantity_converted` and `units_converted` columns are retained for traceability.

---

## Canonical geography mapping summary

The geography mapping seed contains only the 11 values that require remapping. All other geography values are already in their canonical form after standardisation and are not included in the seed. All canonical values are lowercase.

| raw_value | canonical_value |
|---|---|
| bahamas, the | the bahamas |
| virgin islands, u.s. | virgin islands |
| burma | myanmar |
| congo (brazzaville) | republic of the congo |
| congo (kinshasa) | democratic republic of the congo |
| gambia, the | the gambia |
| korea, south | south korea |
| micronesia, federated states of | micronesia |
| northern mariana | northern mariana islands |
| türkiye | turkey |
| côte d'ivoire | cote divoire |

The four DMDC special codes (us_zz-unknown, overseas_zz-unknown, armed forces europe, armed forces pacific) are not in the seed as they already map to themselves. The intermediate layer handles them as unresolvable based on their known values.

---

## Open Decisions

| # | Decision | Informed by | Status |
|---|---|---|---|
| 1 | Case normalisation via geography mapping seed | A1, B1 | Closed |
| 2 | Special codes retained with null geo_key | B1 | Closed |
| 3 | Northern Mariana Islands canonical | B1 | Closed |
| 4 | Virgin Islands canonical | B1 | Closed |
| 5 | Georgia, Guam, Puerto Rico grain includes location column | B1 | Closed |
| 6 | component canonical values confirmed | A2 | Closed |
| 7 | branch canonical values confirmed | B2 | Closed |
| 8 | component_category canonical values confirmed | B3 | Closed |
| 9 | fuel_description collapsed to 4 canonical values | D2, E2 | Closed |
| 10 | remaining reserves as primary classification for analytics tab | E3 | Closed |
| 11 | In-place volumes retained in analytics tab calculation | E3 | Closed |
| 12 | Cyrillic characters mapped to Latin equivalents | E3 | Closed |
| 13 | Case inconsistencies in reserves_classification resolved by standardised_txt layer | E3 | Closed |
| 14 | Production units normalised to million bbl/y in intermediate | D4 | Closed |
| 15 | Reserves units normalised to million bbl in intermediate | E5 | Closed |
| 16 | All canonical country names agreed | A1, B1, C1 | Closed |
| 17 | onshore_offshore canonical values confirmed | C7 | Closed |
| 18 | production_type canonical values confirmed | C4 | Closed |
| 19 | status canonical values confirmed | C5 | Closed |
| 20 | status_detail canonical values confirmed - 98.38% null, not suitable as dashboard filter | C6 | Closed |

{% enddocs %}
