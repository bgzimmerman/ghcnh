**GHCNh System Spec for Agentic Ingestion + QC Pipeline**

This summary provides a complete schema and processing guide for coding agents ingesting and performing quality control (QC) on the GHCNh dataset. All codes and metadata fields from the full documentation are included. This specification assumes hourly data in `.psv` or `.parquet` format.

---

## 1. File Access Structure

### Download Options:
- **Period-of-record (POR) file per station:** `GHCNh_<station>_por.psv`
- **Station-by-year file:** `GHCNh_<station>_<year>.psv`
- **Yearly archive (all stations):** tar.gz format

### Download Locations:
- Base: https://www.ncei.noaa.gov/oa/global-historical-climatology-network/index.html#hourly
- Station: `/access/by-station/`
- Year: `/access/by-year/<year>/`

---

## 2. Station Metadata
From `ghcnh-station-list.txt` or `.csv`

### Fields:
- `ID`: 11-char GHCN ID (2 char FIPS, 1 network, 8 char station code)
- `LATITUDE`, `LONGITUDE`, `ELEVATION`: float
- `STATE`: U.S. only
- `NAME`: Station name
- `GSN FLAG`: GSN or blank
- `HCN/CRN FLAG`: HCN, CRN, or blank
- `WMO ID`, `ICAO`: optional codes

---

## 3. Data Format

### File Types:
- `.psv`: Pipe-separated value, 1 header row
- `.parquet`: Efficient columnar format

### File Variants:
- POR: `Year`, `Month`, `Day`, `Hour`, `Minute`
- By-year: ISO `DATE` timestamp

### Each variable includes 6 fields:
- `<var>`: value
- `<var>_Measurement_Code`
- `<var>_Quality_Code`
- `<var>_Report_Type`
- `<var>_Source_Code`
- `<var>_Source_Station_ID`

---

## 4. Variables

### Core Meteorological:
- `temperature`, `dew_point_temperature`, `wet_bulb_temperature` (°C tenths)
- `station_level_pressure`, `sea_level_pressure` (hPa)
- `wind_direction` (degrees from true N), `wind_speed`, `wind_gust` (m/s)
- `precipitation` and multi-period versions (mm)
- `relative_humidity` (%), `altimeter` (hPa)
- `snow_depth` (mm), `visibility` (km)
- `pressure_3hr_change` (hPa)

### Sky & Weather Conditions:
- `sky_cover_1/2/3`, `sky_cover_baseht_1/2/3`
- `pres_wx_MW1/2/3`, `pres_wx_AU1/2/3`, `pres_wx_AW1/2/3`
- `remarks`

---

## 5. Measurement Codes

### Wind Direction/Speed:
- `H`: 5min avg
- `R`: 60min avg
- `C`: Calm
- `V`: Variable
- `A`, `B`, `N`, `Q`, `T`: see original doc
- `9`: Missing

### Pressure Tendency:
- `0–8`: Defined 3hr trends
- `4`: Steady
- `9`: Missing

### Precipitation:
- `2`: Trace
- `3`: Measurable
- `Z`: Assumed 0
- `E`: Estimated
- `XX-hr-accum`, `g`, `T`: carry-over codes

### Sky Cover:
- `00–08`: Oktas (CLR to OVC)
- `09`: Obscured
- `10`: Partial obscuration

### RH / Wet Bulb:
- `D`: Derived

### Visibility:
- `A–S`, `9`: Source-specific

### Snow Depth:
- `0–6`, `E`, `9`

---

## 6. Quality Control Codes

### Modern GHCNh (multi-source merge):
- `L`: Logical consistency
- `o`: Outlier
- `F`: Frequent value
- `U`: Diurnal inconsistency
- `D`, `d`: Distributions
- `W`: World record exceedance
- `K`: Streak
- `C`: Climatological outlier
- `T`: Timestamp
- `S`: Spike
- `h`, `V`, `w`, `N`, `E`, `p`, `H`

### Legacy QC (Sources 313–348):
- `0–1`: Good
- `2–3`: Suspect/Error
- `4–5`: Passed + from NCEI
- `6–7`: Suspect/Error from NCEI
- `A`: Accepted suspect
- `U`, `P`, `I`, `M`, `R`, `C`: Human/editorial edits

### Source 382 (C-HPD):
- `X`: Global extreme
- `N`: Negative precip
- `Y`: State extreme
- `K`: Frequent value
- `O`: Climatological outlier
- `Z`: Official flag
- `M`, `D`, `Q/q`, `R`, `A`: Special precipitation states

---

## 7. Report Types

### Common Codes (Table 4):
- `AUTO`: Automated
- `FM15`: METAR
- `FM16`: SPECI
- `FM12`: SYNOP
- `SAO`, `COOPD`, `CRN05`, `NSRDB`, etc.

### Source Flag Codes (Table 4a):
- `1–9`: Source families (USAF, NCEI, merged)
- `A–O`: Derived/validated/merged

Report code format: `<report>_<sourceflag>` (e.g., `AUTO_4-USA`)

---

## 8. Weather Code Descriptors

### `pres_wx_MW/AU/AW` Variables:
- Up to 3 values reported per type
- AU/AW: ASOS/AWOS automated, MW: manual
- Code ranges (e.g., 00–99) map to events like fog, smoke, snow, TS, hail, etc.
- Intensity modifiers and descriptors (e.g., `+`, `MI`, `TS`, `FZ`, `RA`, `SN`, `BR`, etc.)

Instruction: Maintain complete mapping of code-value pairs during ingestion.

---

## 9. Precipitation Interpretation (Important!)

For METAR-derived data:
- Use **last report of hour** (commonly minute 55) for `precipitation`
- Code `Pxxxx` shows total since last routine METAR
- Example:
  - P0065 at 13:15 UTC = 0.65 in = 16.5 mm
  - P0186 at 13:55 UTC = 1.86 in = total hourly accumulation
- Difference between last 2 reports in hour = intermediate precip

---

## 10. Columns in PSV Files

### By-station:
- `Year`, `Month`, `Day`, `Hour`, `Minute` (5 cols)
- Total columns: 238

### By-year:
- `DATE` (ISO), `LAT`, `LON`, `ELEV`, followed by variables
- Total columns: 234

Instruction: dynamically parse column headers to handle updates.

---

## 11. Processing Instructions for Coding Agents

1. **Parse** headers dynamically.
2. **Map** `<var>` + 5 metadata fields using table references.
3. **Convert** time to UTC-aware `datetime`.
4. **Decode** measurement and QC codes using static maps.
5. **Handle Precip:** retain only the final observation per hour for METAR-based stations.
6. **QC Filtering:**
   - Reject values with `QC` of `2`, `3`, or modern flags `o`, `S`, `W`, etc.
   - Retain `0`, `1`, or blank.
7. **Flag Special States:** measurement code = `E`, `Z`, `T`, etc. should be handled explicitly.
8. **Store** raw and cleaned values separately.
9. **Metadata join**: map to station metadata for lat/lon/elev/state as needed.

---

## 12. Reference Links
- Main page: https://www.ncei.noaa.gov/oa/global-historical-climatology-network/index.html#hourly
- Product info: https://www.ncei.noaa.gov/products/global-historical-climatology-network-hourly
- Station list: https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh-station-list.txt
- FMH1 report: https://www.icams-portal.gov/resources/ofcm/fmh/FMH1/fmh1_2019.pdf
- ASOS/AWOS: https://www.ncei.noaa.gov/products/land-based-station/automated-surface-weather-observing-systems

---

**End of Agent Spec** — this document contains all codes, variables, flags, and instructions to ingest, QC, and store GHCNh hourly weather data autonomously.

