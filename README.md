# GHCNh Data Processor

This module provides a Python class, `GHCNhProcessor`, designed to simplify the process of downloading, cleaning, processing, and resampling hourly station data from the Global Historical Climatology Network (GHCNh).

It handles the entire pipeline from finding stations and fetching raw data to applying quality control, intelligently combining different report types, and generating analysis-ready time series files.

## Features

- **Modular Design**: A `GHCNhProcessor` for data analysis and an internal `GHCNhDownloader` for robust networking and caching.
- **Advanced Hourly Processing**: Intelligently combines different report types (METAR/SPECI, SYNOP) into a single, clean hourly time series.
- **Automated Resampling**: Automatically generates and saves resampled time series (3-hourly, 6-hourly, daily, weekly, monthly, seasonal) in organized subdirectories.
- **Automated Data Caching**: Saves downloaded data to a local cache to prevent redundant downloads.
- **Robust Downloading**: Uses configurable timeouts to prevent hanging network requests.
- **Flexible Data Fetching**: Process data for a single year or a list of years, with parallel downloads for speed.
- **Automatic Station List Management**: Downloads the required station metadata list on the first run.
- **Configurable Quality Control**: Applies data quality control (QC) based on the flags provided in the dataset. Supports both `strict` and `lenient` QC levels.
- **Configurable Logging**: Uses Python's standard `logging` module, allowing for flexible output control.
- **Helper Utilities**: Includes tools to easily find stations based on metadata criteria (e.g., ICAO code, WMO ID, state).

## Usage

### 1. Initialization

First, import and create an instance of the `GHCNhProcessor`. The first time you run this, it will automatically download the master station list (`ghcnh-station-list.csv`) if it's not found. You can configure the cache directory, logging level, and download timeout.

```python
import logging
from ghcnh_processor import GHCNhProcessor

# Initialize the processor
processor = GHCNhProcessor(
    station_list_path='ghcnh-station-list.csv',
    cache_dir='.ghcnh_cache',
    log_level=logging.INFO, # Control verbosity
    download_timeout=60     # Seconds before a download times out
)
```

### 2. Finding a Station

You can use the `find_stations` method to locate stations of interest. For example, to find all stations in Texas that have an ICAO (airport) code:

```python
# Find all stations in Texas with an ICAO code
tx_airports = processor.find_stations(has_icao=True, state='TX')

if not tx_airports.empty:
    print("Found Texas airports:")
    print(tx_airports.head())
    # Get the ID of the first station (Houston Intercontinental)
    station_id = processor.get_ghcn_id_from_icao('KIAH')
else:
    print("No Texas airports found in the station list.")
    station_id = 'USW00012960' # Fallback to a known station
```

### 3. Processing Data (Recommended Workflow)

The `process_to_hourly` method is the primary entry point. It handles the entire pipeline: downloading, cleaning, combining report types, and saving the final hourly data along with resampled frequencies.

```python
# Process data for multiple years and save all outputs
combined_df, metar_df, synop_df = processor.process_to_hourly(
    station_id=station_id,
    years=[2022, 2023],
    qc_level='strict',
    # The save_path is used as a base for creating an organized output directory structure
    save_path=f'./output/{station_id}_2022-2023.csv'
)

if combined_df is not None:
    print("\nProcessing complete. Output files saved in the './output/' directory.")
    print("Final combined data sample:")
    print(combined_df.head())
```

This will create an `output` directory with the following structure:

```
output/
├── 1-hourly/
│   └── USW00012960_2022-2023_1-hourly.csv
├── 3-hourly/
│   └── USW00012960_2022-2023_3-hourly.csv
├── 6-hourly/
│   └── USW00012960_2022-2023_6-hourly.csv
├── daily/
│   └── ...
├── weekly/
│   └── ...
├── monthly/
│   └── ...
├── seasonal/
│   └── ...
└── raw-report-type/
    ├── USW00012960_2022-2023_metar.csv
    └── USW00012960_2022-2023_synop.csv
```

### 4. Basic Data Retrieval (Alternative)

If you only need the raw, quality-controlled data without the advanced hourly processing and resampling, you can use `get_station_years_data`.

```python
# Get cleaned data for a single year (2023)
data_2023 = processor.get_station_years_data(
    station_id=station_id,
    years=2023,
    qc_level='strict' # Use the strictest quality control
)

if data_2023 is not None:
    print("\nSample of cleaned 2023 data:")
    print(data_2023.head())
```
