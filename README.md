# GHCNh Data Processor

This module provides a Python class, `GHCNhProcessor`, designed to simplify the process of downloading, cleaning, and quality-controlling hourly station data from the Global Historical Climatology Network (GHCNh).

## Features

- **Automated Data Caching**: Automatically saves downloaded data to a local cache to prevent redundant downloads.
- **Flexible Data Fetching**: Download data for a single year or a list of years.
- **Automatic Station List Management**: Downloads the required station metadata list on the first run.
- **Configurable Quality Control**: Applies data quality control (QC) based on the flags provided in the dataset. Supports both `strict` and `lenient` QC levels.
- **Helper Utilities**: Includes tools to easily find stations based on metadata criteria (e.g., ICAO code, WMO ID, state).
- **Silent Operation**: Verbosity can be turned off for quiet execution in automated scripts.
- **Direct-to-CSV Saving**: Save cleaned data directly to a CSV file as part of the processing pipeline.

## Usage

### 1. Initialization

First, import and create an instance of the `GHCNhProcessor`. The first time you run this, it will automatically download the master station list (`ghcnh-station-list.csv`) if it's not found.

```python
from ghcnh_processor import GHCNhProcessor

# Initialize the processor
processor = GHCNhProcessor(
    station_list_path='/scratch/qws-data/csv/ghcnh-station-list.csv',
    cache_dir = '/scratch/qws-data/ghcnh_data_cache')
```

### 2. Finding a Station

You can use the `find_stations` method to locate stations of interest. For example, to find all stations in Texas that have an ICAO (airport) code:

```python
# Find all stations in Texas with an ICAO code
tx_airports = processor.find_stations(has_icao=True, state='TX')

if not tx_airports.empty:
    print("Found Texas airports:")
    print(tx_airports.head())
    station_id = tx_airports.index[0] # Get the ID of the first station
else:
    print("No Texas airports found in the station list.")
    station_id = 'USW00012921' # Fallback to a known station (Houston)
```

### 3. Downloading and Processing Data

The main method for fetching and cleaning data is `get_station_years_data`. It's flexible and can handle single or multiple years.

#### Get Data for a Single Year

To get data for a single year, pass the year as an list[int].

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

#### Get Data for Multiple Years and Save to File

To get data for a range of years, pass a list of integers. You can also use the `save_path` argument to save the output directly to a CSV file.

```python
# Get data for multiple years and save it directly to a file
multi_year_data = processor.get_station_years_data(
    station_id=station_id,
    years=[2021, 2022, 2023],
    qc_level='lenient', # Use less strict QC
    verbose=False, # Run silently
    save_path=f'./output/{station_id}_2021-2023.csv'
)

if multi_year_data is not None:
    print("\nMulti-year data processed and saved.")
    print("Date Range:", multi_year_data.index.min(), "to", multi_year_data.index.max())
```
