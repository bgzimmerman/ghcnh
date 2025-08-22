# GHCNh Data Processor

This module provides a Python class, `GHCNhProcessor`, designed to simplify the process of downloading, cleaning, processing, and resampling hourly station data from the Global Historical Clology Network (GHCNh).

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
- **Data Quality Summary**: The main processing function returns a DataFrame summarizing the completeness of the data in every file it generates.

## Usage

### 1. Initialization

First, import and create an instance of the `GHCNhProcessor`. The first time you run this, it will automatically download the master station list (`ghcnh-station-list.csv`) if it's not found. You can configure the cache and output directories, logging level, and download timeout.

```python
import logging
from ghcnh_processor import GHCNhProcessor

# Initialize the processor
processor = GHCNhProcessor(
    station_list_path='ghcnh-station-list.csv',
    cache_dir='.ghcnh_cache',
    save_dir='./output', # Where all output files will be saved
    log_level=logging.INFO,
    download_timeout=60
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
    # Get the ID of a specific station (Houston Intercontinental)
    station_id = processor.get_ghcn_id_from_icao('KIAH')
else:
    print("No Texas airports found in the station list.")
    station_id = 'USW00012960' # Fallback to a known station
```

### 3. Processing Data and Getting a Summary

The `process_to_hourly` method is the primary entry point. It handles the entire pipeline: downloading, cleaning, combining report types, and saving the final hourly data along with resampled frequencies.

By default, it saves all generated files to the `save_dir` specified during initialization and returns a pandas DataFrame summarizing the data completeness for each file.

```python
# Process data for multiple years and get the summary report
summary_df = processor.process_to_hourly(
    station_id=station_id,
    years=[2022, 2023],
    qc_level='strict'
)

if summary_df is not None:
    print("\nProcessing complete. Data quality summary:")
    print(summary_df)
```

This will create an `output` directory with a full set of resampled files, and the `summary_df` will contain a report like this:

| StationID   | ICAO | File                                 | Resample Frequency | Start Year | End Year | temperature | dew_point_temperature | ... |
|-------------|------|--------------------------------------|--------------------|------------|----------|-------------|-----------------------|-----|
| USW00012960 | KIAH | USW00012960_2022-2023_1-hourly.csv   | 1-hourly           | 2022       | 2023     | 98.71       | 98.65                 | ... |
| USW00012960 | KIAH | USW00012960_2022-2023_3-hourly.csv   | 3-hourly           | 2022       | 2023     | 100.00      | 100.00                | ... |
| ...         | ...  | ...                                  | ...                | ...        | ...      | ...         | ...                   | ... |
| USW00012960 | KIAH | USW00012960_2022-2023_metar.csv      | raw-metar          | 2022       | 2023     | 98.60       | 98.54                 | ... |


### 4. Next Steps: Post-Processing

The output files and the summary report can be used to initialize the `GHCNPostProcessor` class, which is a placeholder for your own custom analysis, visualization, or data transformation workflows.

```python
from ghcnh_processor import GHCNPostProcessor

if summary_df is not None:
    post_processor = GHCNPostProcessor(
        summary_df=summary_df,
        data_dir='./output'
    )
    # Start your custom analysis
    # post_processor.example_analysis_method(station_id)
```
