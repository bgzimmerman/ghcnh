import sys
import os
import urllib.request
import concurrent.futures
import numpy as np
import pandas as pd


class GHCNhProcessor:
    """
    A class to download, process, and quality-control GHCN-hourly data.

    This class provides a structured way to interact with the GHCNh dataset,
    automating the download, metadata integration, quality control, and
    final processing steps.

    Attributes:
        station_metadata (pd.DataFrame): A DataFrame containing metadata for all stations.
        base_url (str): The base URL for accessing the NCEI data repository.
        qc_flags_to_reject (dict): A dictionary mapping QC levels ('strict', 'lenient')
                                   to lists of QC flags that should be rejected.
    """

    def __init__(self, station_list_path='ghcnh-station-list.csv', cache_dir='.ghcnh_cache'):
        """
        Initializes the processor by loading station metadata and setting up caching.

        If the station list file does not exist at the specified path, it will be
        downloaded automatically.

        Args:
            station_list_path (str): Path to the GHCNh station list CSV file.
            cache_dir (str): The directory to use for caching downloaded parquet files.
        """
        self.station_list_path = station_list_path
        self._download_station_list_if_missing()

        self.station_metadata = self._load_station_metadata()
        self.base_url = 'https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year'
        self.qc_summary = None
        self.cache_dir = cache_dir
        if self.cache_dir:
            os.makedirs(self.cache_dir, exist_ok=True)
            print(f"Using cache directory: {os.path.abspath(self.cache_dir)}")

        self.qc_flags_to_reject = {
            'strict': [
                # Modern QC Flags (Section 6, GHCNh Agent Spec)
                'L', 'o', 'F', 'U', 'D', 'd', 'W', 'K', 'C', 'T', 'S',
                'h', 'V', 'w', 'N', 'E', 'p', 'H',
                # Legacy QC Flags
                '2', '3', '6', '7',
                # Source-specific C-HPD Flags
                'X', 'N', 'Y', 'K', 'O', 'Z', 'M', 'D', 'Q', 'q', 'R', 'A'
            ],
            'lenient': [
                'W',  # World record exceedance
                'S',  # Spike
                'X',  # Global extreme
                'o'   # Outlier
            ]
        }

    def _download_station_list_if_missing(self):
        """Checks if the station list exists and downloads it if not."""
        if not os.path.exists(self.station_list_path):
            url = 'https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh-station-list.csv'
            print(f"Station list not found at '{self.station_list_path}'.")
            print(f"Downloading from {url}...")
            
            try:
                # Ensure the directory exists before downloading
                dir_name = os.path.dirname(self.station_list_path)
                if dir_name:
                    os.makedirs(dir_name, exist_ok=True)
                
                urllib.request.urlretrieve(url, self.station_list_path)
                print("Successfully downloaded station list.")
            except Exception as e:
                print(f"Error: Failed to download station list: {e}", file=sys.stderr)
                # Set station_metadata to None if download fails
                self.station_metadata = None

    def _load_station_metadata(self):
        """Loads and preprocesses the station list file."""
        if not os.path.exists(self.station_list_path):
            print(f"Error: Station metadata file not found at {self.station_list_path}", file=sys.stderr)
            return None
            
        df = pd.read_csv(self.station_list_path)
        df.columns = df.columns.str.replace(r'[\(\)-]', '', regex=True).str.replace('__', '_')
        df.set_index('GHCN_ID', inplace=True)
        if 'ICAO' in df.columns:
            df['ICAO'] = df['ICAO'].str.strip()
        return df


    def find_stations(self, has_icao=None, state=None, name_contains=None):
        """
        Finds stations based on metadata criteria.

        Args:
            has_icao (bool, optional): If True, returns only stations with an ICAO code.
                                       If False, returns stations without one. Defaults to None.
            state (str, optional): Filters by 2-letter state abbreviation. Defaults to None.
            name_contains (str, optional): Filters by a string contained in the station name (case-insensitive).
                                           Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame of matching stations, or None if metadata is not loaded.
        """
        if self.station_metadata is None:
            return None

        filtered_df = self.station_metadata.copy()
        if has_icao is not None:
            if has_icao:
                filtered_df = filtered_df[filtered_df['ICAO'].notna() & (filtered_df['ICAO'] != '')]
            else:
                filtered_df = filtered_df[filtered_df['ICAO'].isna() | (filtered_df['ICAO'] == '')]
        if state:
            filtered_df = filtered_df[filtered_df['STATE'].str.upper() == state.upper()]
        if name_contains:
            filtered_df = filtered_df[filtered_df['NAME'].str.contains(name_contains, case=False)]

        return filtered_df

    def _download_year_data(self, station_id, year):
        """
        Downloads data for a single station and year, using a local cache to avoid re-downloads.

        This is an internal helper method.

        Args:
            station_id (str): The GHCN_ID of the station.
            year (int): The year to download data for.

        Returns:
            pd.DataFrame or None: A DataFrame with the station's data for that year,
                                  or None if the download or read fails.
        """
        file_name = f"GHCNh_{station_id}_{year}.parquet"
        cache_path = os.path.join(self.cache_dir, file_name) if self.cache_dir else None

        # Step 1: Ensure the parquet file is available locally (from cache or download)
        if not (cache_path and os.path.exists(cache_path)):
            parquet_url = f"{self.base_url}/{year}/parquet/{file_name}"
            print(f"Attempting to download from: {parquet_url}")

            if not cache_path:
                print("Error: Caching is required but no cache directory is set.", file=sys.stderr)
                return None
            
            try:
                urllib.request.urlretrieve(parquet_url, cache_path)
                print(f"Successfully downloaded and cached file to: {cache_path}")
            except urllib.error.HTTPError as e:
                # NCEI returns 404 if data for a station-year doesn't exist
                if e.code == 404:
                    print(f"No data found for station {station_id} for year {year} (404 Not Found).", file=sys.stderr)
                else:
                    print(f"Failed to download data for {station_id}, year {year}: {e}", file=sys.stderr)
                return None
            except Exception as e:
                print(f"An unexpected error occurred during download for {station_id}, year {year}: {e}", file=sys.stderr)
                return None
        else:
            print(f"Found cached file: {cache_path}")

        # Step 2: Read the local parquet file into a DataFrame
        try:
            df = pd.read_parquet(cache_path)

            # Convert to datetime and set as index
            if 'DATE' in df.columns:
                df['DATE'] = pd.to_datetime(df['DATE'], utc=True)
                df.set_index('DATE', inplace=True)
            
            # Fill metadata from station list
            df['Station_ID'] = station_id
            if self.station_metadata is not None and station_id in self.station_metadata.index:
                station_info = self.station_metadata.loc[station_id]
                df['Station_name'] = station_info['NAME']
                df['Latitude'] = station_info['LATITUDE']
                df['Longitude'] = station_info['LONGITUDE']
                df['Elevation'] = station_info['ELEVATION']
            else:
                print(f"Warning: station {station_id} not found in metadata.", file=sys.stderr)
                
            return df
            
        except Exception as e:
            print(f"Failed to read or process parquet file {cache_path}: {e}", file=sys.stderr)
            # Optional: could remove the corrupted cached file here
            # os.remove(cache_path)
            return None

    def download_years_data(self, station_id, years):
        """
        Downloads and concatenates data for a station over a list of years using parallel threads.

        This method uses a ThreadPoolExecutor to download multiple yearly files concurrently,
        significantly speeding up the process for multi-year requests.

        Args:
            station_id (str): The GHCN_ID of the station.
            years (int or list of int): A single year or a list of years to download.

        Returns:
            pd.DataFrame or None: A DataFrame containing data for all specified
                                  years, or None if no data could be retrieved.
        """
        if isinstance(years, int):
            years = [years]

        all_years_dfs = []
        # Use a ThreadPoolExecutor to download years in parallel
        # This is ideal for I/O-bound tasks like downloading files
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Create a future for each year's download
            future_to_year = {executor.submit(self._download_year_data, station_id, year): year for year in years}
            for future in concurrent.futures.as_completed(future_to_year):
                year = future_to_year[future]
                try:
                    year_df = future.result()
                    if year_df is not None:
                        all_years_dfs.append(year_df)
                except Exception as exc:
                    print(f'{year} generated an exception: {exc}', file=sys.stderr)

        if not all_years_dfs:
            print(f"Warning: Could not retrieve any data for station {station_id} for the specified years.", file=sys.stderr)
            return None

        # Sort by the 'DATE' index to ensure the combined dataframe is in chronological order
        return pd.concat(all_years_dfs).sort_index()

    def _get_variables_from_df(self, df):
        """Identifies core meteorological variables from the DataFrame columns, excluding 'remarks'."""
        qc_cols = [col for col in df.columns if col.endswith('_Quality_Code')]
        variables = [col.replace('_Quality_Code', '') for col in qc_cols]
        # Remarks are text-based and do not undergo the same QC process.
        return [var for var in variables if var != 'remarks']

    def get_variable_details(self, df, variable_name):
        """
        Extracts all related columns for a single variable from a DataFrame.

        This helper function is useful for inspecting all the metadata associated
        with a specific variable (e.g., its value, quality code, source code).

        Args:
            df (pd.DataFrame): The DataFrame containing the raw station data.
            variable_name (str): The core name of the variable (e.g., 'temperature').

        Returns:
            pd.DataFrame: A DataFrame containing only the columns related to the
                          specified variable, or an empty DataFrame if none are found.
        """
        related_cols = [variable_name]
        suffixes = [
            '_Measurement_Code',
            '_Quality_Code',
            '_Report_Type',
            '_Source_Code',
            '_Source_Station_ID'
        ]

        for suffix in suffixes:
            related_cols.append(f"{variable_name}{suffix}")

        existing_cols = [col for col in related_cols if col in df.columns]

        if not existing_cols:
            print(f"Warning: No columns found for variable '{variable_name}'.", file=sys.stderr)
            return pd.DataFrame()

        return df[existing_cols].copy()

    def quality_control(self, df, level='strict'):
        """
        Applies quality control to the data, setting flagged values to NaN.

        This method creates new columns with a '_clean' suffix for QC'd data,
        preserving the original values. It also tracks the number of values
        flagged for each variable and stores it in the `qc_summary` attribute.

        Args:
            df (pd.DataFrame): The input DataFrame with station data.
            level (str): The QC level to apply ('strict' or 'lenient'). Defaults to 'strict'.

        Returns:
            pd.DataFrame: The DataFrame with added '_clean' columns.
        """
        if level not in self.qc_flags_to_reject:
            raise ValueError(f"QC level must be one of: {list(self.qc_flags_to_reject.keys())}")

        flags_to_reject = self.qc_flags_to_reject[level]
        variables = self._get_variables_from_df(df)
        df_qc = df.copy()
        qc_counts = {}

        print(f"Applying '{level}' QC to {len(variables)} variables.")

        for var in variables:
            clean_col = f"{var}_clean"
            qc_col = f"{var}_Quality_Code"
            df_qc[clean_col] = df_qc[var]

            num_flagged = 0
            if qc_col in df_qc.columns:
                bad_data_mask = df_qc[qc_col].isin(flags_to_reject)
                num_flagged = bad_data_mask.sum()
                df_qc.loc[bad_data_mask, clean_col] = np.nan
            
            qc_counts[var] = num_flagged
        
        self.qc_summary = pd.Series(qc_counts).sort_values(ascending=False)
        
        print("QC Summary: Number of values flagged for removal.")
        flagged_summary = self.qc_summary[self.qc_summary > 0]
        if not flagged_summary.empty:
            print(flagged_summary)
        else:
            print("No values flagged for removal based on current QC level.")
            
        return df_qc

    def get_station_years_data(self, station_id, years, qc_level='strict'):
        """
        High-level method to download and process data for one or more years.

        This is the primary method for fetching year-based data. It can handle
        a single year or a list of years.

        Args:
            station_id (str): The GHCN_ID of the station.
            years (int or list of int): A single year or a list of years to process.
            qc_level (str): The quality control level.

        Returns:
            pd.DataFrame or None: A cleaned DataFrame for the specified years.
        """
        if isinstance(years, int):
            years = [years]

        df = self.download_years_data(station_id, years)
        if df is None:
            return None

        df_qc = self.quality_control(df, level=qc_level)
        
        core_cols = ['Station_ID', 'Station_name', 'Latitude', 'Longitude', 'Elevation', 'remarks']
        cleaned_data_cols = [f"{var}_clean" for var in self._get_variables_from_df(df)]
        
        final_cols = core_cols + cleaned_data_cols
        final_cols_exist = [col for col in final_cols if col in df_qc.columns]
        
        return df_qc[final_cols_exist]


if __name__ == '__main__':
    # This is an example of how to use the class
    
    # Initialize the processor
    processor = GHCNhProcessor(
        station_list_path='ghcnh-station-list.csv',
        cache_dir='ghcnh_data_cache' # Use a specific cache directory for the example
    )

    # --- Find a station ---
    # Find stations in Texas with ICAO codes
    tx_airports = processor.find_stations(has_icao=True, state='TX')
    
    station_to_process = None
    year_to_process = 2023

    if tx_airports is not None and not tx_airports.empty:
        print("Found Texas airports:")
        print(tx_airports.head())
        # Let's pick one station to process, e.g., the first one
        station_to_process = tx_airports.index[0]
    else:
        # As a fallback if no TX airports are in the short list, use a known station
        print("Texas airport not in the short station list, using a default station for demonstration.")
        station_to_process = 'AEI0000OMAA' # Abu Dhabi Intl, from the short list
    
    if station_to_process:
        # --- Demonstrate getting raw data and inspecting a variable ---
        year_to_inspect = 2023
        print(f"\nDownloading raw data for {station_to_process} for {year_to_inspect} to inspect 'temperature' variable...")
        raw_data = processor.download_years_data(station_to_process, year_to_inspect)
        if raw_data is not None:
            temp_details = processor.get_variable_details(raw_data, 'temperature')
            print("\nDetails for 'temperature' variable (raw data):")
            print(temp_details.head())

        # --- Get and process data for that station using the main by-year pipeline ---
        print(f"\nProcessing data for station {station_to_process} for year {year_to_inspect}...")
        processed_data = processor.get_station_years_data(
            station_id=station_to_process,
            years=year_to_inspect,
            qc_level='strict'
        )

        if processed_data is not None:
            print("\nSuccessfully processed data for a single year. Here's a sample:")
            print(processed_data.head())
            
            # Save to a file
            output_file = f"./{station_to_process}_{year_to_inspect}_clean.csv"
            processed_data.to_csv(output_file)
            print(f"\nCleaned single-year data saved to {output_file}")
            
        # --- Demonstrate the multi-year download functionality ---
        print(f"\n--- Now demonstrating multi-year download for {station_to_process} (2021-2023) ---")
        years_to_process = [2021, 2022, 2023]
        multi_year_data = processor.get_station_years_data(
            station_id=station_to_process,
            years=years_to_process,
            qc_level='strict'
        )
        if multi_year_data is not None:
            print("\nSuccessfully processed multi-year data. Here's a sample:")
            print(multi_year_data.head())
            
            start_date = multi_year_data.index.min().strftime('%Y-%m-%d')
            end_date = multi_year_data.index.max().strftime('%Y-%m-%d')
            print(f"\nMulti-year data covers from {start_date} to {end_date}")
            
            output_file = f"./{station_to_process}_{years_to_process[0]}-{years_to_process[-1]}_clean.csv"
            multi_year_data.to_csv(output_file)
            print(f"\nCleaned multi-year data saved to {output_file}")
