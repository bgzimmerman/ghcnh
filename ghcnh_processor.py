import sys
import os
import urllib.request
import concurrent.futures
import logging
import shutil
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

    def __init__(self, station_list_path='ghcnh-station-list.csv', cache_dir='.ghcnh_cache', log_level=logging.INFO, download_timeout=60):
        """
        Initializes the processor by loading station metadata and setting up caching.

        If the station list file does not exist at the specified path, it will be
        downloaded automatically.

        Args:
            station_list_path (str): Path to the GHCNh station list CSV file.
            cache_dir (str): The directory to use for caching downloaded parquet files.
            log_level (int): The logging level for the logger (e.g., logging.INFO).
            download_timeout (int): Timeout in seconds for network download operations.
        """
        self.station_list_path = station_list_path
        self._setup_logger(log_level)
        self._download_station_list_if_missing()

        self.station_metadata = self._load_station_metadata()
        self.base_url = 'https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year'
        self.qc_summary = None
        self.cache_dir = cache_dir
        self.download_timeout = download_timeout
        if self.cache_dir:
            os.makedirs(self.cache_dir, exist_ok=True)
            self.logger.info(f"Using cache directory: {os.path.abspath(self.cache_dir)}")

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
        self.core_variables = [
            'temperature', 'dew_point_temperature', 'station_level_pressure', 
            'sea_level_pressure', 'wind_direction', 'wind_speed', 'wind_gust', 
            'precipitation', 'relative_humidity', 'wet_bulb_temperature'
        ]
        self.static_metadata_columns = [
            'Station_ID', 'Station_name', 'Latitude', 
            'Longitude', 'Elevation', 'ICAO'
        ]
        self.static_metadata_rename_dict = {
            'Station_ID': 'GHCN_ID', 
            'ICAO': 'station_code'
        }

    def _setup_logger(self, log_level):
        """Initializes a logger for the class instance."""
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.handlers:
            self.logger.setLevel(log_level)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def _download_station_list_if_missing(self):
        """Checks if the station list exists and downloads it if not."""
        if not os.path.exists(self.station_list_path):
            url = 'https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh-station-list.csv'
            self.logger.warning(f"Station list not found at '{self.station_list_path}'.")
            self.logger.info(f"Downloading from {url}...")
            
            try:
                # Ensure the directory exists before downloading
                dir_name = os.path.dirname(self.station_list_path)
                if dir_name:
                    os.makedirs(dir_name, exist_ok=True)
                
                urllib.request.urlretrieve(url, self.station_list_path)
                self.logger.info("Successfully downloaded station list.")
            except Exception as e:
                self.logger.error(f"Failed to download station list: {e}")
                # Set station_metadata to None if download fails
                self.station_metadata = None

    def _load_station_metadata(self):
        """Loads and preprocesses the station list file."""
        if not os.path.exists(self.station_list_path):
            self.logger.error(f"Station metadata file not found at {self.station_list_path}")
            return None
            
        df = pd.read_csv(self.station_list_path)
        df.set_index('GHCN_ID', inplace=True)
        return df

    def find_stations(self, has_icao=None, has_wmo_id=None, state=None, name_contains=None):
        """
        Finds stations based on metadata criteria.

        Args:
            has_icao (bool, optional): If True, returns only stations with an ICAO code.
                                       If False, returns stations without one. Defaults to None.
            has_wmo_id (bool, optional): If True, returns only stations with a WMO ID.
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
            if 'ICAO' in filtered_df.columns:
                if has_icao:
                    filtered_df = filtered_df[filtered_df['ICAO'].notna() & (filtered_df['ICAO'] != '')]
                else:
                    filtered_df = filtered_df[filtered_df['ICAO'].isna() | (filtered_df['ICAO'] == '')]
            elif has_icao:
                return pd.DataFrame(columns=filtered_df.columns)

        if has_wmo_id is not None:
            if 'WMO_ID' in filtered_df.columns:
                if has_wmo_id:
                    filtered_df = filtered_df[filtered_df['WMO_ID'].notna()]
                else:
                    filtered_df = filtered_df[filtered_df['WMO_ID'].isna()]
            elif has_wmo_id:
                return pd.DataFrame(columns=filtered_df.columns)

        if state:
            if 'STATE' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['STATE'].str.upper() == state.upper()]

        if name_contains:
            if 'NAME' in filtered_df.columns:
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
            self.logger.info(f"Attempting to download from: {parquet_url}")

            if not cache_path:
                self.logger.error("Caching is required but no cache directory is set.")
                return None
            
            try:
                # Use urlopen with a timeout to prevent hanging threads
                with urllib.request.urlopen(parquet_url, timeout=self.download_timeout) as response, open(cache_path, 'wb') as out_file:
                    shutil.copyfileobj(response, out_file)
                self.logger.info(f"Successfully downloaded and cached file to: {cache_path}")
            except urllib.error.HTTPError as e:
                # NCEI returns 404 if data for a station-year doesn't exist
                if e.code == 404:
                    self.logger.warning(f"No data found for station {station_id} for year {year} (404 Not Found).")
                else:
                    self.logger.error(f"Failed to download data for {station_id}, year {year}: {e}")
                return None
            except TimeoutError:
                self.logger.error(f"Download for {station_id}, year {year} timed out after {self.download_timeout} seconds.")
                return None
            except Exception as e:
                self.logger.error(f"An unexpected error occurred during download for {station_id}, year {year}: {e}")
                return None
        else:
            self.logger.info(f"Found cached file: {cache_path}")

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
                if 'ICAO' in station_info and pd.notna(station_info['ICAO']):
                    df['ICAO'] = station_info['ICAO']
            else:
                self.logger.warning(f"station {station_id} not found in metadata.")
                
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read or process parquet file {cache_path}: {e}")
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
                    self.logger.error(f'{year} generated an exception: {exc}')

        if not all_years_dfs:
            self.logger.warning(f"Could not retrieve any data for station {station_id} for the specified years.")
            return None

        # Sort by the 'DATE' index to ensure the combined dataframe is in chronological order
        return pd.concat(all_years_dfs).sort_index()

    def _get_variables_from_df(self, df):
        """Identifies core meteorological variables from the DataFrame columns, excluding 'remarks'."""
        qc_cols = [col for col in df.columns if col.endswith('_Quality_Code')]
        variables = [col.replace('_Quality_Code', '') for col in qc_cols]
        # Remarks are text-based metar reports - TODO: add python metar library to parse these
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
            self.logger.warning(f"No columns found for variable '{variable_name}'.")
            return pd.DataFrame()

        return df[existing_cols].copy()

    def quality_control(self, df, level='strict'):
        """
        Applies quality control to the data, setting flagged values to NaN.

        This method modifies the variable columns in-place. It also tracks
        the number of values flagged for each variable and stores it in the
        `qc_summary` attribute.

        Args:
            df (pd.DataFrame): The input DataFrame with station data.
            level (str): The QC level to apply ('strict' or 'lenient'). Defaults to 'strict'.

        Returns:
            pd.DataFrame: The DataFrame with quality-controlled values in the
                          original variable columns.
        """
        if level not in self.qc_flags_to_reject:
            raise ValueError(f"QC level must be one of: {list(self.qc_flags_to_reject.keys())}")

        flags_to_reject = self.qc_flags_to_reject[level]
        variables = self._get_variables_from_df(df)
        df_qc = df.copy()
        qc_counts = {}

        self.logger.info(f"Applying '{level}' QC to {len(variables)} variables.")

        for var in variables:
            qc_col = f"{var}_Quality_Code"

            num_flagged = 0
            if qc_col in df_qc.columns:
                bad_data_mask = df_qc[qc_col].isin(flags_to_reject)
                num_flagged = bad_data_mask.sum()
                # Overwrite original variable column with NaNs where QC flags are present
                df_qc.loc[bad_data_mask, var] = np.nan
            
            qc_counts[var] = num_flagged
        
        self.qc_summary = pd.Series(qc_counts).sort_values(ascending=False)
        
        self.logger.info("QC Summary: Number of values flagged for removal.")
        flagged_summary = self.qc_summary[self.qc_summary > 0]
        if not flagged_summary.empty:
            self.logger.info(f"\n{flagged_summary.to_string()}")
        else:
            self.logger.info("No values flagged for removal based on current QC level.")
            
        return df_qc

    def get_station_years_data(self, station_id, years, qc_level='strict', save_path=None):
        """
        High-level method to download and process data for one or more years.

        This is the primary method for fetching year-based data. It can handle
        a single year or a list of years.

        Args:
            station_id (str): The GHCN_ID of the station.
            years (int or list of int): A single year or a list of years to process.
            qc_level (str): The quality control level.
            save_path (str, optional): If provided, the final DataFrame will be saved
                                       to this path as a CSV file. Defaults to None.

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
        cleaned_data_cols = self._get_variables_from_df(df)
        
        final_cols = core_cols + cleaned_data_cols
        final_cols_exist = [col for col in final_cols if col in df_qc.columns]
        
        final_df = df_qc[final_cols_exist]

        if save_path:
            try:
                # Ensure the directory for the save path exists
                output_dir = os.path.dirname(save_path)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                
                final_df.to_csv(save_path)
                self.logger.info(f"Data successfully saved to {save_path}")
            except Exception as e:
                self.logger.error(f"Error: Failed to save data to {save_path}: {e}")

        return final_df

    def _resample_and_save(self, df, base_save_path, frequencies):
        """
        Resamples a DataFrame to specified frequencies and saves them to subdirectories.
        """
        freq_map = {
            '3-hourly': '3h', '6-hourly': '6h', '12-hourly': '12h',
            'daily': 'D', 'weekly': 'W', 'monthly': 'MS', 'seasonal': 'QS'
        }

        output_dir = os.path.dirname(base_save_path)
        base_name, ext = os.path.splitext(os.path.basename(base_save_path))

        # Identify static metadata columns and extract their values
        final_static_cols = list(self.static_metadata_rename_dict.values())
        final_static_cols.extend([
            col for col in self.static_metadata_columns
            if col not in self.static_metadata_rename_dict
        ])
        present_static_cols = [col for col in final_static_cols if col in df.columns]
        static_data = {}
        if not df.empty and present_static_cols:
            # Get the first valid value for each static column
            first_valid_row = df[present_static_cols].dropna()
            if not first_valid_row.empty:
                static_data = first_valid_row.iloc[0].to_dict()

        numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
        # Ensure we don't try to aggregate coordinate columns
        coord_cols = ['Latitude', 'Longitude', 'Elevation']
        numeric_cols = [col for col in numeric_cols if col not in coord_cols]

        if not numeric_cols:
            self.logger.warning("No numeric data columns found to resample.")
            return

        agg_dict = {col: 'mean' for col in numeric_cols}
        if 'precipitation' in agg_dict:
            agg_dict['precipitation'] = 'sum'

        for freq_name in frequencies:
            pd_freq = freq_map.get(freq_name)
            if not pd_freq:
                self.logger.warning(f"Unknown frequency '{freq_name}' requested. Skipping.")
                continue

            try:
                # Perform resampling
                resampled_df = df[numeric_cols].resample(pd_freq).agg(agg_dict)

                # Add static metadata back to the resampled DataFrame
                for col, val in static_data.items():
                    resampled_df[col] = val

                # Create subdirectory and new save path
                freq_dir = os.path.join(output_dir, freq_name)
                os.makedirs(freq_dir, exist_ok=True)
                new_save_path = os.path.join(freq_dir, f"{base_name}_{freq_name}{ext}")

                resampled_df.to_csv(new_save_path)
                self.logger.info(f"Successfully saved {freq_name} resampled data to: {new_save_path}")

            except Exception as e:
                self.logger.error(f"Error: Failed to resample or save for frequency '{freq_name}': {e}")

    def _resample_and_combine(self, df):
        """
        Internal helper to process raw data into a clean, hourly time series.
        """
        # Step A: Filter for core variables and their metadata
        cols_to_keep = self.core_variables[:]
        for var in self.core_variables:
            cols_to_keep.extend([f"{var}_Quality_Code", f"{var}_Report_Type"])
        
        # Also keep station metadata
        cols_to_keep.extend(self.static_metadata_columns)
        
        df_filtered = df[[col for col in cols_to_keep if col in df.columns]].copy()

        # Step B: Apply Quality Control (silently for this pipeline)
        df_qc = self.quality_control(df_filtered, level='strict')

        # Step C: Prioritize and Separate Data
        # Use temperature's report type as the primary one
        if 'temperature_Report_Type' in df_qc.columns:
            df_qc['Report_Type'] = df_qc['temperature_Report_Type']
        else:
            # Fallback if temperature report type is not available
            df_qc['Report_Type'] = 'UNKNOWN'

        metar_reports = ['FM15-METAR', 'FM16-SPECI']
        synop_reports = ['FM12-SYNOP']

        df_metar = df_qc[df_qc['Report_Type'].isin(metar_reports)]
        df_synop = df_qc[df_qc['Report_Type'].isin(synop_reports)]
        
        # Step D: Define Aggregations and Resample
        agg_rules = {
            'temperature': 'mean', 'dew_point_temperature': 'mean', 'station_level_pressure': 'mean',
            'sea_level_pressure': 'mean', 'wind_speed': 'mean', 'wind_gust': 'mean',
            'relative_humidity': 'mean', 'wet_bulb_temperature': 'mean',
            'wind_direction': 'last', 'precipitation': 'last' # Use .last() for precip from METARs
        }
        
        # Filter rules for columns that actually exist in the dataframe
        agg_rules_filtered = {k: v for k, v in agg_rules.items() if k in df_qc.columns}

        hourly_metar = df_metar.resample('1h').agg(agg_rules_filtered) if not df_metar.empty else pd.DataFrame()
        hourly_synop = df_synop.resample('1h').agg(agg_rules_filtered) if not df_synop.empty else pd.DataFrame()

        # Step E: Combine with priority
        # Start with METAR, then fill missing with SYNOP
        combined_df = hourly_metar.combine_first(hourly_synop)

        # Add back station metadata, which is static
        static_data = {}
        for col in self.static_metadata_columns:
            if col in df_qc.columns and not df_qc[col].empty:
                # Get the first valid value for the static column
                first_valid = df_qc[col].dropna().iloc[0] if not df_qc[col].dropna().empty else None
                if first_valid is not None:
                    static_data[col] = first_valid

        for col, val in static_data.items():
            combined_df[col] = val
            if not hourly_metar.empty:
                hourly_metar[col] = val
            if not hourly_synop.empty:
                hourly_synop[col] = val

        # Rename columns to match user's preference
        rename_dict = {'Station_ID': 'GHCN_ID', 'ICAO': 'station_code'}
        combined_df.rename(columns=rename_dict, inplace=True)
        if not hourly_metar.empty:
            hourly_metar.rename(columns=rename_dict, inplace=True)
        if not hourly_synop.empty:
            hourly_synop.rename(columns=rename_dict, inplace=True)

        return combined_df, hourly_metar, hourly_synop
        
    def process_to_hourly(self, station_id, years, qc_level='strict', save_path=None, resample_frequencies=None):
        """
        Downloads, cleans, and processes data into a final hourly time series,
        intelligently combining different report types. If a save_path is provided,
        this function also generates and saves resampled datasets for all standard
        frequencies by default.

        Args:
            station_id (str): The GHCN_ID of the station.
            years (int or list of int): A single year or a list of years to process.
            qc_level (str): The quality control level.
            save_path (str, optional): If provided, the final hourly DataFrame and any
                                       resampled DataFrames will be saved. Defaults to None.
            resample_frequencies (list, optional): A list of frequencies to resample to.
                                                   Defaults to all available frequencies.
                                                   To disable resampling when saving, pass an empty list: [].
        """
        # If no frequencies are specified, default to all of them.
        if resample_frequencies is None:
            resample_frequencies = [
                '3-hourly', '6-hourly', '12-hourly', 'daily', 
                'weekly', 'monthly', 'seasonal'
            ]

        # Download all raw data first
        raw_df = self.download_years_data(station_id, years)
        if raw_df is None:
            self.logger.error(f"No data found for station {station_id}. Aborting.")
            return None, None, None

        # Process and resample
        combined_df, metar_df, synop_df = self._resample_and_combine(raw_df)

        # Save primary and intermediate files if a path is provided
        if save_path:
            try:
                # Deconstruct the user-provided path to get the directory and base filename
                output_dir = os.path.dirname(save_path)
                base_name, ext = os.path.splitext(os.path.basename(save_path))

                # Create subdirectories for organized output
                hourly_dir = os.path.join(output_dir, '1-hourly')
                report_type_dir = os.path.join(output_dir, 'raw-report-type')
                os.makedirs(hourly_dir, exist_ok=True)
                os.makedirs(report_type_dir, exist_ok=True)

                # --- Save the main 1-hourly file ---
                hourly_save_path = os.path.join(hourly_dir, f"{base_name}_1-hourly{ext}")
                combined_df.to_csv(hourly_save_path)
                self.logger.info(f"Successfully saved combined 1-hourly data to: {hourly_save_path}")

                # --- Save the intermediate report-type files ---
                metar_path = os.path.join(report_type_dir, f"{base_name}_metar{ext}")
                synop_path = os.path.join(report_type_dir, f"{base_name}_synop{ext}")
                
                if not metar_df.empty:
                    metar_df.to_csv(metar_path)
                    self.logger.info(f"Successfully saved METAR data to: {metar_path}")
                
                if not synop_df.empty:
                    synop_df.to_csv(synop_path)
                    self.logger.info(f"Successfully saved SYNOP data to: {synop_path}")

            except Exception as e:
                self.logger.error(f"Error during file saving: {e}")

        # Perform and save additional resampling if requested
        if save_path and resample_frequencies:
            self._resample_and_save(combined_df, save_path, resample_frequencies)

        return combined_df, metar_df, synop_df

    def get_ghcn_id_from_icao(self, icao_code):
        result = self.find_stations(has_icao=True)
        match = result[result['ICAO'] == icao_code]
        return match.index[0] if not match.empty else None