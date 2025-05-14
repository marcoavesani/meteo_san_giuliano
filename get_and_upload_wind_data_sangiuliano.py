import os
import re
import json
from datetime import datetime, timedelta # timedelta not used in this script, but kept from original
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
import requests

# --- Constants ---
TIMEZONE = ZoneInfo("Europe/Rome")
SOURCE_URL = 'https://meteo-venezia.net/'

GITHUB_DATA_URL_TEMPLATE = "https://raw.githubusercontent.com/marcoavesani/meteo_san_giuliano/master/data/measurements/measured_wind_venice_{month}_{year}.csv"
LOCAL_DATA_DIR = "./data/measurements"
LOCAL_DATA_FILE_TEMPLATE = "measured_wind_venice_{month}_{year}.csv"

# Columns to drop from scraped data
COLUMNS_TO_DROP = ['xtmp', 'ytmp', 'ygusttmp', 'x']

# Column rename mapping
COLUMN_RENAME_MAP = {
    "t": "time_measured",
    "y": "wind_speed_measured",
    "ygust": "wind_gust_measured",
    "dir": "wind_direction_measured",
    "dirdegree": "wind_direction_degree_measured"
}

# Desired final column order
FINAL_COLUMN_ORDER = [
    'time_measured', 'wind_speed_measured', 'wind_gust_measured',
    'wind_direction_measured', 'wind_direction_degree_measured'
]

# --- Functions ---

def get_wind_data_today(debug: bool = False) -> pd.DataFrame:
    """
    Scrapes wind data from the meteo-venezia.net website.
    The data is embedded in a JavaScript variable 'dataPoints'.
    """
    try:
        response = requests.get(SOURCE_URL)
        response.raise_for_status()  # Raise an exception for HTTP errors
        html_data = response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {SOURCE_URL}: {e}")
        return pd.DataFrame()

    # Regex to find the 'dataPoints' array assignment.
    # It looks for 'dataPoints = [...]' and captures the content of the array.
    # re.DOTALL allows '.' to match newlines, in case the array spans multiple lines.
    match = re.search(r'dataPoints\s*=\s*(\[[\s\S]*?\])\s*;', html_data, re.DOTALL)

    if not match:
        if debug:
            print("Could not find 'dataPoints' array in the HTML content.")
        return pd.DataFrame()

    js_array_string = match.group(1)
    if debug:
        print(f"--- Extracted JavaScript array string (first 200 chars): ---\n{js_array_string[:200]}...")

    # Convert the JavaScript-like array string to a valid JSON string:
    # 1. Quote unquoted keys (e.g., x: -> "x":)
    #    This regex looks for patterns like '{key:', ',key:', '[key:'
    #    and adds quotes around the key.
    json_string = re.sub(r'([{,\[]\s*)(\w+)(\s*:)', r'\1"\2"\3', js_array_string)

    # 2. Handle 'new Date(timestamp)' by replacing it with just the timestamp number.
    #    e.g., "x": new Date(1234567890000) -> "x": 1234567890000
    json_string = re.sub(r'new Date\((\d+)\)', r'\1', json_string)
    
    # 3. Ensure any remaining single quotes around values (if any from odd source formatting) become double quotes
    #    This step might be optional depending on the exact source format variability
    #    For example, if a string value was t: '00:00:00' instead of t: "00:00:00"
    # json_string = json_string.replace("'", '"') # Use with caution, might break valid strings with apostrophes

    if debug:
        print(f"\n--- Transformed JSON string (first 200 chars): ---\n{json_string[:200]}...")

    try:
        data_list = json.loads(json_string)
        df = pd.DataFrame(data_list)
        if debug:
            print("\n--- Successfully parsed data into DataFrame: ---")
            print(df.head())
        return df
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        if debug:
            print(f"Problematic JSON string part: {json_string[e.pos-20:e.pos+20] if e.pos > 20 else json_string[:e.pos+20]}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred during DataFrame creation: {e}")
        return pd.DataFrame()


def process_wind_data(df: pd.DataFrame, current_date_str: str, debug: bool = False) -> pd.DataFrame:
    """Processes the raw scraped wind data DataFrame."""
    if df.empty:
        return pd.DataFrame()

    # Make a copy to avoid SettingWithCopyWarning
    df_processed = df.copy()

    # Drop unnecessary columns
    cols_to_drop_existing = [col for col in COLUMNS_TO_DROP if col in df_processed.columns]
    df_processed = df_processed.drop(columns=cols_to_drop_existing)

    if 't' not in df_processed.columns:
        if debug: print("Column 't' not found for time processing.")
        return pd.DataFrame() # Cannot proceed without time column

    # Concatenate current date with time column and convert to datetime
    # Assuming 't' contains time strings like "HH:MM:SS"
    try:
        df_processed['t'] = current_date_str + ' ' + df_processed['t'].astype(str)
        df_processed['t'] = pd.to_datetime(df_processed['t'], format="%Y-%m-%d %H:%M:%S")
    except Exception as e:
        if debug: print(f"Error converting 't' column to datetime: {e}")
        # Optionally, drop rows that couldn't be converted or return empty
        return pd.DataFrame()


    # Set data types
    if 'dir' in df_processed.columns:
        df_processed['dir'] = df_processed['dir'].astype(pd.StringDtype())
    if 'dirdegree' in df_processed.columns:
        # Handle potential non-numeric values before astype(int)
        df_processed['dirdegree'] = pd.to_numeric(df_processed['dirdegree'], errors='coerce').fillna(0).astype(np.int64)


    # Rename columns
    df_processed = df_processed.rename(columns=COLUMN_RENAME_MAP)

    # Reorder columns, only including those that exist after renaming
    existing_final_columns = [col for col in FINAL_COLUMN_ORDER if col in df_processed.columns]
    df_processed = df_processed[existing_final_columns]

    if debug:
        print("\n--- Processed DataFrame: ---")
        print(df_processed.head())
        print(df_processed.info())
    return df_processed


def get_old_data_github(current_dt: datetime) -> pd.DataFrame:
    """
    Fetches previously measured data from GitHub.
    Returns an empty DataFrame if the file is not found or an error occurs.
    """
    file_url = GITHUB_DATA_URL_TEMPLATE.format(
        month=current_dt.month,
        year=current_dt.year
    )
    try:
        # Assuming CSVs saved with index=False, so no index_col needed for reading.
        # If they were saved with an index, use index_col=0.
        df_old = pd.read_csv(
            file_url,
            parse_dates=['time_measured'], # Assuming 'time_measured' is the correct column name
            date_format="%Y-%m-%d %H:%M:%S" # Be explicit if format is known
        )
        print(f"Found and opened old file: {file_url}")

        # Ensure correct dtypes after loading
        if 'time_measured' in df_old.columns and not pd.api.types.is_datetime64_any_dtype(df_old['time_measured']):
             df_old['time_measured'] = pd.to_datetime(df_old['time_measured'], format="%Y-%m-%d %H:%M:%S")
        if 'wind_direction_measured' in df_old.columns:
            df_old['wind_direction_measured'] = df_old['wind_direction_measured'].astype(pd.StringDtype())
        return df_old
    except pd.errors.EmptyDataError:
        print(f"No data in file (or file not found): {file_url}. Creating a new DataFrame.")
    except requests.exceptions.HTTPError as e: # More specific for URL errors (like 404)
        if e.response.status_code == 404:
            print(f"File not found on GitHub (404): {file_url}. Creating a new DataFrame.")
        else:
            print(f"HTTP error fetching {file_url}: {e}. Creating a new DataFrame.")
    except FileNotFoundError: # For local file reads, less likely here.
        print(f"File not found (locally, unexpected for URL): {file_url}. Creating a new DataFrame.")
    except Exception as e:
        print(f"Error reading {file_url}: {e}. Creating a new DataFrame.")
    return pd.DataFrame()


# --- Main Execution ---
if __name__ == "__main__":
    debug_mode = True # Set to False for less verbose output

    print(f"Running script at {datetime.now(TIMEZONE)}")
    current_datetime = datetime.now(TIMEZONE)
    current_date_string = current_datetime.strftime('%Y-%m-%d')

    print("\nFetching current wind data...")
    df_new_raw = get_wind_data_today(debug=debug_mode)

    if not df_new_raw.empty:
        print("\nProcessing fetched wind data...")
        df_new_processed = process_wind_data(df_new_raw, current_date_string, debug=debug_mode)

        if not df_new_processed.empty:
            print("\nFetching old data from GitHub...")
            df_old = get_old_data_github(current_datetime)

            print("\nMerging data...")
            # Align columns before concat if df_old might have different columns or be empty
            if not df_old.empty:
                # Ensure df_old has at least the columns of df_new_processed if it's to be appended to
                # Or, if you want to preserve all columns from both, ensure both are aligned to the union of columns.
                # For simplicity, let's assume new data's columns are the standard.
                cols_for_concat = df_new_processed.columns
                df_old = df_old.reindex(columns=cols_for_concat) # Fills missing with NaN
            
            df_merged = pd.concat([df_old, df_new_processed], ignore_index=True)
            
            # Drop duplicates: consider 'time_measured' as the primary key for deduplication
            if 'time_measured' in df_merged.columns:
                df_merged = df_merged.drop_duplicates(subset=['time_measured'], keep='last')
                df_merged = df_merged.sort_values(by='time_measured').reset_index(drop=True)
            else:
                 df_merged = df_merged.drop_duplicates(keep='last').reset_index(drop=True)


            os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
            file_name = LOCAL_DATA_FILE_TEMPLATE.format(
                month=current_datetime.month,
                year=current_datetime.year
            )
            output_path = os.path.join(LOCAL_DATA_DIR, file_name)
            df_merged.to_csv(output_path, index=False) # Save without pandas index
            print(f"\nSaved merged data to {output_path}")
            if debug_mode:
                print("\n--- Final Merged DataFrame (head): ---")
                print(df_merged.head())
        else:
            print("Processing new data resulted in an empty DataFrame. No data saved.")
    else:
        print("Fetching new data resulted in an empty DataFrame. No data saved.")

    print("\nScript finished.")
