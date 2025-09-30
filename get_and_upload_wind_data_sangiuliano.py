"""
Wind Data Scraper for Venice Meteo Station
Scrapes current wind measurements from meteo-venezia.net and merges with historical data.
"""
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuration ---
TIMEZONE = ZoneInfo("Europe/Rome")
SOURCE_URL = 'https://meteo-venezia.net/'
GITHUB_DATA_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/marcoavesani/meteo_san_giuliano/"
    "master/data/measurements/measured_wind_venice_{month}_{year}.csv"
)
LOCAL_DATA_DIR = Path("./data/measurements")
LOCAL_DATA_FILE_TEMPLATE = "measured_wind_venice_{month}_{year}.csv"

# Data transformation constants
COLUMNS_TO_DROP = ['xtmp', 'ytmp', 'ygusttmp', 'x']
COLUMN_RENAME_MAP = {
    "t": "time_measured",
    "y": "wind_speed_measured",
    "ygust": "wind_gust_measured",
    "dir": "wind_direction_measured",
    "dirdegree": "wind_direction_degree_measured"
}
FINAL_COLUMN_ORDER = [
    'time_measured', 'wind_speed_measured', 'wind_gust_measured',
    'wind_direction_measured', 'wind_direction_degree_measured'
]

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def create_session_with_retries(
    retries: int = 3,
    backoff_factor: float = 0.3,
    status_forcelist: tuple = (500, 502, 504)
) -> requests.Session:
    """
    Create a requests session with automatic retry logic.
    
    Args:
        retries: Number of retry attempts
        backoff_factor: Delay factor between retries
        status_forcelist: HTTP status codes that trigger a retry
        
    Returns:
        Configured requests.Session object
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def validate_dataframe_structure(df: pd.DataFrame, required_columns: list) -> bool:
    """
    Validate that DataFrame has the required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        True if valid, False otherwise
    """
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        logger.warning(f"Missing columns: {missing_cols}")
        return False
    return True


# --- Main Functions ---

def scrape_wind_data() -> pd.DataFrame:
    """
    Scrapes wind data from the meteo-venezia.net website.
    
    The data is embedded in JavaScript as a 'dataPoints' array with objects
    containing properties like x, y, ygust, dir, dirdegree, t, etc.
    
    Returns:
        DataFrame containing the scraped wind data, or empty DataFrame on error
    """
    session = create_session_with_retries()
    
    try:
        response = session.get(SOURCE_URL, timeout=10)
        response.raise_for_status()
        html_data = response.text
        logger.info(f"Successfully retrieved data from {SOURCE_URL}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {SOURCE_URL}: {e}")
        return pd.DataFrame()
    
    try:
        # Find the dataPoints section in the HTML
        datapoints_loc = html_data.find("dataPoints=")
        if datapoints_loc == -1:
            logger.error("Could not find 'dataPoints=' in the HTML content")
            return pd.DataFrame()
        
        # Extract the JSON array
        remaining_html = html_data[datapoints_loc:]
        bracket_start = remaining_html.find("[")
        bracket_end = remaining_html.find("]")
        
        if bracket_start == -1 or bracket_end == -1:
            logger.error("Could not find complete data array in HTML")
            return pd.DataFrame()
        
        raw_data = remaining_html[bracket_start:bracket_end+1]
        logger.debug(f"Found data array with length: {len(raw_data)}")
        
        # Convert JavaScript object syntax to valid JSON
        js_properties = ["x", "y", "ygust", "dir", "dirdegree", "xtmp", "ytmp", "ygusttmp", "t"]
        json_data = raw_data
        
        for prop in js_properties:
            json_data = json_data.replace(f"{prop}:", f"\"{prop}\":")
        
        # Parse individual objects
        json_objects = json_data.split("},{")
        
        # Clean up first and last elements
        if json_objects and json_objects[0].startswith("[{"):
            json_objects[0] = json_objects[0][1:]
        
        if json_objects:
            last_obj = json_objects[-1]
            closing_brace_pos = last_obj.find("}]")
            if closing_brace_pos != -1:
                json_objects[-1] = last_obj[:closing_brace_pos+1]
        
        # Parse each object
        array_dict = []
        parse_errors = 0
        for obj in json_objects:
            try:
                json_obj = json.loads("{" + obj + "}")
                array_dict.append(json_obj)
            except json.JSONDecodeError:
                parse_errors += 1
        
        if parse_errors > 0:
            logger.warning(f"Failed to parse {parse_errors} data objects")
        
        if not array_dict:
            logger.error("No valid data objects found")
            return pd.DataFrame()
        
        df = pd.DataFrame(array_dict)
        logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
        return df
    
    except Exception as e:
        logger.error(f"Error processing wind data: {e}", exc_info=True)
        return pd.DataFrame()


def transform_wind_data(df: pd.DataFrame, current_date_str: str) -> pd.DataFrame:
    """
    Transforms the raw scraped wind data DataFrame.
    
    Args:
        df: Raw DataFrame from scraping
        current_date_str: Date string in YYYY-MM-DD format
        
    Returns:
        Processed DataFrame with renamed columns and proper types
    """
    if df.empty:
        logger.warning("Empty DataFrame provided for transformation")
        return pd.DataFrame()

    df_processed = df.copy()

    # Drop unnecessary columns
    cols_to_drop = [col for col in COLUMNS_TO_DROP if col in df_processed.columns]
    if cols_to_drop:
        df_processed = df_processed.drop(columns=cols_to_drop)
        logger.debug(f"Dropped columns: {cols_to_drop}")

    # Validate required column
    if 't' not in df_processed.columns:
        logger.error("Column 't' not found - cannot process time data")
        return pd.DataFrame()

    # Convert time column
    try:
        df_processed['t'] = current_date_str + ' ' + df_processed['t'].astype(str)
        df_processed['t'] = pd.to_datetime(
            df_processed['t'], 
            format="%Y-%m-%d %H:%M:%S"
        )
        logger.debug("Successfully converted time column")
    except Exception as e:
        logger.error(f"Error converting time column: {e}")
        return pd.DataFrame()

    # Set data types
    if 'dir' in df_processed.columns:
        df_processed['dir'] = df_processed['dir'].astype(pd.StringDtype())
    
    if 'dirdegree' in df_processed.columns:
        original_nulls = df_processed['dirdegree'].isna().sum()
        df_processed['dirdegree'] = pd.to_numeric(
            df_processed['dirdegree'], 
            errors='coerce'
        )
        coerced_nulls = df_processed['dirdegree'].isna().sum() - original_nulls
        if coerced_nulls > 0:
            logger.warning(f"Coerced {coerced_nulls} non-numeric values to NaN in 'dirdegree'")
        df_processed['dirdegree'] = df_processed['dirdegree'].fillna(0).astype(np.int64)

    # Rename columns
    df_processed = df_processed.rename(columns=COLUMN_RENAME_MAP)

    # Reorder columns
    existing_columns = [col for col in FINAL_COLUMN_ORDER if col in df_processed.columns]
    df_processed = df_processed[existing_columns]

    logger.info(f"Processed {len(df_processed)} records")
    return df_processed


def fetch_historical_data(current_dt: datetime) -> pd.DataFrame:
    """
    Fetches previously measured data from GitHub.
    
    Args:
        current_dt: Current datetime with timezone
        
    Returns:
        DataFrame with historical data, or empty DataFrame if not found
    """
    file_url = GITHUB_DATA_URL_TEMPLATE.format(
        month=current_dt.month,
        year=current_dt.year
    )
    
    try:
        df_old = pd.read_csv(
            file_url,
            parse_dates=['time_measured'],
            date_format="%Y-%m-%d %H:%M:%S"
        )
        logger.info(f"Loaded historical data from GitHub: {len(df_old)} records")
        
        # Ensure correct dtypes
        if 'time_measured' in df_old.columns:
            if not pd.api.types.is_datetime64_any_dtype(df_old['time_measured']):
                df_old['time_measured'] = pd.to_datetime(
                    df_old['time_measured'], 
                    format="%Y-%m-%d %H:%M:%S"
                )
        
        if 'wind_direction_measured' in df_old.columns:
            df_old['wind_direction_measured'] = df_old['wind_direction_measured'].astype(
                pd.StringDtype()
            )
        
        return df_old
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.info(f"No historical file found (404): {file_url}")
        else:
            logger.error(f"HTTP error fetching historical data: {e}")
    except pd.errors.EmptyDataError:
        logger.warning(f"Historical file is empty: {file_url}")
    except Exception as e:
        logger.error(f"Error reading historical data: {e}")
    
    return pd.DataFrame()


def merge_and_deduplicate(df_old: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    """
    Merges old and new data, removes duplicates, and sorts.
    
    Args:
        df_old: Historical DataFrame
        df_new: New scraped and processed DataFrame
        
    Returns:
        Merged and deduplicated DataFrame
    """
    # Align columns
    if not df_old.empty:
        df_old = df_old.reindex(columns=df_new.columns)
    
    df_merged = pd.concat([df_old, df_new], ignore_index=True)
    
    # Deduplicate based on timestamp
    if 'time_measured' in df_merged.columns:
        initial_count = len(df_merged)
        df_merged = df_merged.drop_duplicates(
            subset=['time_measured'], 
            keep='last'
        )
        duplicates_removed = initial_count - len(df_merged)
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate records")
        
        df_merged = df_merged.sort_values(by='time_measured').reset_index(drop=True)
    else:
        df_merged = df_merged.drop_duplicates(keep='last').reset_index(drop=True)
    
    logger.info(f"Final merged dataset: {len(df_merged)} records")
    return df_merged


def save_data(df: pd.DataFrame, current_dt: datetime) -> None:
    """
    Saves the DataFrame to local storage.
    
    Args:
        df: DataFrame to save
        current_dt: Current datetime for filename generation
    """
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    file_name = LOCAL_DATA_FILE_TEMPLATE.format(
        month=current_dt.month,
        year=current_dt.year
    )
    output_path = LOCAL_DATA_DIR / file_name
    
    df.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df)} records to {output_path}")


# --- Main Execution ---

def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("Starting Wind Data Collection Script")
    logger.info("=" * 60)
    
    current_datetime = datetime.now(TIMEZONE)
    current_date_string = current_datetime.strftime('%Y-%m-%d')
    logger.info(f"Current time: {current_datetime}")
    
    # Step 1: Scrape current data
    logger.info("Step 1: Scraping current wind data...")
    df_new_raw = scrape_wind_data()
    
    if df_new_raw.empty:
        logger.error("Failed to scrape data. Exiting.")
        return
    
    # Step 2: Transform data
    logger.info("Step 2: Transforming scraped data...")
    df_new_processed = transform_wind_data(df_new_raw, current_date_string)
    
    if df_new_processed.empty:
        logger.error("Data transformation failed. Exiting.")
        return
    
    # Step 3: Fetch historical data
    logger.info("Step 3: Fetching historical data from GitHub...")
    df_old = fetch_historical_data(current_datetime)
    
    # Step 4: Merge and deduplicate
    logger.info("Step 4: Merging and deduplicating data...")
    df_merged = merge_and_deduplicate(df_old, df_new_processed)
    
    # Step 5: Save to disk
    logger.info("Step 5: Saving merged data...")
    save_data(df_merged, current_datetime)
    
    logger.info("=" * 60)
    logger.info("Script completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
