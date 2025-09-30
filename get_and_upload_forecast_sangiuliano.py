"""
Wind Forecast Data Fetcher for Venice from Windguru
Fetches forecast models and predictions, merges with historical data.
"""
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import List, Dict, Any, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuration ---
TIMEZONE = ZoneInfo("Europe/Rome")
SPOT_ID = '536155'
BASE_IAPI_URL = 'https://www.windguru.cz/int/iapi.php'
FORECAST_IAPI_URL = 'https://www.windguru.net/int/iapi.php'
REQUEST_TIMEOUT = 15  # seconds
INTER_REQUEST_DELAY = 0.5  # seconds between requests to be polite

# Headers - Define base headers and update as needed
BASE_HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'it-IT,it;q=0.5',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-GPC': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Brave";v="126"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

# Data paths
GITHUB_DATA_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/marcoavesani/meteo_san_giuliano/"
    "master/data/predictions/predicted_wind_venice_{month}_{year}_{id}.csv"
)
LOCAL_DATA_DIR = Path("./data/predictions")
LOCAL_DATA_FILE_TEMPLATE = "predicted_wind_venice_{month}_{year}_{id}.csv"

# Desired forecast data keys (some may be missing in responses)
DESIRED_FORECAST_KEYS = [
    'WINDSPD', 'GUST', 'WINDDIR', 'SLP', 'TMP', 'TMPE', 'FLHGT', 'RH',
    'TCDC', 'APCP', 'APCP1', 'HCDC', 'MCDC', 'LCDC', 'SLHGT', 'PCPT'
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
    backoff_factor: float = 0.5,
    status_forcelist: tuple = (500, 502, 503, 504)
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


# --- Main Functions ---

def get_wind_models_for_spot(spot_id: str, session: requests.Session) -> List[Dict[str, Any]]:
    """
    Fetches the list of available forecast models for a given spot.
    
    Args:
        spot_id: Windguru spot ID
        session: Requests session with retry logic
        
    Returns:
        List of model configuration dictionaries
    """
    headers = {
        **BASE_HEADERS,
        'Referer': f'https://www.windguru.cz/{spot_id}',
    }
    params = {
        'q': 'forecast_spot',
        'id_spot': spot_id,
    }
    
    try:
        response = session.get(
            BASE_IAPI_URL, 
            params=params, 
            headers=headers, 
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        
        logger.debug("Successfully fetched spot forecast models")
        
        id_model_arr = data.get("tabs", [{}])[0].get("id_model_arr", [])
        logger.info(f"Found {len(id_model_arr)} forecast models for spot {spot_id}")
        return id_model_arr
        
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching wind models for spot {spot_id}")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching wind models: {e}")
        return []
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logger.error(f"Error parsing wind models response: {e}")
        return []


def get_forecast_data_for_model(
    model_config: Dict[str, Any], 
    spot_id: str, 
    session: requests.Session
) -> Optional[Dict[str, Any]]:
    """
    Fetches detailed forecast data for a specific model configuration.
    
    Args:
        model_config: Model configuration dictionary with id_model, rundef, etc.
        spot_id: Windguru spot ID
        session: Requests session with retry logic
        
    Returns:
        Forecast data dictionary or None if fetch fails
    """
    headers = {
        'sec-ch-ua': BASE_HEADERS['sec-ch-ua'],
        'Referer': 'https://www.windguru.cz/',
        'sec-ch-ua-mobile': BASE_HEADERS['sec-ch-ua-mobile'],
        'User-Agent': BASE_HEADERS['User-Agent'],
        'sec-ch-ua-platform': BASE_HEADERS['sec-ch-ua-platform'],
    }
    params = {
        'q': 'forecast',
        'id_model': model_config["id_model"],
        'rundef': model_config["rundef"],
        'initstr': model_config["initstr"],
        'id_spot': spot_id,
        'cachefix': model_config["cachefix"],
    }
    
    model_id = model_config.get("id_model", "Unknown")
    
    try:
        response = session.get(
            FORECAST_IAPI_URL, 
            params=params, 
            headers=headers, 
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Successfully fetched forecast data for model {model_id}")
        return data
        
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout fetching forecast for model {model_id}")
    except requests.exceptions.RequestException as e:
        logger.warning(f"Error fetching forecast for model {model_id}: {e}")
    except json.JSONDecodeError as e:
        logger.warning(f"Error parsing forecast data for model {model_id}: {e}")
    
    return None


def fetch_all_forecasts(spot_id: str) -> List[Dict[str, Any]]:
    """
    Orchestrates fetching all model forecasts for a spot.
    
    Args:
        spot_id: Windguru spot ID
        
    Returns:
        List of forecast data dictionaries from all models
    """
    session = create_session_with_retries()
    
    model_configs = get_wind_models_for_spot(spot_id, session)
    if not model_configs:
        logger.warning("No model configurations found")
        return []

    wind_data_per_model = []
    total_models = len(model_configs)
    
    for idx, config in enumerate(model_configs, 1):
        logger.info(f"Fetching model {idx}/{total_models}: {config.get('id_model', 'Unknown')}")
        
        forecast_data = get_forecast_data_for_model(config, spot_id, session)
        if forecast_data:
            wind_data_per_model.append(forecast_data)
        
        # Be polite to the server - add small delay between requests
        if idx < total_models:
            sleep(INTER_REQUEST_DELAY)
    
    logger.info(f"Successfully fetched data for {len(wind_data_per_model)}/{total_models} models")
    return wind_data_per_model


def fetch_historical_data(model_id: int, current_dt: datetime) -> pd.DataFrame:
    """
    Fetches previously predicted data for a model from GitHub.
    
    Args:
        model_id: Model ID number
        current_dt: Current datetime with timezone
        
    Returns:
        DataFrame with historical predictions or empty DataFrame if not found
    """
    file_url = GITHUB_DATA_URL_TEMPLATE.format(
        month=current_dt.month,
        year=current_dt.year,
        id=model_id
    )
    
    try:
        # Read without index column (files are saved with index=False)
        df_old = pd.read_csv(
            file_url,
            parse_dates=['timestamp'],
            date_format="%Y-%m-%d %H:%M:%S"
        )
        
        logger.info(f"Loaded {len(df_old)} historical records for model {model_id}")
        
        # Ensure correct dtypes
        if 'timestamp' in df_old.columns:
            if not pd.api.types.is_datetime64_any_dtype(df_old['timestamp']):
                df_old['timestamp'] = pd.to_datetime(
                    df_old['timestamp'], 
                    format="%Y-%m-%d %H:%M:%S"
                )
        
        if 'model_name' in df_old.columns:
            df_old['model_name'] = df_old['model_name'].astype(pd.StringDtype())
        
        return df_old
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.info(f"No historical file found for model {model_id} (404)")
        else:
            logger.warning(f"HTTP error fetching historical data for model {model_id}: {e}")
    except pd.errors.EmptyDataError:
        logger.warning(f"Historical file is empty for model {model_id}")
    except Exception as e:
        logger.warning(f"Error reading historical data for model {model_id}: {e}")
    
    return pd.DataFrame()


def extract_forecast_dataframe(
    model_output: Dict[str, Any], 
    target_date: datetime.date
) -> Optional[pd.DataFrame]:
    """
    Extract and transform forecast data into a DataFrame for a specific date.
    
    Args:
        model_output: Raw model forecast data from API
        target_date: Date to filter forecasts for
        
    Returns:
        DataFrame with forecasts for target date, or None if extraction fails
    """
    fcst_data = model_output.get("fcst")
    wgmodel_data = model_output.get("wgmodel")
    
    if not fcst_data or not wgmodel_data:
        logger.warning("Missing 'fcst' or 'wgmodel' data in model output")
        return None
    
    model_id = wgmodel_data.get('id_model', 'Unknown')
    model_name = wgmodel_data.get('model_longname', 'Unknown')
    
    try:
        # Parse initialization time
        init_date_str = fcst_data["initdate"]
        time_local_init = datetime.strptime(init_date_str, "%Y-%m-%d %H:%M:%S")
        
        # Get forecast hours
        model_hours = fcst_data.get("hours", [])
        if not model_hours:
            logger.warning(f"No hours data for model {model_id}")
            return None
        
        # Create timestamps
        timestamps = [time_local_init + timedelta(hours=h) for h in model_hours]
        
        # Build DataFrame with available forecast keys
        data_for_df = {"timestamp": timestamps}
        missing_keys = []
        
        for key in DESIRED_FORECAST_KEYS:
            if key in fcst_data:
                if len(fcst_data[key]) == len(timestamps):
                    data_for_df[key] = fcst_data[key]
                else:
                    logger.warning(
                        f"Length mismatch for '{key}' in model {model_id}: "
                        f"expected {len(timestamps)}, got {len(fcst_data[key])}"
                    )
                    data_for_df[key] = [np.nan] * len(timestamps)
            else:
                missing_keys.append(key)
        
        if missing_keys:
            logger.debug(f"Model {model_id} missing keys: {', '.join(missing_keys)}")
        
        # Create DataFrame
        full_model_df = pd.DataFrame(data_for_df)
        
        # Filter for target date
        model_df_filtered = full_model_df[
            full_model_df["timestamp"].dt.date == target_date
        ].copy()
        
        if model_df_filtered.empty:
            logger.debug(f"No data for {target_date} in model {model_name}")
            return None
        
        # Add model metadata
        model_df_filtered["model_name"] = model_name
        model_df_filtered["model_id"] = model_id
        model_df_filtered["model_name"] = model_df_filtered["model_name"].astype(pd.StringDtype())
        
        logger.info(f"Extracted {len(model_df_filtered)} forecasts for model {model_id} ({model_name})")
        return model_df_filtered
        
    except KeyError as e:
        logger.error(f"KeyError processing model {model_id}: {e}")
    except ValueError as e:
        logger.error(f"ValueError processing model {model_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error processing model {model_id}: {e}")
    
    return None


def merge_and_deduplicate_forecasts(
    df_old: pd.DataFrame, 
    df_new: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge old and new forecast data, removing duplicates.
    
    Args:
        df_old: Historical forecast DataFrame
        df_new: New forecast DataFrame
        
    Returns:
        Merged and deduplicated DataFrame
    """
    # Align columns
    if not df_old.empty:
        df_old = df_old.reindex(columns=df_new.columns)
    
    # Merge
    df_merged = pd.concat([df_old, df_new], ignore_index=True)
    
    # Deduplicate - keep latest forecast for each timestamp
    dedup_columns = ['timestamp', 'model_id'] + [
        k for k in DESIRED_FORECAST_KEYS if k in df_merged.columns
    ]
    
    initial_count = len(df_merged)
    df_merged = df_merged.drop_duplicates(
        subset=dedup_columns, 
        keep='last'
    ).reset_index(drop=True)
    
    duplicates_removed = initial_count - len(df_merged)
    if duplicates_removed > 0:
        logger.debug(f"Removed {duplicates_removed} duplicate forecasts")
    
    # Sort by timestamp
    df_merged = df_merged.sort_values(by="timestamp").reset_index(drop=True)
    
    return df_merged


def save_forecast_data(df: pd.DataFrame, model_id: int, current_dt: datetime) -> None:
    """
    Save forecast DataFrame to CSV file.
    
    Args:
        df: DataFrame to save
        model_id: Model ID for filename
        current_dt: Current datetime for filename
    """
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    file_name = LOCAL_DATA_FILE_TEMPLATE.format(
        month=current_dt.month,
        year=current_dt.year,
        id=model_id
    )
    output_path = LOCAL_DATA_DIR / file_name
    
    # Save without index to match existing file format (index=False)
    df.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df)} forecasts for model {model_id} to {output_path.name}")


def process_and_save_forecasts(
    model_data_list: List[Dict[str, Any]], 
    current_dt: datetime
) -> None:
    """
    Process raw model data, filter for tomorrow, merge with historical data, and save.
    
    Args:
        model_data_list: List of raw forecast data from all models
        current_dt: Current datetime with timezone
    """
    if not model_data_list:
        logger.warning("No model data to process")
        return

    tomorrow_date = (current_dt + timedelta(days=1)).date()
    logger.info(f"Processing forecasts for {tomorrow_date}")
    
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    processed_count = 0

    for model_output in model_data_list:
        # Extract forecast DataFrame for tomorrow
        df_new = extract_forecast_dataframe(model_output, tomorrow_date)
        
        if df_new is None or df_new.empty:
            continue
        
        # Get model ID
        model_id = int(df_new['model_id'].iloc[0])
        
        # Fetch historical data
        df_old = fetch_historical_data(model_id, current_dt)
        
        # Align columns if historical data exists
        if df_old.empty:
            df_old = pd.DataFrame(columns=df_new.columns)
        
        # Merge and deduplicate
        df_merged = merge_and_deduplicate_forecasts(df_old, df_new)
        
        # Save to file
        save_forecast_data(df_merged, model_id, current_dt)
        processed_count += 1
    
    logger.info(f"Successfully processed and saved {processed_count} model forecasts")


# --- Main Execution ---

def main():
    """Main execution function."""
    logger.info("=" * 70)
    logger.info("Starting Wind Forecast Collection Script")
    logger.info("=" * 70)
    
    current_datetime = datetime.now(TIMEZONE)
    logger.info(f"Current time: {current_datetime}")
    logger.info(f"Forecast target: {(current_datetime + timedelta(days=1)).date()}")
    
    # Step 1: Fetch all forecast models
    logger.info("Step 1: Fetching forecast models from Windguru...")
    raw_model_data_list = fetch_all_forecasts(SPOT_ID)
    
    if not raw_model_data_list:
        logger.error("Could not fetch any forecast data. Exiting.")
        return
    
    # Step 2: Process and save forecasts
    logger.info("Step 2: Processing and saving forecast data...")
    process_and_save_forecasts(raw_model_data_list, current_datetime)
    
    logger.info("=" * 70)
    logger.info("Script completed successfully!")
    logger.info("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
