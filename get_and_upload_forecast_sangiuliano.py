import os
import json # Keep for potential direct json use, though response.json() handles it
import re   # Not used in the original snippet, can be removed if not needed elsewhere
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
import requests

# --- Constants ---
TIMEZONE = ZoneInfo("Europe/Rome")
SPOT_ID = '536155'
BASE_IAPI_URL = 'https://www.windguru.cz/int/iapi.php' # .cz and .net seem interchangeable for iapi

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

# GitHub and Local Data Paths
GITHUB_DATA_URL_TEMPLATE = "https://raw.githubusercontent.com/marcoavesani/meteo_san_giuliano/master/data/predictions/predicted_wind_venice_{month}_{year}_{id}.csv"
LOCAL_DATA_DIR = "./data/predictions"
LOCAL_DATA_FILE_TEMPLATE = "predicted_wind_venice_{month}_{year}_{id}.csv"

# Desired forecast data keys
DESIRED_FORECAST_KEYS = [
    'WINDSPD', 'GUST', 'WINDDIR', 'SLP', 'TMP', 'TMPE', 'FLHGT', 'RH',
    'TCDC', 'APCP', 'APCP1', 'HCDC', 'MCDC', 'LCDC', 'SLHGT', 'PCPT'
]

# --- Functions ---

def get_wind_models_for_spot(spot_id: str, debug: bool = False) -> List[Dict[str, Any]]:
    """Fetches the list of available forecast models for a given spot."""
    headers = {
        **BASE_HEADERS,
        'Referer': f'https://www.windguru.cz/{spot_id}',
        # 'If-Modified-Since': 'Fri, 19 Jul 2024 17:54:20 GMT', # Likely not useful if hardcoded
    }
    params = {
        'q': 'forecast_spot',
        'id_spot': spot_id,
    }
    try:
        response = requests.get(BASE_IAPI_URL, params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        if debug:
            print("--- Spot Forecast Models Response ---")
            print(json.dumps(data, indent=2))

        id_model_arr = data.get("tabs", [{}])[0].get("id_model_arr", [])
        if debug:
            print("\n--- Extracted id_model_arr ---")
            print(id_model_arr)
        return id_model_arr
    except requests.exceptions.RequestException as e:
        print(f"Error fetching wind models: {e}")
        return []
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        print(f"Error parsing wind models response: {e}")
        if debug and 'response' in locals():
            print("Response content:", response.text)
        return []


def get_forecast_data_for_model(model_config: Dict[str, Any], spot_id: str, debug: bool = False) -> Optional[Dict[str, Any]]:
    """Fetches detailed forecast data for a specific model configuration."""
    headers = {
        # Simpler headers for this endpoint as per original
        'sec-ch-ua': BASE_HEADERS['sec-ch-ua'],
        'Referer': 'https://www.windguru.cz/', # General referer
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
    try:
        # Using .net as in original, though .cz might also work
        response = requests.get('https://www.windguru.net/int/iapi.php', params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        if debug:
            print(f"\n--- Forecast Data for Model {model_config['id_model']} ---")
            print(json.dumps(data, indent=2))
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching forecast for model {model_config['id_model']}: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing forecast data for model {model_config['id_model']}: {e}")
        if debug and 'response' in locals():
            print("Response content:", response.text)
    return None


def get_wind_data_today_wg(spot_id: str, debug: bool = False) -> List[Dict[str, Any]]:
    """Orchestrates fetching all model forecasts for a spot."""
    model_configs = get_wind_models_for_spot(spot_id, debug=debug)
    if not model_configs:
        print("No model configurations found. Exiting.")
        return []

    wind_data_per_model = []
    for config in model_configs:
        forecast_data = get_forecast_data_for_model(config, spot_id, debug=debug)
        if forecast_data:
            wind_data_per_model.append(forecast_data)
        # Optional: Add a small delay to be polite to the server
        # import time
        # time.sleep(0.5) 
    return wind_data_per_model


def get_old_data_github_wg(model_id: int, current_dt: datetime) -> pd.DataFrame:
    """
    Fetches previously predicted data for a model from GitHub.
    Returns an empty DataFrame if the file is not found or an error occurs.
    """
    file_url = GITHUB_DATA_URL_TEMPLATE.format(
        month=current_dt.month,
        year=current_dt.year,
        id=model_id
    )
    try:
        # parse_dates=['timestamp'] and date_format for direct conversion
        # index_col=[0] assumes the saved CSV has an unnamed index column
        df_old = pd.read_csv(
            file_url,
            index_col=0, # Assumes first column is the index from previous saves
            parse_dates=['timestamp'],
            date_format="%Y-%m-%d %H:%M:%S" # Be explicit if format is known
        )
        # Ensure 'timestamp' is datetime if it wasn't parsed correctly or not the index
        if 'timestamp' in df_old.columns and not pd.api.types.is_datetime64_any_dtype(df_old['timestamp']):
             df_old['timestamp'] = pd.to_datetime(df_old['timestamp'], format="%Y-%m-%d %H:%M:%S")
        
        if 'model_name' in df_old.columns:
            df_old['model_name'] = df_old['model_name'].astype(pd.StringDtype())
        return df_old
    except FileNotFoundError: # For local file reads, not directly for URLs
        print(f"File not found locally (this shouldn't happen with URL read): {file_url}")
    except pd.errors.EmptyDataError:
        print(f"No data in file (or file not found): {file_url}. Creating a new DataFrame.")
    except requests.exceptions.HTTPError as e: # More specific for URL errors (like 404)
        print(f"HTTP error fetching {file_url}: {e}. Creating a new DataFrame.")
    except Exception as e: # Catch-all for other pandas or request errors
        print(f"Error reading {file_url}: {e}. Creating a new DataFrame.")
    return pd.DataFrame()


def process_and_save_forecasts(model_data_list: List[Dict[str, Any]], current_dt: datetime, debug: bool = False):
    """Processes raw model data, filters for tomorrow, merges with old data, and saves."""
    if not model_data_list:
        print("No model data to process.")
        return

    tomorrow_date = (current_dt + timedelta(days=1)).date()
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

    for model_output in model_data_list:
        fcst_data = model_output.get("fcst")
        wgmodel_data = model_output.get("wgmodel")

        if not fcst_data or not wgmodel_data:
            print("Skipping model due to missing 'fcst' or 'wgmodel' data.")
            if debug: print(f"Problematic model_output: {model_output}")
            continue

        try:
            init_date_str = fcst_data["initdate"]
            time_local_init = datetime.strptime(init_date_str, "%Y-%m-%d %H:%M:%S")
            
            # Create a DataFrame for all hours from this model run
            model_hours = fcst_data.get("hours", [])
            if not model_hours:
                print(f"No hours data for model {wgmodel_data.get('id_model', 'Unknown')}. Skipping.")
                continue

            timestamps = [time_local_init + timedelta(hours=h) for h in model_hours]
            
            data_for_df = {"timestamp": timestamps}
            for key in DESIRED_FORECAST_KEYS:
                if key in fcst_data:
                    # Ensure data length matches timestamps length
                    if len(fcst_data[key]) == len(timestamps):
                        data_for_df[key] = fcst_data[key]
                    else:
                        if debug:
                            print(f"Warning: Length mismatch for key '{key}' in model {wgmodel_data.get('id_model')}. "
                                  f"Expected {len(timestamps)}, got {len(fcst_data[key])}. Padding with NaN.")
                        # Pad with NaN or skip key. Padding helps maintain DataFrame structure.
                        data_for_df[key] = [np.nan] * len(timestamps) 
                # else: # Optionally log missing keys
                #     if debug: print(f"Key '{key}' not found in forecast data for model {wgmodel_data.get('id_model')}")


            full_model_df = pd.DataFrame(data_for_df)

            # Filter for tomorrow's date
            model_df_tomorrow = full_model_df[full_model_df["timestamp"].dt.date == tomorrow_date].copy()

            if model_df_tomorrow.empty:
                if debug:
                    print(f"No data for tomorrow for model {wgmodel_data['model_longname']}. Skipping this model.")
                continue

            model_df_tomorrow["model_name"] = wgmodel_data["model_longname"]
            model_df_tomorrow["model_id"] = wgmodel_data["id_model"]
            model_df_tomorrow["model_name"] = model_df_tomorrow["model_name"].astype(pd.StringDtype())

            model_id = int(wgmodel_data["id_model"])
            
            df_old = get_old_data_github_wg(model_id, current_dt)
            
            # Ensure columns match before concat, especially if df_old is empty
            # If df_old is empty, it won't have columns, this helps align them.
            if df_old.empty:
                df_old = pd.DataFrame(columns=model_df_tomorrow.columns)
            else: # Align columns, new data takes precedence for column set
                df_old = df_old.reindex(columns=model_df_tomorrow.columns)


            df_merged = pd.concat([df_old, model_df_tomorrow], ignore_index=True)
            
            # Drop duplicates based on timestamp and model_id primarily.
            # Keep the last entry, assuming it's the newest/most updated forecast for that hour.
            # All columns should ideally be considered for a true duplicate.
            df_merged = df_merged.drop_duplicates(subset=['timestamp', 'model_id'] + [k for k in DESIRED_FORECAST_KEYS if k in df_merged.columns], 
                                                  keep='last').reset_index(drop=True)
            
            # Sort by timestamp for consistency
            df_merged = df_merged.sort_values(by="timestamp").reset_index(drop=True)


            file_name = LOCAL_DATA_FILE_TEMPLATE.format(
                month=current_dt.month,
                year=current_dt.year,
                id=model_id
            )
            output_path = os.path.join(LOCAL_DATA_DIR, file_name)
            df_merged.to_csv(output_path, index=False) # Save without pandas index
            print(f"Saved merged data for model {model_id} to {output_path}")

        except KeyError as e:
            print(f"KeyError processing model data: {e}. Model data: {model_output}")
        except ValueError as e:
            print(f"ValueError processing model data: {e}. Model init date: {fcst_data.get('initdate', 'N/A')}")
        except Exception as e:
            print(f"An unexpected error occurred processing model {wgmodel_data.get('id_model', 'Unknown')}: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    # Set debug=True for verbose output
    debug_mode = False # Or True
    
    print(f"Running script at {datetime.now(TIMEZONE)}")
    
    current_datetime = datetime.now(TIMEZONE)
    
    print("Fetching wind data from Windguru...")
    raw_model_data_list = get_wind_data_today_wg(SPOT_ID, debug=debug_mode)
    
    if raw_model_data_list:
        print(f"Successfully fetched data for {len(raw_model_data_list)} models.")
        process_and_save_forecasts(raw_model_data_list, current_datetime, debug=debug_mode)
    else:
        print("Could not fetch any wind data.")
        
    print("Script finished.")
