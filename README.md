# Meteo San Giuliano

This repository contains scripts and workflows for fetching, processing, and storing weather data for San Giuliano. The data includes both measurements and predictions, which are stored in various formats for different use cases.

## Directory Structure

- `.github/workflows`: Contains GitHub Actions workflows for automating data fetching and processing.
  - `fetch_forecasts.yml`: Fetches weather forecasts and uploads them to the repository.
  - `fetch_measurements.yml`: Fetches weather measurements and uploads them to the repository.
- `data`: Contains subdirectories for measurements and predictions.
  - `measurements`: Stores weather measurements in CSV, JSON, and SQLite formats.
    - `measurements.csv`: CSV file with weather measurements.
    - `measurements.json`: JSON file with weather measurements.
    - `measurements.sqlite`: SQLite database with weather measurements.
    - `README.md`: Instructions on how to use the measurements data.
  - `predictions`: Stores weather predictions in CSV and JSON formats.
    - `predictions.csv`: CSV file with weather predictions.
    - `predictions.json`: JSON file with weather predictions.
    - `README.md`: Instructions on how to use the predictions data.
- `get_and_upload_wind_data_sangiuliano.py`: Script for fetching and processing wind data measurements.
- `get_and_upload_forecast_sangiuliano.py`: Script for fetching and processing wind data forecasts.
- `requirements.txt`: Lists the Python dependencies required to run the scripts.

## GitHub Actions Workflows

### Fetch Forecasts

The `fetch_forecasts.yml` workflow is triggered by a scheduled cron job or a push to the master branch. It performs the following steps:

1. Checks out the repository code.
2. Sets up the Python environment.
3. Installs dependencies.
4. Runs the Python script to fetch weather forecasts.
5. Commits and pushes the generated CSV files to the repository.

### Fetch Measurements

The `fetch_measurements.yml` workflow runs daily at 12:00 UTC and performs the following steps:

1. Checks out the repository code.
2. Sets up the Python environment.
3. Installs dependencies.
4. Runs the Python script to fetch weather measurements.
5. Commits and pushes the generated CSV files to the repository.

## Data Files

### Measurements

- `measurements.csv`: Contains columns for `timestamp`, `location`, `temperature`, `humidity`, `pressure`, and `wind_speed`.
- `measurements.json`: JSON format of the measurements data.
- `measurements.sqlite`: SQLite database containing the measurements data.

### Predictions

- `predictions.csv`: Contains columns for `id`, `prediction`, and `probability`.
- `predictions.json`: JSON format of the predictions data.

## Scripts

### `get_and_upload_wind_data_sangiuliano.py`

This script fetches wind data from a specified URL, processes it, and stores it in a CSV file. The script performs the following steps:

1. Fetches wind data from the URL.
2. Processes the data to extract relevant information.
3. Merges the new data with existing data from the repository.
4. Saves the merged data to a CSV file in the `data/measurements` directory.

### `get_and_upload_forecast_sangiuliano.py`

This script fetches wind data forecasts from Windguru, processes them, and stores them in CSV files. The script performs the following steps:

1. Fetches wind data forecasts from Windguru.
2. Processes the data to extract relevant information.
3. Merges the new data with existing data from the repository.
4. Saves the merged data to CSV files in the `data/predictions` directory.

## Requirements

The required Python packages are listed in the `requirements.txt` file:
`
numpy==2.0.1 pandas==2.2.2 Requests==2.32.3
`

## Usage

1. Clone the repository.
2. Install the required Python packages using `pip install -r requirements.txt`.
3. Run the scripts to fetch and process weather data.

For more detailed instructions, refer to the README files in the `data/measurements` and `data/predictions` directories.
