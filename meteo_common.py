"""
Shared schema and safe I/O helpers for the San Giuliano wind dataset.

Conventions (see README):
  * Measurements: ``time_measured`` is naive LOCAL time (Europe/Rome), as shown
    by meteo-venezia.net. One file per calendar month of the measurement.
  * Forecasts: ``timestamp`` is naive UTC (Windguru ``initdate`` + ``hours``).
    One file per (calendar month of ``timestamp``, model_id).
  * CSVs are written without index, with a fixed column order, sorted by time.
"""
from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Callable, Iterable, Optional
from zoneinfo import ZoneInfo

import pandas as pd

logger = logging.getLogger(__name__)

TIMEZONE = ZoneInfo("Europe/Rome")
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

REPO_ROOT = Path(__file__).resolve().parent
MEASUREMENTS_DIR = REPO_ROOT / "data" / "measurements"
PREDICTIONS_DIR = REPO_ROOT / "data" / "predictions"

# --- Measurements -----------------------------------------------------------
MEASUREMENT_TIME = "time_measured"
MEASUREMENT_COLUMNS = [
    MEASUREMENT_TIME,
    "wind_speed_measured",
    "wind_gust_measured",
    "wind_direction_measured",
    "wind_direction_degree_measured",
]
MEASUREMENT_KEY = [MEASUREMENT_TIME]

# --- Forecasts --------------------------------------------------------------
FORECAST_TIME = "timestamp"
# Windguru variables kept for atmospheric models. Missing ones are written as
# empty cells so that every atmospheric file has exactly the same header.
ATMOSPHERIC_KEYS = [
    "WINDSPD", "GUST", "WINDDIR", "SLP", "TMP", "TMPE", "FLHGT", "RH",
    "TCDC", "APCP", "APCP1", "HCDC", "MCDC", "LCDC", "SLHGT", "PCPT",
]
# Wave models (GFS-Wave, id 84) only provide these, and get their own header
WAVE_MODEL_IDS = {84}
WAVE_KEYS = [
    "HTSGW", "PERPW", "DIRPW", "WVHGT", "WVPER", "WVDIR",
    "SWELL1", "SWPER1", "SWDIR1", "SWELL2", "SWPER2", "SWDIR2",
]
ALL_FORECAST_KEYS = ATMOSPHERIC_KEYS + WAVE_KEYS
FORECAST_META = ["model_name", "model_id", "init_time"]
FORECAST_KEY = [FORECAST_TIME, "model_id"]


def forecast_keys(model_id: Optional[int]) -> list[str]:
    return WAVE_KEYS if model_id in WAVE_MODEL_IDS else ATMOSPHERIC_KEYS


def forecast_columns(model_id: Optional[int]) -> list[str]:
    return [FORECAST_TIME, *forecast_keys(model_id), *FORECAST_META]


def measurement_path(year: int, month: int, base: Path = MEASUREMENTS_DIR) -> Path:
    return base / f"measured_wind_venice_{month}_{year}.csv"


def forecast_path(year: int, month: int, model_id: int, base: Path = PREDICTIONS_DIR) -> Path:
    return base / f"predicted_wind_venice_{month}_{year}_{model_id}.csv"


# --- Normalisation ----------------------------------------------------------

def _drop_index_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the anonymous index column written by older versions (``Unnamed: 0``)."""
    unnamed = [c for c in df.columns if isinstance(c, str) and c.startswith("Unnamed:")]
    return df.drop(columns=unnamed) if unnamed else df


def _parse_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", format=DATETIME_FORMAT)


def normalize_measurements(df: pd.DataFrame, drop_invalid: bool = False) -> pd.DataFrame:
    df = _drop_index_columns(df.copy())
    df = df.reindex(columns=MEASUREMENT_COLUMNS)
    df[MEASUREMENT_TIME] = _parse_datetime(df[MEASUREMENT_TIME])
    for col in ("wind_speed_measured", "wind_gust_measured"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    # Nullable integer: a missing direction must stay missing, not become 0 (= North)
    df["wind_direction_degree_measured"] = (
        pd.to_numeric(df["wind_direction_degree_measured"], errors="coerce").round().astype("Int64")
    )
    df["wind_direction_measured"] = df["wind_direction_measured"].astype("string")
    if drop_invalid:
        df = df[df[MEASUREMENT_TIME].notna()]
    return df


def normalize_forecasts(df: pd.DataFrame, drop_invalid: bool = False,
                        model_id: Optional[int] = None) -> pd.DataFrame:
    """Fixed header for the model (inferred from the ``model_id`` column if not given)."""
    df = _drop_index_columns(df.copy())
    if model_id is None and "model_id" in df:
        ids = pd.to_numeric(df["model_id"], errors="coerce").dropna()
        model_id = int(ids.iloc[0]) if len(ids) else None
    df = df.reindex(columns=forecast_columns(model_id))
    df[FORECAST_TIME] = _parse_datetime(df[FORECAST_TIME])
    df["init_time"] = _parse_datetime(df["init_time"])
    for key in forecast_keys(model_id):
        df[key] = pd.to_numeric(df[key], errors="coerce").astype("float64")
    df["model_id"] = pd.to_numeric(df["model_id"], errors="coerce").astype("Int64")
    df["model_name"] = df["model_name"].astype("string")
    if drop_invalid:
        df = df[df[FORECAST_TIME].notna() & df["model_id"].notna()]
    return df


# --- Safe read / merge / write ----------------------------------------------

def read_existing(path: Path, normalizer: Callable[[pd.DataFrame], pd.DataFrame]) -> pd.DataFrame:
    """
    Read the file already in the repository checkout.

    A missing file is fine (new month); a file that exists but cannot be parsed
    raises, so that the caller never overwrites history with a partial dataset.
    """
    if not path.exists():
        logger.info("No existing file %s, starting a new one", path.name)
        return normalizer(pd.DataFrame())
    df = pd.read_csv(path)
    df = normalizer(df)
    time_col = df.columns[0]
    n_bad = int(df[time_col].isna().sum())
    if n_bad:
        # Do not silently drop or propagate them: the file needs a manual look
        raise ValueError(f"{path} contains {n_bad} rows with invalid '{time_col}'")
    return df


def merge_rows(old: pd.DataFrame, new: pd.DataFrame, key: list[str]) -> pd.DataFrame:
    """Union of old and new rows; on identical key the new row wins."""
    frames = [f for f in (old, new) if not f.empty]
    if not frames:
        return old
    columns = frames[-1].columns
    # all-empty columns (e.g. init_time in old rows) are dropped before concat to keep dtypes stable
    merged = pd.concat([f.dropna(axis=1, how="all") for f in frames], ignore_index=True)
    merged = merged.reindex(columns=columns)
    merged = merged.drop_duplicates(subset=key, keep="last")
    return merged.sort_values(key, kind="stable").reset_index(drop=True)


def write_csv_safely(df: pd.DataFrame, path: Path, previous_rows: int, key: Iterable[str]) -> None:
    """Validate and atomically write ``df``; refuse to lose rows or write bad timestamps."""
    key = list(key)
    if df[key[0]].isna().any():
        raise ValueError(f"Refusing to write {path.name}: invalid values in '{key[0]}'")
    if df.duplicated(subset=key).any():
        raise ValueError(f"Refusing to write {path.name}: duplicated keys {key}")
    if len(df) < previous_rows:
        raise ValueError(
            f"Refusing to write {path.name}: {len(df)} rows < {previous_rows} already stored"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    os.close(fd)
    try:
        df.to_csv(tmp, index=False, date_format=DATETIME_FORMAT)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    logger.info("Wrote %s rows to %s (+%s)", len(df), path.name, len(df) - previous_rows)


def upsert(path: Path, new: pd.DataFrame, normalizer, key: list[str]) -> Optional[pd.DataFrame]:
    """Read existing file, merge ``new`` into it and write it back safely."""
    old = read_existing(path, normalizer)
    merged = normalizer(merge_rows(old, normalizer(new), key))
    write_csv_safely(merged, path, previous_rows=len(old), key=key)
    return merged
