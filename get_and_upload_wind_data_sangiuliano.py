"""
Wind measurement scraper for the Polo Nautico station on meteo-venezia.net.

The home page embeds today's 5-minute samples (from 00:00 local time to now) in
a JavaScript ``dataPoints=[...]`` array with time-of-day only. Anything not saved
before local midnight is lost, so this script must run several times a day
(see .github/workflows/fetch_measurements.yml).

Rows are merged into data/measurements/measured_wind_venice_{month}_{year}.csv.
``time_measured`` is naive local time (Europe/Rome).
"""
from __future__ import annotations

import json
import logging
import re
import sys
from datetime import datetime, timedelta

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import meteo_common as mc

SOURCE_URL = "https://meteo-venezia.net/"
REQUEST_TIMEOUT = 20
# Samples stamped later than "now" by more than this belong to the previous day
FUTURE_TOLERANCE = timedelta(minutes=10)

RENAME = {
    "t": "time_measured",
    "y": "wind_speed_measured",
    "ygust": "wind_gust_measured",
    "dir": "wind_direction_measured",
    "dirdegree": "wind_direction_degree_measured",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

_ARRAY_RE = re.compile(r"dataPoints\s*=\s*(\[.*?\])", re.S)
_KEY_RE = re.compile(r"([{,]\s*)([A-Za-z_]\w*)\s*:")
_TRAILING_COMMA_RE = re.compile(r",\s*([\]}])")


def parse_datapoints(html: str) -> pd.DataFrame:
    """Extract the ``dataPoints`` JS array into a DataFrame (raw column names)."""
    match = _ARRAY_RE.search(html)
    if not match:
        raise ValueError("'dataPoints=[...]' not found in page")
    js = match.group(1)
    js = _KEY_RE.sub(r'\1"\2":', js)          # quote object keys
    js = _TRAILING_COMMA_RE.sub(r"\1", js)    # drop trailing commas
    js = js.replace("'", '"')
    return pd.DataFrame(json.loads(js))


def to_measurements(raw: pd.DataFrame, now_local: datetime) -> pd.DataFrame:
    """Rename columns and attach the right calendar date to each time-of-day."""
    if raw.empty or "t" not in raw:
        return mc.normalize_measurements(pd.DataFrame())
    df = raw.rename(columns=RENAME)
    now_naive = now_local.replace(tzinfo=None)
    times = pd.to_datetime(now_naive.strftime("%Y-%m-%d ") + df["time_measured"].astype(str),
                           format=mc.DATETIME_FORMAT, errors="coerce")
    in_future = times > now_naive + FUTURE_TOLERANCE
    if in_future.any():
        logger.warning("%s samples are after 'now': assigning them to the previous day",
                       int(in_future.sum()))
        times = times.where(~in_future, times - pd.Timedelta(days=1))
    df["time_measured"] = times
    df = mc.normalize_measurements(df)
    bad = df["time_measured"].isna()
    if bad.any():
        logger.warning("Dropping %s samples with unparseable time", int(bad.sum()))
        df = df[~bad]
    # Same local time can appear twice on the DST fall-back night: keep the last one
    return df.drop_duplicates(mc.MEASUREMENT_KEY, keep="last")


def fetch_page() -> str:
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(
        total=4, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504))))
    resp = session.get(SOURCE_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def main() -> int:
    now = datetime.now(mc.TIMEZONE)
    logger.info("Now %s", now.isoformat())
    df = to_measurements(parse_datapoints(fetch_page()), now)
    if df.empty:
        logger.error("No samples found on the page")
        return 1
    logger.info("Scraped %s samples, %s .. %s", len(df),
                df["time_measured"].min(), df["time_measured"].max())

    ts = df["time_measured"]
    for (year, month), part in df.groupby([ts.dt.year, ts.dt.month]):
        mc.upsert(mc.measurement_path(year, month), part,
                  mc.normalize_measurements, mc.MEASUREMENT_KEY)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # make the GitHub Action fail visibly
        logger.exception("Measurement collection failed")
        sys.exit(1)
