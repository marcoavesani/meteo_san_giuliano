"""
Wind forecast fetcher for Venice (Windguru spot 536155).

For every model offered on the spot, keeps the forecast for *tomorrow*
(00:00-23:00 UTC of the day after the run date, as in the historical data) from
the latest available run and merges it into
data/predictions/predicted_wind_venice_{month}_{year}_{model_id}.csv, where
month/year are those of the forecast timestamp.

Timestamps are UTC (Windguru ``initdate`` is UTC); ``init_time`` is the model
run used, so the lead time is ``timestamp - init_time``.
"""
from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timedelta
from time import sleep
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import meteo_common as mc

SPOT_ID = "536155"
SPOT_IAPI_URL = "https://www.windguru.cz/int/iapi.php"
FORECAST_IAPI_URL = "https://www.windguru.net/int/iapi.php"
REQUEST_TIMEOUT = 20
INTER_REQUEST_DELAY = 0.5
# Abort (non-zero exit) if fewer models than this could be saved
MIN_MODELS_OK = 5

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "it-IT,it;q=0.5",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
    "Referer": f"https://www.windguru.cz/{SPOT_ID}",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=1.0,
                  status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(HEADERS)
    return session


def get_models(session: requests.Session) -> List[Dict[str, Any]]:
    resp = session.get(SPOT_IAPI_URL, params={"q": "forecast_spot", "id_spot": SPOT_ID},
                       timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    models = resp.json()["tabs"][0]["id_model_arr"]
    logger.info("Found %s forecast models", len(models))
    return models


def get_forecast(session: requests.Session, cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    params = {"q": "forecast", "id_spot": SPOT_ID, "id_model": cfg["id_model"],
              "rundef": cfg["rundef"], "initstr": cfg["initstr"], "cachefix": cfg["cachefix"]}
    try:
        resp = session.get(FORECAST_IAPI_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("Model %s: download failed: %s", cfg.get("id_model"), exc)
        return None


def forecast_to_dataframe(payload: Dict[str, Any], target_date: date) -> pd.DataFrame:
    """
    Convert a Windguru forecast payload into rows whose UTC date is ``target_date``.
    """
    fcst, model = payload.get("fcst") or {}, payload.get("wgmodel") or {}
    hours = fcst.get("hours") or []
    if not hours or "initdate" not in fcst:
        return pd.DataFrame()

    init_utc = datetime.strptime(fcst["initdate"], mc.DATETIME_FORMAT)
    times_utc = pd.DatetimeIndex([init_utc + timedelta(hours=h) for h in hours])

    data = {mc.FORECAST_TIME: times_utc}
    for key in mc.ALL_FORECAST_KEYS:
        values = fcst.get(key)
        if values is not None and len(values) == len(hours):
            data[key] = values
        elif values is not None:
            logger.warning("Model %s: length mismatch for %s", model.get("id_model"), key)
    df = pd.DataFrame(data)
    df["model_name"] = model.get("model_longname") or fcst.get("model_longname")
    df["model_id"] = model.get("id_model") or fcst.get("id_model")
    df["init_time"] = init_utc
    df = df[times_utc.date == target_date]
    return mc.normalize_forecasts(df)


def main() -> int:
    now = datetime.now(mc.TIMEZONE)
    tomorrow = (now + timedelta(days=1)).date()
    logger.info("Now %s, collecting forecasts for %s (UTC hours)", now.isoformat(), tomorrow)

    session = make_session()
    models = get_models(session)
    saved, failed = 0, []
    for i, cfg in enumerate(models):
        if i:
            sleep(INTER_REQUEST_DELAY)
        model_id = cfg.get("id_model")
        payload = get_forecast(session, cfg)
        df = forecast_to_dataframe(payload, tomorrow) if payload else pd.DataFrame()
        if df.empty:
            failed.append(model_id)
            continue
        # Files are bucketed by the month of the forecast timestamp
        ts = df[mc.FORECAST_TIME]
        for (year, month), part in df.groupby([ts.dt.year, ts.dt.month]):
            path = mc.forecast_path(year, month, int(model_id))
            mc.upsert(path, part, mc.normalize_forecasts, mc.FORECAST_KEY)
        saved += 1

    logger.info("Saved %s/%s models; without data: %s", saved, len(models), failed)
    if saved < min(MIN_MODELS_OK, len(models)):
        logger.error("Too few models saved")
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # make the GitHub Action fail visibly
        logger.exception("Forecast collection failed")
        sys.exit(1)
