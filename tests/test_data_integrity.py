"""Checks every CSV under data/ against the dataset conventions (see README)."""
import re

import pandas as pd
import pytest

import meteo_common as mc

MEAS_FILES = sorted(mc.MEASUREMENTS_DIR.glob("*.csv"))
PRED_FILES = sorted(mc.PREDICTIONS_DIR.glob("*.csv"))


def _ym(path):
    month, year = map(int, re.search(r"_(\d+)_(\d{4})", path.name).groups())
    return year, month


@pytest.mark.parametrize("path", MEAS_FILES, ids=lambda p: p.name)
def test_measurement_file(path):
    raw = pd.read_csv(path)
    assert list(raw.columns) == mc.MEASUREMENT_COLUMNS, "header / column order"
    df = mc.normalize_measurements(raw)
    t = df["time_measured"]
    assert t.notna().all(), "unparseable timestamps"
    assert t.is_monotonic_increasing and not t.duplicated().any()
    year, month = _ym(path)
    assert ((t.dt.year == year) & (t.dt.month == month)).all(), "rows outside file month"
    assert df["wind_speed_measured"].between(0, 80).all()
    assert df["wind_direction_degree_measured"].dropna().between(0, 360).all()


@pytest.mark.parametrize("path", PRED_FILES, ids=lambda p: p.name)
def test_forecast_file(path):
    model_id = int(path.stem.rsplit("_", 1)[1])
    raw = pd.read_csv(path)
    assert list(raw.columns) == mc.forecast_columns(model_id), "header / column order"
    df = mc.normalize_forecasts(raw, model_id=model_id)
    t = df["timestamp"]
    assert t.notna().all(), "unparseable timestamps"
    assert t.is_monotonic_increasing and not t.duplicated().any()
    year, month = _ym(path)
    assert ((t.dt.year == year) & (t.dt.month == month)).all(), "rows outside file month"
    assert (df["model_id"] == model_id).all()
    assert not df[mc.forecast_keys(model_id)].isna().all(axis=1).any(), "rows without any variable"
    if "WINDSPD" in df:
        assert df["WINDSPD"].dropna().between(0, 150).all()
