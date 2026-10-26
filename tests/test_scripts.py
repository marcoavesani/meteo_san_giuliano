"""Unit tests for parsing and safe-merge logic (no network)."""
from datetime import date, datetime

import pandas as pd
import pytest

import meteo_common as mc
from get_and_upload_forecast_sangiuliano import forecast_to_dataframe
from get_and_upload_wind_data_sangiuliano import parse_datapoints, to_measurements

# Excerpt in the real page format (unquoted keys, trailing comma before "]")
PAGE = """<script> var   dataPoints=[
    {x:0.00, y:1.8, ygust:2.5, dir:"N", dirdegree:"349",t:"00:00:00",xtmp:0,ytmp:0,ygusttmp:0 },{x:0.08, y:0.8, ygust:2.5, dir:"N", dirdegree:"",t:"00:05:00",xtmp:0,ytmp:0,ygusttmp:0 },{x:23.00, y:2.5, ygust:3.5, dir:"NNW", dirdegree:"347",t:"23:00:01",xtmp:0,ytmp:0,ygusttmp:0 },
        ]; other=[1,2];</script>"""


def test_parse_datapoints():
    raw = parse_datapoints(PAGE)
    assert len(raw) == 3
    assert list(raw["t"]) == ["00:00:00", "00:05:00", "23:00:01"]


def test_measurements_same_day():
    now = datetime(2026, 10, 26, 23, 30, tzinfo=mc.TIMEZONE)
    df = to_measurements(parse_datapoints(PAGE), now)
    assert list(df.columns) == mc.MEASUREMENT_COLUMNS
    assert df["time_measured"].iloc[-1] == pd.Timestamp("2026-10-26 23:00:01")
    # empty direction stays missing instead of becoming 0 (= North)
    assert pd.isna(df["wind_direction_degree_measured"].iloc[1])


def test_measurements_after_midnight_go_to_previous_day():
    now = datetime(2026, 10, 27, 0, 3, tzinfo=mc.TIMEZONE)
    df = to_measurements(parse_datapoints(PAGE), now)
    assert df["time_measured"].iloc[0] == pd.Timestamp("2026-10-27 00:00:00")
    assert df["time_measured"].iloc[-1] == pd.Timestamp("2026-10-26 23:00:01")


def _payload():
    hours = list(range(0, 72, 3))
    return {
        "wgmodel": {"id_model": 45, "model_longname": "ICON 13 km (World)"},
        "fcst": {
            "initdate": "2026-10-26 12:00:00", "hours": hours,
            "WINDSPD": [float(h) for h in hours], "WINDDIR": [180] * len(hours),
            "GUST": [1.0] * 3,  # wrong length: must be ignored, not crash
        },
    }


def test_forecast_to_dataframe():
    df = forecast_to_dataframe(_payload(), date(2026, 10, 27))
    assert list(df.columns) == mc.forecast_columns(45)
    assert df["timestamp"].dt.hour.tolist() == [0, 3, 6, 9, 12, 15, 18, 21]
    assert (df["timestamp"].dt.date == date(2026, 10, 27)).all()
    assert (df["init_time"] == pd.Timestamp("2026-10-26 12:00")).all()
    assert df["GUST"].isna().all() and df["WINDSPD"].iloc[0] == 12.0
    assert (df["model_id"] == 45).all()


def test_upsert_merges_and_new_values_win(tmp_path):
    path = tmp_path / "m.csv"
    a = pd.DataFrame({"time_measured": ["2026-10-01 00:00:00", "2026-10-01 00:05:00"],
                      "wind_speed_measured": [1.0, 2.0]})
    b = pd.DataFrame({"time_measured": ["2026-10-01 00:05:00", "2026-10-01 00:10:00"],
                      "wind_speed_measured": [9.0, 3.0]})
    mc.upsert(path, a, mc.normalize_measurements, mc.MEASUREMENT_KEY)
    out = mc.upsert(path, b, mc.normalize_measurements, mc.MEASUREMENT_KEY)
    assert out["wind_speed_measured"].tolist() == [1.0, 9.0, 3.0]
    assert pd.read_csv(path).columns.tolist() == mc.MEASUREMENT_COLUMNS


def test_upsert_refuses_to_overwrite_unreadable_history(tmp_path):
    path = tmp_path / "m.csv"
    path.write_text("time_measured,wind_speed_measured\n,1.0\n2026-10-01 00:05:00,2.0\n")
    new = pd.DataFrame({"time_measured": ["2026-10-01 00:10:00"], "wind_speed_measured": [3.0]})
    with pytest.raises(ValueError):
        mc.upsert(path, new, mc.normalize_measurements, mc.MEASUREMENT_KEY)
    assert path.read_text().count("\n") == 3  # untouched


def test_write_refuses_to_lose_rows(tmp_path):
    df = mc.normalize_measurements(pd.DataFrame({"time_measured": ["2026-10-01 00:00:00"]}))
    with pytest.raises(ValueError):
        mc.write_csv_safely(df, tmp_path / "x.csv", previous_rows=5, key=mc.MEASUREMENT_KEY)


def test_legacy_index_column_is_dropped():
    legacy = pd.DataFrame({"Unnamed: 0": [0], "time_measured": ["2024-07-24 00:00:00"]})
    assert list(mc.normalize_measurements(legacy).columns) == mc.MEASUREMENT_COLUMNS


def test_wave_model_has_its_own_header():
    payload = {"wgmodel": {"id_model": 84, "model_longname": "GFS-Wave 16 km (waves)"},
               "fcst": {"initdate": "2026-10-26 12:00:00", "hours": [12, 13],
                        "HTSGW": [0.5, 0.6], "PERPW": [4.0, 4.1]}}
    df = forecast_to_dataframe(payload, date(2026, 10, 27))
    assert list(df.columns) == mc.forecast_columns(84)
    assert "WINDSPD" not in df and df["HTSGW"].tolist() == [0.5, 0.6]
