# Meteo San Giuliano

Daily collection of wind **measurements** from the Polo Nautico station
(meteo-venezia.net, Venice – San Giuliano) and of next-day **forecasts** from all
Windguru models available for spot [536155](https://www.windguru.cz/536155),
to compare forecasts against observations.

## Data

### `data/measurements/measured_wind_venice_{month}_{year}.csv`

One file per calendar month, 5-minute samples, sorted by time, no duplicates.

| column | unit / format |
|---|---|
| `time_measured` | `YYYY-MM-DD HH:MM:SS`, **local time Europe/Rome** (naive; ambiguous during the DST fall-back hour) |
| `wind_speed_measured` | as shown on the station page (knots) |
| `wind_gust_measured` | as above |
| `wind_direction_measured` | 16-point compass (`N`, `NNE`, …) |
| `wind_direction_degree_measured` | degrees, integer; empty if unknown |

### `data/predictions/predicted_wind_venice_{month}_{year}_{model_id}.csv`

One file per calendar month **of the forecast timestamp** and per model.
Each day the latest available run is saved for the following day (00–23 UTC).

| column | unit / format |
|---|---|
| `timestamp` | `YYYY-MM-DD HH:MM:SS`, **UTC** (naive) |
| `WINDSPD`, `GUST`, `WINDDIR`, `SLP`, `TMP`, `TMPE`, `FLHGT`, `RH`, `TCDC`, `APCP`, `APCP1`, `HCDC`, `MCDC`, `LCDC`, `SLHGT`, `PCPT` | Windguru variables as returned by the API (wind in knots, direction in degrees, `SLP` hPa, `TMP` °C, `RH`/cloud cover %) |
| `model_name`, `model_id` | Windguru model |
| `init_time` | model run (UTC); lead time = `timestamp - init_time`. Empty for data collected before Oct 2026 |

All atmospheric models share this exact header; variables a model does not
provide are empty. The wave model (GFS-Wave, id 84) has its own header with
`HTSGW, PERPW, DIRPW, WVHGT, WVPER, WVDIR, SWELL1, SWPER1, SWDIR1, SWELL2, SWPER2, SWDIR2`
in place of the atmospheric variables.

**Time zones:** to align forecasts with measurements convert one of the two, e.g.

```python
m["time_utc"] = (m.time_measured.dt.tz_localize("Europe/Rome", ambiguous="NaT", nonexistent="NaT")
                 .dt.tz_convert("UTC").dt.tz_localize(None))
```

### Known gaps

* The station page only shows the current day: before October 2026 the
  measurement job ran once a day late in the evening and lost everything after
  the run; from late August 2026 the scheduled run was being delayed past
  midnight, so most days only contain 00:00–02:00. The job now runs hourly.
* Night-time gaps in 2024–early 2025 come from the station itself.

## Scripts

* `get_and_upload_wind_data_sangiuliano.py` – scrapes the station page and merges into the monthly file.
* `get_and_upload_forecast_sangiuliano.py` – downloads all Windguru models and merges tomorrow's forecast.
* `meteo_common.py` – schema, normalisation and safe read/merge/write shared by both.
* `tools/rebuild_from_git_history.py` – rebuilds every CSV from all committed
  versions (recovers rows lost by past bugs and normalises the format).

Safety rules: existing data are read from the local checkout; a file that
exists but cannot be parsed aborts the run; a write that would reduce the number
of rows or contain invalid/duplicated timestamps is refused; files are written
atomically; any failure exits non-zero so the GitHub Action is marked failed.

## Automation (`.github/workflows`)

* `fetch_measurements.yml` – every hour (`:23`).
* `fetch_forecasts.yml` – daily at 14:47 UTC.
* `tests.yml` – `pytest` on code changes (unit tests + integrity check of every CSV).

Both fetch workflows share a concurrency group so they never push at the same time,
and can be started manually (`workflow_dispatch`).

## Local use

```bash
pip install -r requirements.txt -r requirements-dev.txt
python get_and_upload_wind_data_sangiuliano.py
python get_and_upload_forecast_sangiuliano.py
pytest -q
```
