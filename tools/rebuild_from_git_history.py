"""
Rebuild every data CSV from the full git history of the repository.

Every committed version of every file under data/ is read; rows with a valid
timestamp are pooled together and, for each key, the value from the most recent
commit is kept. This recovers rows that were later lost, e.g.

  * May-Sep 2025 forecasts whose timestamps were blanked day after day;
  * Aug 2026 forecasts overwritten on 2026-08-17 after a failed history download.

Output files follow the conventions in meteo_common (no index column, fixed
column order, forecasts bucketed by the month of the forecast timestamp).

Usage (from the repository root):
    python tools/rebuild_from_git_history.py --out data_rebuilt     # dry run
    python tools/rebuild_from_git_history.py --in-place             # overwrite data/
"""
from __future__ import annotations

import argparse
import io
import logging
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import meteo_common as mc  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("rebuild")
logging.getLogger("meteo_common").setLevel(logging.WARNING)

MEAS_RE = re.compile(r"data/measurements/measured_wind_venice_(\d+)_(\d+)\.csv$")
PRED_RE = re.compile(r"data/predictions/predicted_wind_venice_(\d+)_(\d+)_(\d+)\.csv$")


def git(*args: str, repo: Path) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True,
                          capture_output=True, text=True).stdout


def file_versions(repo: Path):
    """Yield (commit_order, path, blob_sha) for every version of every data file."""
    out = git("log", "--reverse", "--format=C %H", "--raw", "--no-abbrev",
              "--no-renames", "--", "data", repo=repo)
    order = -1
    for line in out.splitlines():
        if line.startswith("C "):
            order += 1
        elif line.startswith(":"):
            meta, path = line.split("\t", 1)
            new_sha = meta.split()[3]
            if set(new_sha) != {"0"}:  # skip deletions
                yield order, path, new_sha


def read_blobs(repo: Path, shas: list[str]) -> dict[str, bytes]:
    proc = subprocess.run(["git", "-C", str(repo), "cat-file", "--batch"],
                          input="\n".join(shas).encode() + b"\n", capture_output=True, check=True)
    data, pos, blobs = proc.stdout, 0, {}
    for sha in shas:
        header_end = data.index(b"\n", pos)
        _, _, size = data[pos:header_end].split()
        start = header_end + 1
        blobs[sha] = data[start:start + int(size)]
        pos = start + int(size) + 1
    return blobs


def collect(repo: Path):
    versions = list(file_versions(repo))
    log.info("%s file versions in history", len(versions))
    unique = sorted({sha for _, _, sha in versions})
    blobs = read_blobs(repo, unique)
    meas, pred = [], []
    for order, path, sha in versions:
        if MEAS_RE.search(path):
            target, norm = meas, mc.normalize_measurements
        elif PRED_RE.search(path):
            target, norm = pred, mc.normalize_forecasts
        else:
            continue
        try:
            df = pd.read_csv(io.BytesIO(blobs[sha]))
        except pd.errors.EmptyDataError:
            continue
        df = norm(df, drop_invalid=True)
        df["_order"] = order
        target.append(df)
    return pd.concat(meas, ignore_index=True), pd.concat(pred, ignore_index=True)


def latest_per_key(df: pd.DataFrame, key: list[str]) -> pd.DataFrame:
    df = df.sort_values("_order", kind="stable").drop_duplicates(key, keep="last")
    return df.drop(columns="_order").sort_values(key, kind="stable").reset_index(drop=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--out", type=Path, help="write rebuilt data/ tree here")
    g.add_argument("--in-place", action="store_true", help="replace data/ in the working tree")
    ap.add_argument("--repo", type=Path, default=mc.REPO_ROOT)
    args = ap.parse_args()

    meas_all, pred_all = collect(args.repo)
    meas = latest_per_key(meas_all, mc.MEASUREMENT_KEY)
    pred = latest_per_key(pred_all, mc.FORECAST_KEY)

    out_root = (args.repo / "data") if args.in_place else args.out
    if args.in_place:
        for sub in ("measurements", "predictions"):
            shutil.rmtree(out_root / sub, ignore_errors=True)
    meas_dir, pred_dir = out_root / "measurements", out_root / "predictions"
    meas_dir.mkdir(parents=True, exist_ok=True)
    pred_dir.mkdir(parents=True, exist_ok=True)

    stats = defaultdict(int)
    t = meas[mc.MEASUREMENT_TIME]
    for (y, m), grp in meas.groupby([t.dt.year, t.dt.month]):
        mc.write_csv_safely(grp, mc.measurement_path(y, m, meas_dir), 0, mc.MEASUREMENT_KEY)
        stats["measurement_files"] += 1
    t = pred[mc.FORECAST_TIME]
    for (y, m, mid), grp in pred.groupby([t.dt.year, t.dt.month, pred["model_id"]]):
        grp = mc.normalize_forecasts(grp, model_id=int(mid))
        # Rows without any value (e.g. GFS-Wave before wave variables were saved) carry no data
        empty = grp[mc.forecast_keys(int(mid))].isna().all(axis=1)
        stats["empty_rows"] += int(empty.sum())
        if empty.all():
            continue
        mc.write_csv_safely(grp[~empty], mc.forecast_path(y, m, int(mid), pred_dir), 0, mc.FORECAST_KEY)
        stats["forecast_files"] += 1
        stats["forecast_rows"] += int((~empty).sum())

    log.info("Measurements: %s rows, %s files", len(meas), stats["measurement_files"])
    log.info("Forecasts:    %s rows, %s files (%s rows without any variable dropped)",
             stats["forecast_rows"], stats["forecast_files"], stats["empty_rows"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
