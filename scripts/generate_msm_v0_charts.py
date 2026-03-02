#!/usr/bin/env python3
"""Generate returns_chart.png for each existing MSM v0 run folder."""

from pathlib import Path
import sys

# Allow importing from majors_alts_monitor
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from majors_alts_monitor.msm_funding_v0.msm_outputs import write_returns_chart_from_csv


def main():
    reports_dir = Path(__file__).resolve().parent.parent / "reports" / "msm_funding_v0"
    if not reports_dir.exists():
        print(f"Reports dir not found: {reports_dir}")
        return

    csvs = list(reports_dir.glob("*/msm_timeseries.csv"))
    if not csvs:
        print(f"No msm_timeseries.csv found under {reports_dir}")
        return

    print(f"Generating returns charts for {len(csvs)} run(s)...")
    for csv_path in sorted(csvs):
        run_id = csv_path.parent.name
        print(f"  {run_id} ... ", end="", flush=True)
        try:
            write_returns_chart_from_csv(csv_path, run_id=run_id)
            print("OK")
        except Exception as e:
            print(f"FAILED: {e}")
    print("Done.")


if __name__ == "__main__":
    main()
