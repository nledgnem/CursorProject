import pandas as pd
from pathlib import Path


def main() -> None:
    base = Path("reports") / "msm_funding_v0"
    candidates = sorted(base.rglob("msm_timeseries.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        print("No msm_timeseries.csv files found.")
        return

    latest = candidates[0]
    df = pd.read_csv(latest)
    if "F_tk_apr" not in df.columns:
        if "F_tk" in df.columns:
            df["F_tk_apr"] = df["F_tk"] * 365.0 * 100.0
        else:
            raise SystemExit("No F_tk_apr or F_tk column found in msm_timeseries.csv.")

    f = df["F_tk_apr"].astype(float)
    print(f"Latest msm_timeseries: {latest}")
    print(f"Max APR: {f.max():.4f}%")
    print(f"Min APR: {f.min():.4f}%")
    print(f"Mean APR: {f.mean():.44f}%")
    print(f"Weeks with APR > 0%: {(f > 0.0).sum()} out of {len(f)}")


if __name__ == "__main__":
    main()

