"""BTC/ETH funding summary: monthly average and annualized APR.

Computes per-month mean daily funding rates for BTC and ETH from
Bronze fact_funding, and reports simple annualized APR for each month.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    path = repo_root / "data" / "curated" / "data_lake" / "fact_funding.parquet"

    df = pl.read_parquet(path)

    # Filter BTC/ETH and the requested date window
    fd = df.filter(
        (pl.col("asset_id").is_in(["BTC", "ETH"]))
        & (pl.col("date") >= pl.date(2024, 1, 1))
        & (pl.col("date") <= pl.date(2025, 12, 31))
    )

    if fd.height == 0:
        print("No BTC/ETH funding data found in requested window.")
        return

    fd = fd.with_columns(pl.col("date").dt.strftime("%Y-%m").alias("year_month"))

    agg = (
        fd.group_by(["asset_id", "year_month"])
        .agg(pl.col("funding_rate").mean().alias("mean_daily"))
        .with_columns(
            (pl.col("mean_daily") * 365 * 100)
            .round(2)
            .alias("annualized_apr_pct")
        )
        .sort(["asset_id", "year_month"])
    )

    print("Asset  Year-Month  MeanDailyFunding  AnnualizedAPR(%)")
    for row in agg.iter_rows(named=True):
        print(
            f"{row['asset_id']:>4}  {row['year_month']}  "
            f"{row['mean_daily']:.6f}  {row['annualized_apr_pct']:>8.2f}"
        )


if __name__ == "__main__":
    main()

